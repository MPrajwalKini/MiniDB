"""
MiniDB Page-Based Storage
=========================
4KB fixed-size pages inspired by PostgreSQL's heap pages.
Each page contains a header, slot directory, free space, and tuple data.

Key design (from storage_format.md):
  - Page header: 24 bytes (format_version, page_id, num_slots,
    free_start, flags, free_end, checksum, reserved)
  - Slot directory grows downward from offset 24 (APPEND-ONLY:
    new slots are always appended, never reordered)
  - Tuple data grows upward from page end
  - Free space sits between them

RID Contract
============
A RID = (page_id, slot_id) uniquely identifies a tuple within a table.

  - INSERT  → RID assigned once, stable for the row's lifetime
  - UPDATE  → same RID preserved. If new data fits in existing space,
              updated in-place. If not, data is relocated within the SAME
              page (after compaction). If it still doesn't fit, update
              FAILS (returns False) — caller must delete+re-insert.
              The RID never silently changes.
  - DELETE  → slot marked (0,0). The slot_id becomes REUSABLE by
              future inserts. A deleted RID returns None on lookup.
  - COMPACT → RIDs are STABLE through compaction. Only physical
              offsets change; the slot directory is updated to point
              to the new locations.

Slot Directory Rules
====================
  - Append-only: new slots always get slot_id = num_slots (monotonic).
  - Deleted slots may be reused by future inserts (first-fit scan).
  - Slots are never reordered or removed from the directory.
  - free_start always equals HEADER_SIZE + num_slots * SLOT_SIZE.

Endianness: ALL multi-byte integers use BIG-ENDIAN (network byte order).
This is fixed and documented. The '>' prefix in all struct formats enforces it.

Teaching note:
  PostgreSQL calls this structure a "heap page". The slot directory
  provides indirection — tuples can be reorganized (compacted) without
  changing their external RID, because the slot just points to the
  new offset. This is how VACUUM can defragment pages.
"""

import struct
import zlib
from dataclasses import dataclass
from typing import Optional

# ─── Constants ──────────────────────────────────────────────────────────────

PAGE_SIZE = 4096           # 4KB fixed page size
HEADER_SIZE = 24           # Page header: 24 bytes
SLOT_SIZE = 4              # Each slot: offset (2B) + length (2B)
FORMAT_VERSION = 1         # Current storage format version
MAGIC_BYTES = 0x4D44       # "MD" in ASCII — identifies MiniDB files

# Header struct: format_version(H) page_id(I) num_slots(H) free_start(H)
#                flags(H) free_end(H) checksum(I) reserved(6s)
HEADER_FMT = ">HIHHHHI6s"
HEADER_STRUCT = struct.Struct(HEADER_FMT)

# Slot struct: offset(H) length(H)
SLOT_FMT = ">HH"
SLOT_STRUCT = struct.Struct(SLOT_FMT)

# Deleted slot marker
DELETED_SLOT = (0, 0)


class PageCorruptionError(Exception):
    """Raised when a page fails integrity checks (CRC mismatch, overlap)."""
    pass


@dataclass
class RID:
    """Record ID — uniquely identifies a tuple within a table."""
    page_id: int    # uint32: page number in the table file
    slot_id: int    # uint16: slot index within the page

    def to_bytes(self) -> bytes:
        """Serialize RID to 6 bytes: page_id(4B) + slot_id(2B), big-endian."""
        return struct.pack(">IH", self.page_id, self.slot_id)

    @classmethod
    def from_bytes(cls, data: bytes, offset: int = 0) -> "RID":
        """Deserialize RID from 6 bytes."""
        page_id, slot_id = struct.unpack_from(">IH", data, offset)
        return cls(page_id=page_id, slot_id=slot_id)

    def __repr__(self) -> str:
        return f"RID({self.page_id}, {self.slot_id})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RID):
            return NotImplemented
        return self.page_id == other.page_id and self.slot_id == other.slot_id

    def __hash__(self) -> int:
        return hash((self.page_id, self.slot_id))


class Page:
    """
    A 4KB page with header, slot directory, and tuple storage.

    Memory layout:
      [0..23]  Header
      [24..]   Slot directory (grows down, 4 bytes per slot)
      [..]     Free space
      [..]     Tuple data (grows up from page end)

    Invariants enforced:
      - free_start = HEADER_SIZE + num_slots * SLOT_SIZE
      - free_start <= free_end (no overlap)
      - All tuple offsets fall within [free_end, PAGE_SIZE)
    """

    def __init__(self, page_id: int = 0, data: Optional[bytes] = None,
                 verify: bool = True):
        """
        Create or load a page.

        Args:
            page_id: Page number (used when creating fresh pages)
            data: Raw 4096-byte page data (if loading from disk)
            verify: If True, validate CRC32 checksum on load (default: True)
        """
        if data is not None:
            if len(data) != PAGE_SIZE:
                raise ValueError(f"Page data must be exactly {PAGE_SIZE} bytes, got {len(data)}")
            self._data = bytearray(data)
            self._parse_header()
            if verify:
                self._verify_on_load()
        else:
            # Initialize a fresh empty page
            self._data = bytearray(PAGE_SIZE)
            self._page_id = page_id
            self._format_version = FORMAT_VERSION
            self._num_slots = 0
            self._free_start = HEADER_SIZE  # right after header
            self._flags = 0
            self._free_end = PAGE_SIZE      # end of page
            self._checksum = 0
            self._write_header()

    def _parse_header(self) -> None:
        """Parse the 24-byte page header from _data."""
        vals = HEADER_STRUCT.unpack_from(self._data, 0)
        self._format_version = vals[0]
        self._page_id = vals[1]
        self._num_slots = vals[2]
        self._free_start = vals[3]
        self._flags = vals[4]
        self._free_end = vals[5]
        self._checksum = vals[6]
        # vals[7] is reserved (6 bytes, ignored)

    def _verify_on_load(self) -> None:
        """Validate page integrity on load: CRC + structural invariants."""
        # CRC32 check (skip for fresh pages with checksum=0)
        stored_crc = struct.unpack_from(">I", self._data, 14)[0]
        if stored_crc != 0:
            expected_crc = self.compute_checksum()
            if stored_crc != expected_crc:
                raise PageCorruptionError(
                    f"Page {self._page_id}: CRC mismatch "
                    f"(stored=0x{stored_crc:08X}, computed=0x{expected_crc:08X})")

        # Structural invariant: free_start must never exceed free_end
        if self._free_start > self._free_end:
            raise PageCorruptionError(
                f"Page {self._page_id}: free space overlap "
                f"(free_start={self._free_start} > free_end={self._free_end})")

        # Structural invariant: free_start must match slot count
        expected_start = HEADER_SIZE + self._num_slots * SLOT_SIZE
        if self._free_start != expected_start:
            raise PageCorruptionError(
                f"Page {self._page_id}: slot directory inconsistency "
                f"(free_start={self._free_start}, expected={expected_start})")

    def _write_header(self) -> None:
        """Write the current header fields back to _data."""
        HEADER_STRUCT.pack_into(
            self._data, 0,
            self._format_version,
            self._page_id,
            self._num_slots,
            self._free_start,
            self._flags,
            self._free_end,
            0,  # checksum placeholder (computed on to_bytes)
            b"\x00" * 6,  # reserved
        )

    def _assert_invariants(self) -> None:
        """Debug-mode invariant check after mutations."""
        assert self._free_start <= self._free_end, \
            f"free_start ({self._free_start}) > free_end ({self._free_end})"
        assert self._free_start == HEADER_SIZE + self._num_slots * SLOT_SIZE, \
            f"free_start mismatch: {self._free_start} != {HEADER_SIZE + self._num_slots * SLOT_SIZE}"

    @property
    def page_id(self) -> int:
        return self._page_id

    @property
    def num_slots(self) -> int:
        return self._num_slots

    @property
    def free_space(self) -> int:
        """Available free space in bytes (between slot dir end and tuple data start)."""
        return self._free_end - self._free_start

    def can_fit(self, tuple_size: int) -> bool:
        """Check if a tuple of the given size can fit in this page."""
        # Need space for: 1 new slot entry + the tuple data
        needed = SLOT_SIZE + tuple_size
        return self.free_space >= needed

    # ─── Slot operations ────────────────────────────────────────────

    def _slot_offset(self, slot_id: int) -> int:
        """Byte offset of slot entry in the page."""
        return HEADER_SIZE + slot_id * SLOT_SIZE

    def _read_slot(self, slot_id: int) -> tuple[int, int]:
        """Read a slot entry: (tuple_offset, tuple_length)."""
        off = self._slot_offset(slot_id)
        return SLOT_STRUCT.unpack_from(self._data, off)

    def _write_slot(self, slot_id: int, tuple_offset: int, tuple_length: int) -> None:
        """Write a slot entry."""
        off = self._slot_offset(slot_id)
        SLOT_STRUCT.pack_into(self._data, off, tuple_offset, tuple_length)

    # ─── Tuple CRUD ─────────────────────────────────────────────────

    def insert_tuple(self, tuple_data: bytes) -> int:
        """
        Insert a tuple into this page.
        Returns the slot_id assigned to the tuple.
        Raises ValueError if the page cannot fit the tuple.

        Slot reuse policy: scans for first deleted slot (offset=0, length=0)
        and reuses it. If none found, appends a new slot.
        """
        tuple_len = len(tuple_data)

        # First, try to reuse a deleted slot
        reuse_slot = None
        for i in range(self._num_slots):
            offset, length = self._read_slot(i)
            if (offset, length) == DELETED_SLOT:
                reuse_slot = i
                break

        if reuse_slot is not None:
            # Reuse deleted slot — but still need space for tuple data
            if self._free_end - self._free_start < tuple_len:
                raise ValueError(f"Page {self._page_id}: not enough free space "
                                 f"for tuple ({tuple_len}B, free: {self.free_space}B)")
            # Allocate tuple space from the end
            self._free_end -= tuple_len
            self._data[self._free_end:self._free_end + tuple_len] = tuple_data
            self._write_slot(reuse_slot, self._free_end, tuple_len)
            self._write_header()
            self._assert_invariants()
            return reuse_slot

        # New slot — need space for both slot entry and tuple data
        if not self.can_fit(tuple_len):
            raise ValueError(f"Page {self._page_id}: not enough free space "
                             f"for tuple ({tuple_len}B, free: {self.free_space}B)")

        # Allocate tuple space from the end (grows upward)
        self._free_end -= tuple_len
        self._data[self._free_end:self._free_end + tuple_len] = tuple_data

        # Write new slot entry (grows downward, append-only)
        slot_id = self._num_slots
        self._num_slots += 1
        self._free_start = HEADER_SIZE + self._num_slots * SLOT_SIZE
        self._write_slot(slot_id, self._free_end, tuple_len)

        self._write_header()
        self._assert_invariants()
        return slot_id

    def get_tuple(self, slot_id: int) -> Optional[bytes]:
        """
        Read a tuple by slot_id.
        Returns None if the slot is deleted or out of range.
        """
        if slot_id < 0 or slot_id >= self._num_slots:
            return None

        offset, length = self._read_slot(slot_id)
        if (offset, length) == DELETED_SLOT:
            return None

        return bytes(self._data[offset:offset + length])

    def delete_tuple(self, slot_id: int) -> bool:
        """
        Delete a tuple by marking its slot as deleted (offset=0, length=0).
        Returns True if deleted, False if slot was already deleted/invalid.

        The slot_id becomes reusable by future inserts.
        The actual tuple bytes remain until compaction (like PostgreSQL VACUUM).
        """
        if slot_id < 0 or slot_id >= self._num_slots:
            return False

        offset, length = self._read_slot(slot_id)
        if (offset, length) == DELETED_SLOT:
            return False

        self._write_slot(slot_id, 0, 0)
        self._write_header()
        return True

    def update_tuple(self, slot_id: int, new_data: bytes) -> bool:
        """
        Update a tuple. The RID (slot_id) is ALWAYS preserved.

        Strategy:
          1. If new data fits in existing space → in-place update
          2. If not → compact page, try to allocate new space
          3. If still doesn't fit → return False (caller should delete+reinsert)

        The RID never silently changes. This is a design contract.
        """
        if slot_id < 0 or slot_id >= self._num_slots:
            return False

        old_offset, old_length = self._read_slot(slot_id)
        if (old_offset, old_length) == DELETED_SLOT:
            return False

        new_len = len(new_data)

        if new_len <= old_length:
            # Fits in the same space — update in place
            self._data[old_offset:old_offset + new_len] = new_data
            if new_len < old_length:
                self._data[old_offset + new_len:old_offset + old_length] = b"\x00" * (old_length - new_len)
            self._write_slot(slot_id, old_offset, new_len)
            self._write_header()
            return True
        else:
            # Doesn't fit — mark old slot dead and try with compaction
            self._write_slot(slot_id, 0, 0)

            if self._free_end - self._free_start < new_len:
                self.compact()
                if self._free_end - self._free_start < new_len:
                    # Still doesn't fit — restore old data
                    self._write_slot(slot_id, old_offset, old_length)
                    self._write_header()
                    return False

            self._free_end -= new_len
            self._data[self._free_end:self._free_end + new_len] = new_data
            self._write_slot(slot_id, self._free_end, new_len)
            self._write_header()
            self._assert_invariants()
            return True

    def get_all_tuples(self) -> list[tuple[int, bytes]]:
        """
        Read all live (non-deleted) tuples.
        Returns list of (slot_id, tuple_bytes) in slot order (deterministic).
        """
        result: list[tuple[int, bytes]] = []
        for i in range(self._num_slots):
            data = self.get_tuple(i)
            if data is not None:
                result.append((i, data))
        return result

    def live_tuple_count(self) -> int:
        """Count of non-deleted tuples in this page."""
        count = 0
        for i in range(self._num_slots):
            offset, length = self._read_slot(i)
            if (offset, length) != DELETED_SLOT:
                count += 1
        return count

    # ─── Compaction ─────────────────────────────────────────────────

    def compact(self) -> None:
        """
        Compact the page by removing dead space from deleted tuples.
        Moves all live tuples to be contiguous at the end of the page,
        updates slot directory accordingly.

        RID STABILITY: Slot IDs are preserved. Only physical offsets change.
        External RIDs remain valid after compaction.
        """
        # Collect live tuples
        live: list[tuple[int, bytes]] = []
        for i in range(self._num_slots):
            data = self.get_tuple(i)
            if data is not None:
                live.append((i, data))

        # Clear tuple region
        self._free_end = PAGE_SIZE

        # Re-write tuples from the end
        for slot_id, tdata in live:
            tlen = len(tdata)
            self._free_end -= tlen
            self._data[self._free_end:self._free_end + tlen] = tdata
            self._write_slot(slot_id, self._free_end, tlen)

        # Zero out free space
        self._data[self._free_start:self._free_end] = b"\x00" * (self._free_end - self._free_start)
        self._write_header()
        self._assert_invariants()

    # ─── Serialization ──────────────────────────────────────────────

    def compute_checksum(self) -> int:
        """Compute CRC32 checksum of the page (excluding the checksum field at offset 14..17)."""
        before = self._data[:14]
        after = self._data[18:]
        return zlib.crc32(before + after) & 0xFFFFFFFF

    def to_bytes(self) -> bytes:
        """Serialize the page to PAGE_SIZE bytes with computed CRC32 checksum."""
        checksum = self.compute_checksum()
        self._checksum = checksum
        struct.pack_into(">I", self._data, 14, checksum)
        return bytes(self._data)

    def verify_checksum(self) -> bool:
        """Verify the page's CRC32 checksum."""
        stored = struct.unpack_from(">I", self._data, 14)[0]
        if stored == 0:
            return True  # Fresh page, no checksum yet
        expected = self.compute_checksum()
        return stored == expected

    def __repr__(self) -> str:
        live = self.live_tuple_count()
        return (f"Page(id={self._page_id}, slots={self._num_slots}, "
                f"live={live}, free={self.free_space}B)")
