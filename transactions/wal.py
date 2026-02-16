"""
MiniDB Write-Ahead Log (WAL)
============================
Append-only binary log for crash recovery and rollback.

LSN = byte offset in WAL file (enables O(1) random access for undo).
File starts with 4 zero bytes; NULL_LSN = 0 means "no previous record".
"""

import os
import struct
import zlib
from dataclasses import dataclass
from enum import IntEnum
from typing import Iterator, Optional, BinaryIO

# ─── Constants ──────────────────────────────────────────────────────────────

NULL_LSN = 0          # Sentinel: no previous record
WAL_PADDING = 4       # First 4 bytes of file are zero (reserves offset 0)

class WALRecordType(IntEnum):
    BEGIN      = 0x01
    COMMIT     = 0x02
    ABORT      = 0x03
    INSERT     = 0x10
    DELETE     = 0x11
    UPDATE     = 0x12
    CLR        = 0x20   # Compensation Log Record (redo-only, never undone)
    CHECKPOINT = 0xFF

# Record header: total_len(I) lsn(I) txn_id(I) prev_txn_lsn(I) type(B)
_HDR_FMT = ">IIIIB"
_HDR_SIZE = struct.calcsize(_HDR_FMT)   # 17
_CRC_SIZE = 4
_MIN_RECORD = _HDR_SIZE + _CRC_SIZE     # 21

# ─── WALEntry ───────────────────────────────────────────────────────────────

@dataclass
class WALEntry:
    """Parsed WAL record."""
    lsn: int
    txn_id: int
    prev_lsn: int
    record_type: WALRecordType
    payload: bytes
    total_len: int

# ─── LogManager ─────────────────────────────────────────────────────────────

class LogManager:
    """
    Manages the WAL file.

    Guarantees:
      - LSN = byte offset in wal.log (monotonic, allows random access)
      - CRC32 on every record
      - flush() forces fsync to disk
      - durable_lsn tracks what has been fsynced
    """

    def __init__(self, data_dir: str):
        self._wal_path = os.path.join(data_dir, "wal.log")

        # Create file if needed
        if not os.path.exists(self._wal_path):
            with open(self._wal_path, "wb") as f:
                f.write(struct.pack(">I", 0))  # 4-byte NULL padding
                f.flush()
                os.fsync(f.fileno())

        self._file: BinaryIO = open(self._wal_path, "r+b")

        # Determine next write position
        size = self._file.seek(0, os.SEEK_END)
        if size == 0:
            # Empty file — write padding
            self._file.write(struct.pack(">I", 0))
            self._file.flush()
            os.fsync(self._file.fileno())
            size = WAL_PADDING

        self._next_lsn = size
        self._durable_lsn = size  # On open, whatever is on disk is durable

    # ─── Properties ──────────────────────────────────────────────────────

    @property
    def durable_lsn(self) -> int:
        """Byte offset up to which WAL is durable on disk."""
        return self._durable_lsn

    @property
    def next_lsn(self) -> int:
        return self._next_lsn

    def set_next_lsn(self, lsn: int) -> None:
        """Used by recovery manager to advance LSN after scanning."""
        if lsn > self._next_lsn:
            self._next_lsn = lsn
            self._file.seek(lsn)

    # ─── Core I/O ────────────────────────────────────────────────────────

    def flush(self) -> None:
        """Force all buffered WAL data to disk."""
        if self._file:
            self._file.flush()
            os.fsync(self._file.fileno())
            self._durable_lsn = self._next_lsn

    def close(self) -> None:
        if self._file:
            self.flush()
            self._file.close()
            self._file = None

    def truncate(self, to_lsn: int) -> None:
        """Truncate WAL to given offset. Used after checkpoint."""
        if to_lsn < WAL_PADDING:
            to_lsn = WAL_PADDING
        self._file.truncate(to_lsn)
        self._file.seek(to_lsn)
        self._next_lsn = to_lsn
        self.flush()

    # ─── Write ───────────────────────────────────────────────────────────

    def _write_record(self, txn_id: int, prev_lsn: int,
                      rtype: WALRecordType, payload: bytes) -> int:
        """Append a record. Returns its LSN (byte offset). Does NOT flush."""
        lsn = self._next_lsn
        total_len = _HDR_SIZE + len(payload) + _CRC_SIZE

        hdr = struct.pack(_HDR_FMT, total_len, lsn, txn_id, prev_lsn, rtype)
        crc = zlib.crc32(hdr)
        if payload:
            crc = zlib.crc32(payload, crc)
        crc_bytes = struct.pack(">I", crc & 0xFFFFFFFF)

        self._file.seek(lsn)
        self._file.write(hdr)
        if payload:
            self._file.write(payload)
        self._file.write(crc_bytes)

        self._next_lsn = lsn + total_len
        return lsn

    # ─── Payload builders ────────────────────────────────────────────────

    @staticmethod
    def _pack_table_rid(table_name: str, page_id: int, slot_id: int) -> bytes:
        tb = table_name.encode("utf-8")
        return struct.pack(">H", len(tb)) + tb + struct.pack(">IH", page_id, slot_id)

    # ─── Public append methods ───────────────────────────────────────────

    def append_begin(self, txn_id: int) -> int:
        return self._write_record(txn_id, NULL_LSN, WALRecordType.BEGIN, b"")

    def append_commit(self, txn_id: int, prev_lsn: int) -> int:
        lsn = self._write_record(txn_id, prev_lsn, WALRecordType.COMMIT, b"")
        self.flush()  # COMMIT MUST be durable before acknowledged
        return lsn

    def append_abort(self, txn_id: int, prev_lsn: int) -> int:
        lsn = self._write_record(txn_id, prev_lsn, WALRecordType.ABORT, b"")
        self.flush()
        return lsn

    def append_insert(self, txn_id: int, prev_lsn: int,
                      table_name: str, page_id: int, slot_id: int,
                      tuple_data: bytes) -> int:
        prefix = self._pack_table_rid(table_name, page_id, slot_id)
        payload = prefix + struct.pack(">H", len(tuple_data)) + tuple_data
        return self._write_record(txn_id, prev_lsn, WALRecordType.INSERT, payload)

    def append_delete(self, txn_id: int, prev_lsn: int,
                      table_name: str, page_id: int, slot_id: int,
                      tuple_data: bytes) -> int:
        prefix = self._pack_table_rid(table_name, page_id, slot_id)
        payload = prefix + struct.pack(">H", len(tuple_data)) + tuple_data
        return self._write_record(txn_id, prev_lsn, WALRecordType.DELETE, payload)

    def append_update(self, txn_id: int, prev_lsn: int,
                      table_name: str, page_id: int, slot_id: int,
                      old_data: bytes, new_data: bytes) -> int:
        prefix = self._pack_table_rid(table_name, page_id, slot_id)
        payload = (prefix +
                   struct.pack(">H", len(old_data)) + old_data +
                   struct.pack(">H", len(new_data)) + new_data)
        return self._write_record(txn_id, prev_lsn, WALRecordType.UPDATE, payload)

    def append_clr(self, txn_id: int, prev_lsn: int,
                   undo_next_lsn: int, inner_type: int,
                   inner_payload: bytes) -> int:
        """
        Compensation Log Record — logged during undo so crash-during-rollback
        is safe.  undo_next_lsn = prev record to undo next (chain skip).
        inner_type/payload describe the compensating action (redo-only).
        """
        payload = struct.pack(">IB", undo_next_lsn, inner_type) + inner_payload
        return self._write_record(txn_id, prev_lsn, WALRecordType.CLR, payload)

    def append_checkpoint(self, active_txns: list[tuple[int, int]]) -> int:
        """active_txns = [(txn_id, last_lsn), ...]"""
        payload = struct.pack(">I", len(active_txns))
        for tid, last_lsn in active_txns:
            payload += struct.pack(">II", tid, last_lsn)
        lsn = self._write_record(0, NULL_LSN, WALRecordType.CHECKPOINT, payload)
        self.flush()
        return lsn

    # ─── Read ────────────────────────────────────────────────────────────

    def read_record(self, lsn: int) -> WALEntry:
        """Read a single record at the given LSN (byte offset)."""
        self._file.seek(lsn)
        hdr_data = self._file.read(_HDR_SIZE)
        if len(hdr_data) < _HDR_SIZE:
            raise ValueError(f"Unexpected EOF reading header at LSN {lsn}")

        total_len, rec_lsn, txn_id, prev_lsn, rtype_val = struct.unpack(_HDR_FMT, hdr_data)

        # Sanity checks
        if rec_lsn != lsn:
            raise ValueError(f"LSN mismatch at offset {lsn}: header says {rec_lsn}")
        if total_len < _MIN_RECORD:
            raise ValueError(f"Record too small ({total_len}) at LSN {lsn}")

        payload_len = total_len - _HDR_SIZE - _CRC_SIZE
        rest = self._file.read(payload_len + _CRC_SIZE)
        if len(rest) < payload_len + _CRC_SIZE:
            raise ValueError(f"Unexpected EOF reading payload at LSN {lsn}")

        payload = rest[:payload_len]
        stored_crc = struct.unpack(">I", rest[payload_len:])[0]

        # CRC verify
        computed = zlib.crc32(hdr_data)
        if payload:
            computed = zlib.crc32(payload, computed)
        if (computed & 0xFFFFFFFF) != stored_crc:
            raise ValueError(f"CRC mismatch at LSN {lsn}")

        return WALEntry(
            lsn=lsn,
            txn_id=txn_id,
            prev_lsn=prev_lsn,
            record_type=WALRecordType(rtype_val),
            payload=payload,
            total_len=total_len,
        )

    def scan(self, start_lsn: int = WAL_PADDING) -> Iterator[WALEntry]:
        """Yield all records from start_lsn to end of file."""
        pos = start_lsn
        file_end = self._file.seek(0, os.SEEK_END)
        while pos < file_end:
            entry = self.read_record(pos)
            yield entry
            pos += entry.total_len

    # ─── Payload parsing helpers (used by recovery / txn manager) ────────

    @staticmethod
    def parse_dml_payload(payload: bytes):
        """
        Parse INSERT/DELETE payload → (table_name, page_id, slot_id, tuple_data).
        """
        off = 0
        tb_len = struct.unpack_from(">H", payload, off)[0]; off += 2
        table_name = payload[off:off+tb_len].decode("utf-8"); off += tb_len
        page_id, slot_id = struct.unpack_from(">IH", payload, off); off += 6
        tup_len = struct.unpack_from(">H", payload, off)[0]; off += 2
        tuple_data = payload[off:off+tup_len]
        return table_name, page_id, slot_id, tuple_data

    @staticmethod
    def parse_update_payload(payload: bytes):
        """
        Parse UPDATE payload → (table_name, page_id, slot_id, old_data, new_data).
        """
        off = 0
        tb_len = struct.unpack_from(">H", payload, off)[0]; off += 2
        table_name = payload[off:off+tb_len].decode("utf-8"); off += tb_len
        page_id, slot_id = struct.unpack_from(">IH", payload, off); off += 6
        old_len = struct.unpack_from(">H", payload, off)[0]; off += 2
        old_data = payload[off:off+old_len]; off += old_len
        new_len = struct.unpack_from(">H", payload, off)[0]; off += 2
        new_data = payload[off:off+new_len]
        return table_name, page_id, slot_id, old_data, new_data

    @staticmethod
    def parse_clr_payload(payload: bytes):
        """Parse CLR payload → (undo_next_lsn, inner_type, inner_payload)."""
        undo_next_lsn, inner_type = struct.unpack_from(">IB", payload, 0)
        inner_payload = payload[5:]
        return undo_next_lsn, inner_type, inner_payload

    @staticmethod
    def parse_checkpoint_payload(payload: bytes):
        """Parse CHECKPOINT → list of (txn_id, last_lsn)."""
        n = struct.unpack_from(">I", payload, 0)[0]
        result = []
        off = 4
        for _ in range(n):
            tid, last_lsn = struct.unpack_from(">II", payload, off)
            result.append((tid, last_lsn))
            off += 8
        return result
