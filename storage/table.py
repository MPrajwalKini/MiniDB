"""
MiniDB Table File Manager
=========================
Manages .tbl files: page allocation, row-level CRUD via RIDs,
full table scan, and page I/O through the buffer manager.

File layout:
  Page 0: Header page (schema + table metadata as JSON)
  Page 1..N: Data pages (tuples)

Page allocation safety:
  New pages are written to disk immediately on allocation (append to file).
  This means a crash mid-operation may leave an empty page at the end,
  but never a half-written page (since PAGE_SIZE writes are atomic on
  most filesystems for aligned 4KB blocks).

Scan order:
  Full table scan is deterministic: pages in ascending page_id order,
  tuples in ascending slot_id order within each page.

Teaching note:
  PostgreSQL manages table files ("relations") through its storage
  manager (smgr). Each relation has a "relfilenode" on disk. The buffer
  manager sits between the executor and disk I/O. We mirror this —
  TableFile uses BufferManager for page caching.
"""

import json
import os
import struct
from pathlib import Path
from typing import Any, Iterator, Optional

from storage.buffer import BufferManager
from storage.page import (
    PAGE_SIZE, HEADER_SIZE, FORMAT_VERSION, MAGIC_BYTES,
    Page, RID, PageCorruptionError,
)
from storage.schema import Schema
from storage.serializer import serialize_row, deserialize_row


# ─── Shared global buffer manager ──────────────────────────────────────────
# In a real database this would be a singleton managed at the engine level.
# For simplicity we use a module-level instance.
_global_buffer: Optional[BufferManager] = None


def get_buffer_manager(capacity: int = 64) -> BufferManager:
    """Get or create the global buffer manager."""
    global _global_buffer
    if _global_buffer is None:
        _global_buffer = BufferManager(capacity=capacity)
    return _global_buffer


def reset_buffer_manager() -> None:
    """Reset the global buffer manager (for testing)."""
    global _global_buffer
    if _global_buffer is not None:
        # Flush all dirty pages and clear cache on shutdown
        _global_buffer.flush_all_and_clear()
    _global_buffer = None


class TableFile:
    """
    Manages a single table's .tbl file.

    Provides:
    - create(): Initialize a new table file with schema
    - open(): Open an existing table file (validates CRC on page load)
    - insert_row(): Insert a row, returns RID
    - get_row(): Fetch a row by RID
    - delete_row(): Delete a row by RID
    - update_row(): Update a row by RID (same RID preserved)
    - scan(): Iterate all live rows (deterministic order)
    - close(): Flush and close
    """

    def __init__(self, file_path: str, buffer_mgr: Optional[BufferManager] = None):
        self._file_path = os.path.abspath(file_path)
        self._buffer = buffer_mgr or get_buffer_manager()
        self._schema: Optional[Schema] = None
        self._table_name: str = ""
        self._num_pages: int = 0
        self._is_open: bool = False

    @property
    def file_path(self) -> str:
        return self._file_path

    @property
    def schema(self) -> Schema:
        if self._schema is None:
            raise RuntimeError("Table not open — call open() or create() first")
        return self._schema

    @property
    def table_name(self) -> str:
        return self._table_name

    @property
    def num_data_pages(self) -> int:
        """Number of data pages (excludes header page 0)."""
        return max(0, self._num_pages - 1)

    # ─── Create / Open / Close ──────────────────────────────────────

    def create(self, table_name: str, schema: Schema) -> None:
        """
        Create a new table file with the given schema.
        Writes the header page (page 0) with metadata.
        """
        self._table_name = table_name
        self._schema = schema

        # Build header page content: magic + version + JSON metadata
        meta = {
            "magic": MAGIC_BYTES,
            "format_version": FORMAT_VERSION,
            "table_name": table_name,
            "schema": schema.to_dict(),
        }
        meta_bytes = json.dumps(meta, separators=(",", ":")).encode("utf-8")

        # Create header page
        header_page = Page(page_id=0)
        # Store metadata as a tuple in the header page
        header_page.insert_tuple(meta_bytes)

        # Write to disk
        with open(self._file_path, "wb") as f:
            f.write(header_page.to_bytes())

        self._num_pages = 1
        self._is_open = True

        # Cache the header page
        self._buffer.put_page(self._file_path, 0, header_page)

    def open(self) -> None:
        """
        Open an existing table file and read its schema.
        Pages are CRC32-validated on load.
        """
        if not os.path.exists(self._file_path):
            raise FileNotFoundError(f"Table file not found: {self._file_path}")

        # Read header page (CRC validated by Page constructor)
        header_page = self._read_page_from_disk(0)

        # Extract metadata from the first tuple
        tuples = header_page.get_all_tuples()
        if not tuples:
            raise ValueError(f"Corrupted table file: no metadata in header page")

        meta_bytes = tuples[0][1]
        meta = json.loads(meta_bytes.decode("utf-8"))

        if meta.get("magic") != MAGIC_BYTES:
            raise ValueError(f"Not a MiniDB file (bad magic bytes)")

        self._table_name = meta["table_name"]
        self._schema = Schema.from_dict(meta["schema"])

        # Count pages
        file_size = os.path.getsize(self._file_path)
        self._num_pages = file_size // PAGE_SIZE
        self._is_open = True

        # Cache header page
        self._buffer.put_page(self._file_path, 0, header_page)

    def close(self) -> None:
        """Flush all dirty pages and close the table file."""
        if not self._is_open:
            return
        self._flush()
        self._buffer.invalidate_file(self._file_path)
        self._is_open = False

    def _flush(self) -> None:
        """Flush all dirty pages for this table to disk."""
        dirty_pages = self._buffer.flush_file(self._file_path)
        if dirty_pages:
            with open(self._file_path, "r+b") as f:
                for page_id, page in dirty_pages:
                    f.seek(page_id * PAGE_SIZE)
                    f.write(page.to_bytes())
                f.flush()
                os.fsync(f.fileno())  # Force to disk

    # ─── Page I/O ───────────────────────────────────────────────────

    def _read_page_from_disk(self, page_id: int) -> Page:
        """
        Read a page from disk with CRC32 validation.
        Raises PageCorruptionError if checksum fails.
        """
        with open(self._file_path, "rb") as f:
            f.seek(page_id * PAGE_SIZE)
            data = f.read(PAGE_SIZE)
            if len(data) < PAGE_SIZE:
                raise ValueError(f"Incomplete page read: page {page_id}")
        # Page constructor validates CRC32 by default (verify=True)
        return Page(page_id=page_id, data=data)

    def _get_page(self, page_id: int) -> Page:
        """Get a page from buffer cache or disk (CRC-validated on first load)."""
        # Check cache first (single-frame invariant: only one copy exists)
        page = self._buffer.get_page(self._file_path, page_id)
        if page is not None:
            return page

        # Read from disk (CRC validated)
        page = self._read_page_from_disk(page_id)
        # put_page enforces single-frame: won't duplicate
        self._buffer.put_page(self._file_path, page_id, page)
        return page

    def _allocate_page(self) -> Page:
        """
        Allocate a new data page at the end of the file.

        Safety: the full 4KB page is written to disk immediately.
        On most filesystems, aligned 4KB writes are atomic.
        """
        page_id = self._num_pages
        page = Page(page_id=page_id)
        self._num_pages += 1

        # Write full page to disk immediately (atomic 4KB write)
        with open(self._file_path, "ab") as f:
            f.write(page.to_bytes())
            f.flush()
            os.fsync(f.fileno())

        # Cache it (clean — just written)
        self._buffer.put_page(self._file_path, page_id, page, dirty=False)
        return page

    def _find_page_with_space(self, needed: int) -> Page:
        """
        Find a data page with enough free space for a tuple,
        or allocate a new page.
        """
        # Scan existing data pages (pages 1..N-1)
        for pid in range(1, self._num_pages):
            page = self._get_page(pid)
            if page.can_fit(needed):
                return page

        # No space in existing pages — allocate new
        return self._allocate_page()

    # ─── Row CRUD ───────────────────────────────────────────────────

    def insert_row(self, row: list[Any]) -> RID:
        """
        Insert a row into the table.
        Returns the RID of the inserted row. The RID is stable for the
        row's lifetime (until deleted).
        """
        self._ensure_open()

        # Validate row against schema
        errors = self._schema.validate_row(row)
        if errors:
            raise ValueError(f"Row validation failed: {'; '.join(errors)}")

        # Serialize
        tuple_data = serialize_row(row, self._schema)
        tuple_len = len(tuple_data)

        # Find a page with space
        page = self._find_page_with_space(tuple_len)
        slot_id = page.insert_tuple(tuple_data)

        # Mark dirty
        self._buffer.mark_dirty(self._file_path, page.page_id)

        return RID(page_id=page.page_id, slot_id=slot_id)

    def get_row(self, rid: RID) -> Optional[list[Any]]:
        """
        Fetch a row by its RID.
        Returns the row values, or None if deleted/not found.
        """
        self._ensure_open()

        if rid.page_id < 1 or rid.page_id >= self._num_pages:
            return None

        page = self._get_page(rid.page_id)
        tuple_data = page.get_tuple(rid.slot_id)
        if tuple_data is None:
            return None

        values, _ = deserialize_row(tuple_data, self._schema)
        return values

    def delete_row(self, rid: RID) -> bool:
        """
        Delete a row by its RID.
        The slot becomes reusable by future inserts. Accessing the
        deleted RID returns None.
        """
        self._ensure_open()

        if rid.page_id < 1 or rid.page_id >= self._num_pages:
            return False

        page = self._get_page(rid.page_id)
        deleted = page.delete_tuple(rid.slot_id)
        if deleted:
            self._buffer.mark_dirty(self._file_path, page.page_id)
        return deleted

    def update_row(self, rid: RID, row: list[Any]) -> bool:
        """
        Update a row by its RID. The RID is ALWAYS preserved.

        If the new data fits in the existing space, it's updated in-place.
        If not, the page is compacted and the update retried.
        If it still doesn't fit, returns False — caller must delete + re-insert.
        """
        self._ensure_open()

        errors = self._schema.validate_row(row)
        if errors:
            raise ValueError(f"Row validation failed: {'; '.join(errors)}")

        if rid.page_id < 1 or rid.page_id >= self._num_pages:
            return False

        page = self._get_page(rid.page_id)
        new_data = serialize_row(row, self._schema)
        updated = page.update_tuple(rid.slot_id, new_data)
        if updated:
            self._buffer.mark_dirty(self._file_path, page.page_id)
        return updated

    def scan(self) -> Iterator[tuple[RID, list[Any]]]:
        """
        Full table scan — iterate all live rows.
        Yields (RID, row_values) for each non-deleted tuple.

        Order is deterministic:
          - Pages in ascending page_id order (1, 2, 3, ...)
          - Tuples in ascending slot_id order within each page

        Teaching note:
          This is a sequential scan (SeqScan) — the most basic access method.
          PostgreSQL does the same: read every page, every tuple. It's O(n)
          but guarantees finding all data. Index scans are faster but require
          an index to exist.
        """
        self._ensure_open()

        for pid in range(1, self._num_pages):
            page = self._get_page(pid)
            for slot_id, tuple_data in page.get_all_tuples():
                values, _ = deserialize_row(tuple_data, self._schema)
                yield RID(page_id=pid, slot_id=slot_id), values

    def row_count(self) -> int:
        """Count all live rows (full scan). Use with caution on large tables."""
        return sum(1 for _ in self.scan())

    # ─── Utilities ──────────────────────────────────────────────────

    def _ensure_open(self) -> None:
        if not self._is_open:
            raise RuntimeError("Table not open — call open() or create() first")

    def flush(self) -> None:
        """Public flush — writes dirty pages to disk with fsync."""
        self._flush()

    def __repr__(self) -> str:
        return (f"TableFile(name='{self._table_name}', "
                f"pages={self._num_pages}, path='{self._file_path}')")

    def __del__(self) -> None:
        """Attempt to flush on garbage collection."""
        if self._is_open:
            try:
                self._flush()
            except Exception:
                pass
