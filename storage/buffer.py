"""
MiniDB Buffer Manager
=====================
In-memory page cache with LRU eviction, pin/unpin, dirty tracking.

Safety guarantees:
  - Single-frame invariant: same (file, page_id) is never loaded twice.
    put_page() updates existing entry if present.
  - Pinned pages cannot be evicted (raises RuntimeError if all pinned).
  - flush_all_and_clear() ensures all dirty pages are returned on shutdown.
  - Dirty page flush order is deterministic (insertion/access order via OrderedDict).

Teaching note:
  PostgreSQL has a sophisticated shared buffer pool (shared_buffers)
  with clock-sweep eviction. Snowflake doesn't need a traditional buffer
  pool because it uses cloud object storage with local SSD caching.
  We implement a simple LRU cache that's sufficient for a teaching DB.
"""

from collections import OrderedDict
from typing import Optional

from storage.page import Page, PAGE_SIZE


class BufferManager:
    """
    Page cache with LRU eviction.

    - Pages are identified by (file_path, page_id) tuples
    - Pin count prevents eviction of pages in active use
    - Dirty flag tracks pages that need to be flushed to disk
    - Single-frame invariant: each (file, page_id) appears at most once
    """

    def __init__(self, capacity: int = 64):
        """
        Initialize buffer manager with a maximum number of cached pages.

        Args:
            capacity: Max pages to hold in memory (default: 64 = 256KB)
        """
        self._capacity = capacity
        # OrderedDict gives us LRU ordering: most recently used at the end
        self._cache: OrderedDict[tuple[str, int], _BufferEntry] = OrderedDict()

    @property
    def size(self) -> int:
        """Number of pages currently in the cache."""
        return len(self._cache)

    def get_page(self, file_path: str, page_id: int) -> Optional[Page]:
        """
        Get a page from the cache. Returns None if not cached.
        Moves the page to the most-recently-used position.
        Does NOT pin the page — call pin() separately if needed.
        """
        key = (file_path, page_id)
        entry = self._cache.get(key)
        if entry is None:
            return None
        # Move to end (most recently used)
        self._cache.move_to_end(key)
        return entry.page

    def put_page(self, file_path: str, page_id: int, page: Page,
                 dirty: bool = False) -> Optional[tuple[str, int, Page]]:
        """
        Put a page into the cache (single-frame invariant enforced).

        If the key already exists, the entry is UPDATED (not duplicated).
        If the cache is full, evicts the least-recently-used unpinned page.

        Returns:
          The evicted (file_path, page_id, page) if a dirty page was evicted
          (caller must flush it to disk), or None if no dirty eviction happened.
        """
        key = (file_path, page_id)
        if key in self._cache:
            # Update existing entry — single-frame invariant: no duplicate load
            entry = self._cache[key]
            entry.page = page
            entry.dirty = entry.dirty or dirty
            self._cache.move_to_end(key)
            return None

        evicted = None
        # Evict if at capacity
        if len(self._cache) >= self._capacity:
            evicted = self._evict_one()

        self._cache[key] = _BufferEntry(page=page, dirty=dirty, pin_count=0)
        self._cache.move_to_end(key)
        return evicted

    def pin(self, file_path: str, page_id: int) -> bool:
        """
        Pin a page to prevent eviction.
        Returns True if the page was found and pinned.
        """
        key = (file_path, page_id)
        entry = self._cache.get(key)
        if entry is None:
            return False
        entry.pin_count += 1
        return True

    def unpin(self, file_path: str, page_id: int) -> bool:
        """
        Unpin a page (decrement pin count).
        Returns True if the page was found and unpinned.
        """
        key = (file_path, page_id)
        entry = self._cache.get(key)
        if entry is None:
            return False
        if entry.pin_count > 0:
            entry.pin_count -= 1
        return True

    def mark_dirty(self, file_path: str, page_id: int) -> None:
        """Mark a cached page as dirty (needs flushing)."""
        key = (file_path, page_id)
        entry = self._cache.get(key)
        if entry is not None:
            entry.dirty = True

    def is_dirty(self, file_path: str, page_id: int) -> bool:
        """Check if a cached page is dirty."""
        key = (file_path, page_id)
        entry = self._cache.get(key)
        return entry.dirty if entry is not None else False

    def flush_all(self) -> list[tuple[str, int, Page]]:
        """
        Return all dirty pages that need to be written to disk.
        Clears the dirty flag for each returned page.
        Order: deterministic (insertion/LRU order from OrderedDict).
        """
        dirty_pages: list[tuple[str, int, Page]] = []
        for (fp, pid), entry in self._cache.items():
            if entry.dirty:
                dirty_pages.append((fp, pid, entry.page))
                entry.dirty = False
        return dirty_pages

    def flush_all_and_clear(self) -> list[tuple[str, int, Page]]:
        """
        Shutdown method: return ALL dirty pages and clear the entire cache.
        Must be called before process exit to ensure durability.
        """
        dirty_pages = self.flush_all()
        self._cache.clear()
        return dirty_pages

    def flush_file(self, file_path: str) -> list[tuple[int, Page]]:
        """
        Return all dirty pages for a specific file.
        Clears the dirty flag for each returned page.
        """
        dirty_pages: list[tuple[int, Page]] = []
        for (fp, pid), entry in self._cache.items():
            if fp == file_path and entry.dirty:
                dirty_pages.append((pid, entry.page))
                entry.dirty = False
        return dirty_pages

    def invalidate(self, file_path: str, page_id: int) -> Optional[Page]:
        """Remove a page from the cache. Returns the page if it was dirty."""
        key = (file_path, page_id)
        entry = self._cache.pop(key, None)
        if entry is not None and entry.dirty:
            return entry.page
        return None

    def invalidate_file(self, file_path: str) -> list[tuple[int, Page]]:
        """Remove all pages for a file. Returns list of dirty pages."""
        dirty: list[tuple[int, Page]] = []
        to_remove = [k for k in self._cache if k[0] == file_path]
        for key in to_remove:
            entry = self._cache.pop(key)
            if entry.dirty:
                dirty.append((key[1], entry.page))
        return dirty

    def _evict_one(self) -> Optional[tuple[str, int, Page]]:
        """
        Evict the least-recently-used unpinned page.
        Returns (file_path, page_id, page) if the evicted page was dirty.
        Raises RuntimeError if all pages are pinned.
        """
        for key in list(self._cache.keys()):
            entry = self._cache[key]
            if entry.pin_count == 0:
                self._cache.pop(key)
                if entry.dirty:
                    return (key[0], key[1], entry.page)
                return None

        raise RuntimeError("Buffer pool full: all pages are pinned. "
                           "Cannot evict. Increase buffer pool size or "
                           "unpin pages after use.")

    def stats(self) -> dict:
        """Return buffer pool statistics."""
        pinned = sum(1 for e in self._cache.values() if e.pin_count > 0)
        dirty = sum(1 for e in self._cache.values() if e.dirty)
        return {
            "capacity": self._capacity,
            "used": len(self._cache),
            "pinned": pinned,
            "dirty": dirty,
            "free": self._capacity - len(self._cache),
        }


class _BufferEntry:
    """Internal cache entry."""
    __slots__ = ("page", "dirty", "pin_count")

    def __init__(self, page: Page, dirty: bool = False, pin_count: int = 0):
        self.page = page
        self.dirty = dirty
        self.pin_count = pin_count
