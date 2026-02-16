"""
MiniDB B-Tree Index
===================
Disk-backed B+ Tree supporting insert, exact search, and range scan.
Persisted as .idx files using the existing Page infrastructure.

Architecture:
  - Page 0: metadata (root_page, key_type, table, column, format version)
  - Page 1+: B-Tree nodes (leaf or internal)

Node types:
  - LEAF: stores sorted (key, RID) entries. Linked via right_sibling for range scan.
  - INTERNAL: stores sorted separator keys with child page pointers.
    Invariant: left subtree < K, right subtree >= K.

Key ordering:
  - All keys stored as order-preserving encoded bytes (see key_encoding.py).
  - Duplicate keys ordered by (key_bytes, RID) for deterministic range scans.

Concurrency: single-writer, no locking.
NULLs: not indexed (caller must filter before insert).
Delete: not implemented (Phase 5 scope).
"""

import json
import struct
from typing import Any, Iterator, List, Optional, Tuple

from storage.page import Page, PAGE_SIZE, RID
from storage.buffer import BufferManager
from storage.types import DataType
from indexing.key_encoding import encode_key, decode_key, fixed_key_size

import os

# ─── Constants ──────────────────────────────────────────────────────────────

BTREE_MAGIC = "MDBX"
BTREE_FORMAT_VERSION = 1

NODE_TYPE_LEAF = 0
NODE_TYPE_INTERNAL = 1

# B-Tree order is determined dynamically based on key size and page capacity.
# We use a target fill factor to decide when to split.
# Header overhead per node blob: 1 (type) + 2 (key_count) + 4 (right_sibling)
NODE_HEADER_SIZE = 7  # bytes

# RID is 6 bytes (page_id: 4B, slot_id: 2B)
RID_SIZE = 6
# Child pointer is 4 bytes (page_id)
CHILD_PTR_SIZE = 4

# Maximum usable space in a page for our node blob.
# Page has its own header (~20 bytes), slot directory, etc.
# We store the entire node as ONE tuple in the page.
# Practical max tuple size ~ PAGE_SIZE - 100 (conservative)
MAX_NODE_PAYLOAD = PAGE_SIZE - 200


# ─── Node Serialization ────────────────────────────────────────────────────

class BTreeNode:
    """
    In-memory representation of a B-Tree node (leaf or internal).
    Serialized as a single byte blob stored in a Page.
    """
    __slots__ = (
        'page_id', 'node_type', 'keys', 'rids',
        'children', 'right_sibling', 'dirty',
    )

    def __init__(self, page_id: int, node_type: int):
        self.page_id = page_id
        self.node_type = node_type
        self.keys: List[bytes] = []           # encoded key bytes
        self.rids: List[RID] = []             # leaf only: parallel to keys
        self.children: List[int] = []         # internal only: page_ids
        self.right_sibling: int = 0           # leaf only: 0 = no sibling
        self.dirty: bool = False

    @property
    def key_count(self) -> int:
        return len(self.keys)

    @property
    def is_leaf(self) -> bool:
        return self.node_type == NODE_TYPE_LEAF

    def serialize(self) -> bytes:
        """Serialize node to bytes for storage in a Page tuple."""
        buf = bytearray()
        # Header
        buf.append(self.node_type)
        buf.extend(struct.pack(">H", len(self.keys)))
        buf.extend(struct.pack(">I", self.right_sibling))

        # Keys: each key is [2B length][key_bytes]
        for k in self.keys:
            buf.extend(struct.pack(">H", len(k)))
            buf.extend(k)

        # Pointers
        if self.is_leaf:
            # RIDs: each 6 bytes
            for rid in self.rids:
                buf.extend(rid.to_bytes())
        else:
            # Child page_ids: each 4 bytes. N keys → N+1 children
            for child in self.children:
                buf.extend(struct.pack(">I", child))

        return bytes(buf)

    @classmethod
    def deserialize(cls, page_id: int, data: bytes) -> 'BTreeNode':
        """Deserialize a node from bytes."""
        offset = 0
        node_type = data[offset]; offset += 1
        key_count = struct.unpack_from(">H", data, offset)[0]; offset += 2
        right_sibling = struct.unpack_from(">I", data, offset)[0]; offset += 4

        node = cls(page_id, node_type)
        node.right_sibling = right_sibling

        # Keys
        for _ in range(key_count):
            klen = struct.unpack_from(">H", data, offset)[0]; offset += 2
            node.keys.append(bytes(data[offset:offset + klen]))
            offset += klen

        # Pointers
        if node.is_leaf:
            for _ in range(key_count):
                rid = RID.from_bytes(data, offset)
                node.rids.append(rid)
                offset += RID_SIZE
        else:
            for _ in range(key_count + 1):
                child = struct.unpack_from(">I", data, offset)[0]; offset += 4
                node.children.append(child)

        return node

    def find_insert_pos(self, key_bytes: bytes, rid: Optional[RID] = None) -> int:
        """
        Find insertion position maintaining sorted order.
        For duplicate keys, uses RID as tiebreaker for deterministic ordering.
        """
        for i, k in enumerate(self.keys):
            if key_bytes < k:
                return i
            if key_bytes == k and rid is not None and self.is_leaf:
                # Tiebreak by RID for deterministic duplicate ordering
                if rid.to_bytes() < self.rids[i].to_bytes():
                    return i
        return len(self.keys)

    def find_key_pos(self, key_bytes: bytes) -> int:
        """Find position of first key >= key_bytes (for search/range scan start)."""
        for i, k in enumerate(self.keys):
            if k >= key_bytes:
                return i
        return len(self.keys)


# ─── B-Tree ────────────────────────────────────────────────────────────────

class BTree:
    """
    Disk-backed B+ Tree index.

    Usage:
        # Create new index
        bt = BTree.create(path, 'users', 'age', DataType.INT, buffer_mgr)
        bt.insert(25, RID(1, 0))
        results = bt.search(25)
        bt.close()

        # Open existing
        bt = BTree.open(path, buffer_mgr)
    """

    def __init__(self, file_path: str, buffer_mgr: BufferManager):
        self._file_path = os.path.abspath(file_path)
        self._buffer = buffer_mgr
        self._root_page: int = 0
        self._key_type: DataType = DataType.INT
        self._table_name: str = ""
        self._column_name: str = ""
        self._next_page: int = 0
        self._is_open: bool = False
        self._entry_count: int = 0
        self._tree_height: int = 0

    @property
    def root_page(self) -> int:
        return self._root_page

    @property
    def entry_count(self) -> int:
        return self._entry_count

    @property
    def tree_height(self) -> int:
        return self._tree_height

    # ─── Create / Open / Close ──────────────────────────────────────

    @classmethod
    def create(cls, file_path: str, table_name: str, column_name: str,
               key_type: DataType, buffer_mgr: BufferManager) -> 'BTree':
        """Create a new empty B-Tree index file."""
        bt = cls(file_path, buffer_mgr)
        bt._table_name = table_name
        bt._column_name = column_name
        bt._key_type = key_type

        # Page 0: metadata
        meta = {
            "magic": BTREE_MAGIC,
            "format_version": BTREE_FORMAT_VERSION,
            "table_name": table_name,
            "column_name": column_name,
            "key_type": key_type.value,
            "root_page": 1,
            "next_page": 2,
            "entry_count": 0,
            "tree_height": 1,
        }
        meta_bytes = json.dumps(meta, separators=(",", ":")).encode("utf-8")
        meta_page = Page(page_id=0)
        meta_page.insert_tuple(meta_bytes)

        # Page 1: empty root leaf node
        root_node = BTreeNode(page_id=1, node_type=NODE_TYPE_LEAF)
        root_page = Page(page_id=1)
        root_page.insert_tuple(root_node.serialize())

        # Write to disk
        with open(bt._file_path, "wb") as f:
            f.write(meta_page.to_bytes())
            f.write(root_page.to_bytes())

        # Cache
        bt._buffer.put_page(bt._file_path, 0, meta_page)
        bt._buffer.put_page(bt._file_path, 1, root_page)

        bt._root_page = 1
        bt._next_page = 2
        bt._entry_count = 0
        bt._tree_height = 1
        bt._is_open = True
        return bt

    @classmethod
    def open(cls, file_path: str, buffer_mgr: BufferManager) -> 'BTree':
        """Open an existing B-Tree index file."""
        bt = cls(file_path, buffer_mgr)

        if not os.path.exists(bt._file_path):
            raise FileNotFoundError(f"Index file not found: {bt._file_path}")

        # Read metadata page
        meta_page = bt._read_page(0)
        tuples = meta_page.get_all_tuples()
        if not tuples:
            raise ValueError("Corrupted index file: no metadata")

        meta = json.loads(tuples[0][1].decode("utf-8"))
        if meta.get("magic") != BTREE_MAGIC:
            raise ValueError("Not a MiniDB index file")
        if meta.get("format_version", 0) != BTREE_FORMAT_VERSION:
            raise ValueError(f"Unsupported index format version: {meta.get('format_version')}")

        bt._table_name = meta["table_name"]
        bt._column_name = meta["column_name"]
        bt._key_type = DataType(meta["key_type"])
        bt._root_page = meta["root_page"]
        bt._next_page = meta["next_page"]
        bt._entry_count = meta.get("entry_count", 0)
        bt._tree_height = meta.get("tree_height", 1)

        # Count pages from file size
        file_size = os.path.getsize(bt._file_path)
        actual_pages = file_size // PAGE_SIZE
        if bt._next_page < actual_pages:
            bt._next_page = actual_pages

        bt._is_open = True
        return bt

    def close(self) -> None:
        """Flush metadata and all dirty pages, then close."""
        if not self._is_open:
            return
        self._write_metadata()
        self._flush()
        self._buffer.invalidate_file(self._file_path)
        self._is_open = False

    def __del__(self):
        try:
            if self._is_open:
                self.close()
        except Exception:
            pass

    # ─── Search ─────────────────────────────────────────────────────

    def search(self, key: Any) -> List[RID]:
        """
        Exact-match search. Returns all RIDs matching the key.
        Follows leaf sibling chain for duplicates that span leaves.
        """
        self._ensure_open()
        key_bytes = encode_key(key, self._key_type)
        leaf = self._find_leaf(key_bytes)

        results: List[RID] = []
        while leaf is not None:
            found_in_leaf = False
            for i, k in enumerate(leaf.keys):
                if k == key_bytes:
                    results.append(leaf.rids[i])
                    found_in_leaf = True
                elif k > key_bytes:
                    return results
            # If we found matches and there might be more in next leaf
            if found_in_leaf and leaf.right_sibling != 0:
                leaf = self._read_node(leaf.right_sibling)
            else:
                break
        return results

    def range_scan(self, low: Any = None, high: Any = None,
                   low_inclusive: bool = True,
                   high_inclusive: bool = True) -> Iterator[Tuple[Any, RID]]:
        """
        Range scan over the index. Yields (decoded_key, RID) pairs in order.

        - low=None means unbounded below (start from leftmost leaf).
        - high=None means unbounded above (scan to end).
        """
        self._ensure_open()

        low_bytes = encode_key(low, self._key_type) if low is not None else None
        high_bytes = encode_key(high, self._key_type) if high is not None else None

        # Find starting leaf
        if low_bytes is not None:
            leaf = self._find_leaf(low_bytes)
        else:
            leaf = self._find_leftmost_leaf()

        while leaf is not None:
            for i, k in enumerate(leaf.keys):
                # Check low bound
                if low_bytes is not None:
                    if low_inclusive:
                        if k < low_bytes:
                            continue
                    else:
                        if k <= low_bytes:
                            continue

                # Check high bound
                if high_bytes is not None:
                    if high_inclusive:
                        if k > high_bytes:
                            return
                    else:
                        if k >= high_bytes:
                            return

                # Decode and yield
                val, _ = decode_key(k, 0, self._key_type)
                yield val, leaf.rids[i]

            # Follow sibling chain
            if leaf.right_sibling != 0:
                leaf = self._read_node(leaf.right_sibling)
            else:
                break

    # ─── Insert ─────────────────────────────────────────────────────

    def insert(self, key: Any, rid: RID) -> None:
        """
        Insert a (key, RID) pair into the index.
        Handles node splits and root splits automatically.
        """
        self._ensure_open()
        key_bytes = encode_key(key, self._key_type)
        result = self._insert_recursive(self._root_page, key_bytes, rid)
        self._entry_count += 1

        if result is not None:
            # Root was split — create new root
            split_key, new_child_page = result
            new_root = BTreeNode(
                page_id=self._alloc_page(),
                node_type=NODE_TYPE_INTERNAL,
            )
            new_root.keys.append(split_key)
            new_root.children.append(self._root_page)
            new_root.children.append(new_child_page)
            new_root.dirty = True
            self._write_node(new_root)
            self._root_page = new_root.page_id
            self._tree_height += 1

    def _insert_recursive(self, page_id: int, key_bytes: bytes,
                          rid: RID) -> Optional[Tuple[bytes, int]]:
        """
        Recursive insert. Returns None if no split, or
        (promoted_key, new_child_page_id) if a split occurred.
        """
        node = self._read_node(page_id)

        if node.is_leaf:
            return self._insert_into_leaf(node, key_bytes, rid)
        else:
            # Find child to descend into
            child_idx = self._find_child_index(node, key_bytes)
            child_page = node.children[child_idx]
            result = self._insert_recursive(child_page, key_bytes, rid)

            if result is None:
                return None

            # Child was split — insert promoted key into this internal node
            promoted_key, new_child_page = result
            return self._insert_into_internal(node, promoted_key, new_child_page, child_idx)

    def _insert_into_leaf(self, node: BTreeNode, key_bytes: bytes,
                          rid: RID) -> Optional[Tuple[bytes, int]]:
        """Insert into leaf node. Split if necessary."""
        pos = node.find_insert_pos(key_bytes, rid)
        node.keys.insert(pos, key_bytes)
        node.rids.insert(pos, rid)
        node.dirty = True

        # Check if split needed
        if len(node.serialize()) > MAX_NODE_PAYLOAD:
            return self._split_leaf(node)

        self._write_node(node)
        return None

    def _insert_into_internal(self, node: BTreeNode, key_bytes: bytes,
                              new_child: int,
                              after_child_idx: int) -> Optional[Tuple[bytes, int]]:
        """Insert separator + child pointer into internal node."""
        # Insert key at after_child_idx, new child pointer at after_child_idx + 1
        node.keys.insert(after_child_idx, key_bytes)
        node.children.insert(after_child_idx + 1, new_child)
        node.dirty = True

        # Check if split needed
        if len(node.serialize()) > MAX_NODE_PAYLOAD:
            return self._split_internal(node)

        self._write_node(node)
        return None

    # ─── Split ──────────────────────────────────────────────────────

    def _split_leaf(self, node: BTreeNode) -> Tuple[bytes, int]:
        """
        Split a leaf node at the median.
        Median key is COPIED UP to parent (leaf retains it).
        Returns (promoted_key, new_leaf_page_id).
        """
        mid = len(node.keys) // 2

        # Create new right leaf
        new_leaf = BTreeNode(
            page_id=self._alloc_page(),
            node_type=NODE_TYPE_LEAF,
        )
        new_leaf.keys = node.keys[mid:]
        new_leaf.rids = node.rids[mid:]
        # Maintain sibling chain: new.right = old.right; old.right = new
        new_leaf.right_sibling = node.right_sibling
        new_leaf.dirty = True

        # Trim old leaf
        node.keys = node.keys[:mid]
        node.rids = node.rids[:mid]
        node.right_sibling = new_leaf.page_id
        node.dirty = True

        # Promoted key = first key of new leaf (copied up)
        promoted_key = new_leaf.keys[0]

        self._write_node(node)
        self._write_node(new_leaf)

        return promoted_key, new_leaf.page_id

    def _split_internal(self, node: BTreeNode) -> Tuple[bytes, int]:
        """
        Split an internal node at the median.
        Median key is PUSHED UP to parent (removed from this node).
        Returns (promoted_key, new_internal_page_id).

        Before split: keys=[k0,k1,k2,k3,k4], children=[c0,c1,c2,c3,c4,c5]
        Mid=2: promoted=k2
        Left:  keys=[k0,k1],    children=[c0,c1,c2]
        Right: keys=[k3,k4],    children=[c3,c4,c5]
        """
        mid = len(node.keys) // 2
        promoted_key = node.keys[mid]

        new_internal = BTreeNode(
            page_id=self._alloc_page(),
            node_type=NODE_TYPE_INTERNAL,
        )
        new_internal.keys = node.keys[mid + 1:]
        new_internal.children = node.children[mid + 1:]
        new_internal.dirty = True

        # Trim old node
        node.keys = node.keys[:mid]
        node.children = node.children[:mid + 1]
        node.dirty = True

        self._write_node(node)
        self._write_node(new_internal)

        return promoted_key, new_internal.page_id

    # ─── Navigation ─────────────────────────────────────────────────

    def _find_child_index(self, node: BTreeNode, key_bytes: bytes) -> int:
        """
        Find which child to descend into for an internal node.
        Invariant: left subtree < K, right subtree >= K.
        Returns index into node.children.
        """
        for i, k in enumerate(node.keys):
            if key_bytes < k:
                return i
        return len(node.keys)

    def _find_leaf(self, key_bytes: bytes) -> BTreeNode:
        """Navigate from root to the leaf node that should contain the key."""
        node = self._read_node(self._root_page)
        while not node.is_leaf:
            child_idx = self._find_child_index(node, key_bytes)
            node = self._read_node(node.children[child_idx])
        return node

    def _find_leftmost_leaf(self) -> BTreeNode:
        """Find the leftmost leaf node (for unbounded range scans)."""
        node = self._read_node(self._root_page)
        while not node.is_leaf:
            node = self._read_node(node.children[0])
        return node

    # ─── Page I/O ───────────────────────────────────────────────────

    def _read_page(self, page_id: int) -> Page:
        """Read a page, checking buffer cache first."""
        cached = self._buffer.get_page(self._file_path, page_id)
        if cached is not None:
            return cached

        with open(self._file_path, "rb") as f:
            f.seek(page_id * PAGE_SIZE)
            data = f.read(PAGE_SIZE)
            if len(data) < PAGE_SIZE:
                raise ValueError(f"Truncated page {page_id} in index file")

        page = Page(page_id=page_id, data=data, verify=True)
        self._buffer.put_page(self._file_path, page_id, page)
        return page

    def _read_node(self, page_id: int) -> BTreeNode:
        """Read and deserialize a B-Tree node from a page."""
        page = self._read_page(page_id)
        tuples = page.get_all_tuples()
        if not tuples:
            raise ValueError(f"Empty node page {page_id}")
        return BTreeNode.deserialize(page_id, tuples[0][1])

    def _write_node(self, node: BTreeNode) -> None:
        """Serialize and write a B-Tree node to its page."""
        page = Page(page_id=node.page_id)
        page.insert_tuple(node.serialize())
        self._buffer.put_page(self._file_path, node.page_id, page)
        self._buffer.mark_dirty(self._file_path, node.page_id)

    def _alloc_page(self) -> int:
        """Allocate a new page ID."""
        page_id = self._next_page
        self._next_page += 1
        return page_id

    def _write_metadata(self) -> None:
        """Write the metadata page (page 0)."""
        meta = {
            "magic": BTREE_MAGIC,
            "format_version": BTREE_FORMAT_VERSION,
            "table_name": self._table_name,
            "column_name": self._column_name,
            "key_type": self._key_type.value,
            "root_page": self._root_page,
            "next_page": self._next_page,
            "entry_count": self._entry_count,
            "tree_height": self._tree_height,
        }
        meta_bytes = json.dumps(meta, separators=(",", ":")).encode("utf-8")
        meta_page = Page(page_id=0)
        meta_page.insert_tuple(meta_bytes)
        self._buffer.put_page(self._file_path, 0, meta_page)
        self._buffer.mark_dirty(self._file_path, 0)

    def _flush(self) -> None:
        """Flush all dirty pages for this index to disk."""
        dirty_pages = self._buffer.flush_file(self._file_path)
        if dirty_pages:
            # Ensure file is large enough
            needed_size = self._next_page * PAGE_SIZE
            if not os.path.exists(self._file_path):
                with open(self._file_path, "wb") as f:
                    f.write(b"\x00" * needed_size)
            else:
                current_size = os.path.getsize(self._file_path)
                if current_size < needed_size:
                    with open(self._file_path, "ab") as f:
                        f.write(b"\x00" * (needed_size - current_size))

            with open(self._file_path, "r+b") as f:
                for page_id, page in dirty_pages:
                    f.seek(page_id * PAGE_SIZE)
                    f.write(page.to_bytes())
                f.flush()
                os.fsync(f.fileno())

    def _ensure_open(self) -> None:
        if not self._is_open:
            raise RuntimeError("BTree is not open")

    # ─── Debug / Verification ───────────────────────────────────────

    def verify_structure(self) -> List[str]:
        """
        Verify index structural integrity.
        Returns list of issues found (empty = healthy).
        """
        self._ensure_open()
        issues: List[str] = []
        try:
            self._verify_node(self._root_page, None, None, issues, depth=0)
        except Exception as e:
            issues.append(f"Exception during verification: {e}")

        # Verify leaf sibling chain
        self._verify_leaf_chain(issues)
        return issues

    def _verify_node(self, page_id: int, min_key: Optional[bytes],
                     max_key: Optional[bytes], issues: List[str],
                     depth: int) -> None:
        """Recursively verify a node and its children."""
        node = self._read_node(page_id)

        # Keys must be sorted
        for i in range(1, len(node.keys)):
            if node.keys[i] < node.keys[i - 1]:
                issues.append(f"Page {page_id}: keys not sorted at position {i}")

        # Keys must be within bounds
        for k in node.keys:
            if min_key is not None and k < min_key:
                issues.append(f"Page {page_id}: key below parent separator")
            if max_key is not None and k >= max_key:
                issues.append(f"Page {page_id}: key at/above parent separator")

        if not node.is_leaf:
            # Internal: verify children
            if len(node.children) != len(node.keys) + 1:
                issues.append(f"Page {page_id}: children count mismatch")
                return

            for i, child in enumerate(node.children):
                lo = node.keys[i - 1] if i > 0 else min_key
                hi = node.keys[i] if i < len(node.keys) else max_key
                self._verify_node(child, lo, hi, issues, depth + 1)

    def _verify_leaf_chain(self, issues: List[str]) -> None:
        """Verify the leaf sibling chain is ordered."""
        leaf = self._find_leftmost_leaf()
        prev_max_key: Optional[bytes] = None
        visited = set()

        while leaf is not None:
            if leaf.page_id in visited:
                issues.append(f"Leaf chain cycle at page {leaf.page_id}")
                break
            visited.add(leaf.page_id)

            if leaf.keys and prev_max_key is not None:
                if leaf.keys[0] < prev_max_key:
                    issues.append(
                        f"Leaf chain ordering broken at page {leaf.page_id}")

            if leaf.keys:
                prev_max_key = leaf.keys[-1]

            if leaf.right_sibling != 0:
                leaf = self._read_node(leaf.right_sibling)
            else:
                leaf = None
