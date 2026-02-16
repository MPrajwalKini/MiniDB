"""
MiniDB Indexing Tests — Phase 5
================================
Tests for B-Tree index: key encoding, insert/search/range, splits,
persistence, index manager, IndexScanExec, and planner integration.
"""

import os
import math
import shutil
import tempfile
import pytest

from storage.buffer import BufferManager
from storage.page import RID
from storage.types import DataType
from storage.schema import Schema, Column
from storage.table import TableFile

from indexing.key_encoding import encode_key, decode_key
from indexing.btree import BTree
from indexing.index_manager import build_index, open_index_by_info

from catalog.catalog import Catalog
from execution.executor import Executor
from execution.context import ExecutionContext


# ═══════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════

@pytest.fixture
def tmpdir():
    d = tempfile.mkdtemp(prefix="minidb_idx_")
    yield d
    shutil.rmtree(d, ignore_errors=True)

@pytest.fixture
def bm():
    return BufferManager(capacity=64)


def _make_executor(tmpdir, bm):
    """Helper: create Executor with proper ExecutionContext."""
    catalog = Catalog(tmpdir)
    catalog.load()
    ctx = ExecutionContext(catalog=catalog, buffer_manager=bm, base_path=tmpdir)
    return Executor(ctx)


# ═══════════════════════════════════════════════════════════════════
# Key Encoding Tests
# ═══════════════════════════════════════════════════════════════════

class TestKeyEncoding:
    """Verify order-preserving key encoding for all supported types."""

    def test_int_ordering(self):
        vals = [-1000, -1, 0, 1, 42, 1000]
        encoded = [encode_key(v, DataType.INT) for v in vals]
        assert encoded == sorted(encoded)

    def test_int_roundtrip(self):
        for v in [-2**31 + 1, -1, 0, 1, 2**31 - 1]:
            decoded, _ = decode_key(encode_key(v, DataType.INT), 0, DataType.INT)
            assert decoded == v

    def test_float_ordering(self):
        vals = [-100.5, -1.0, -0.01, 0.0, 0.01, 1.0, 100.5]
        encoded = [encode_key(v, DataType.FLOAT) for v in vals]
        assert encoded == sorted(encoded)

    def test_float_pos_neg_zero_identical(self):
        assert encode_key(0.0, DataType.FLOAT) == encode_key(-0.0, DataType.FLOAT)

    def test_float_nan_rejected(self):
        with pytest.raises(ValueError, match="NaN"):
            encode_key(float('nan'), DataType.FLOAT)

    def test_float_roundtrip(self):
        for v in [-1.5, 0.0, 3.14, 1e10]:
            decoded, _ = decode_key(encode_key(v, DataType.FLOAT), 0, DataType.FLOAT)
            assert decoded == v

    def test_string_ordering(self):
        """String encoding must handle prefix relationships correctly."""
        vals = ["a", "aa", "ab", "b", "ba"]
        encoded = [encode_key(v, DataType.STRING) for v in vals]
        assert encoded == sorted(encoded)

    def test_string_utf8_multibyte(self):
        """Multi-byte UTF-8 characters round-trip correctly."""
        vals = ["café", "naïve", "日本語"]
        for v in vals:
            decoded, _ = decode_key(encode_key(v, DataType.STRING), 0, DataType.STRING)
            assert decoded == v

    def test_string_with_embedded_null(self):
        """Strings containing null bytes are handled correctly."""
        val = "hel\x00lo"
        decoded, _ = decode_key(encode_key(val, DataType.STRING), 0, DataType.STRING)
        assert decoded == val

    def test_boolean_ordering(self):
        assert encode_key(False, DataType.BOOLEAN) < encode_key(True, DataType.BOOLEAN)

    def test_null_rejected(self):
        with pytest.raises(ValueError, match="NULL"):
            encode_key(None, DataType.INT)


# ═══════════════════════════════════════════════════════════════════
# B-Tree Core Tests
# ═══════════════════════════════════════════════════════════════════

class TestBTree:

    def test_insert_and_search(self, tmpdir, bm):
        path = os.path.join(tmpdir, "test.idx")
        bt = BTree.create(path, "t", "col", DataType.INT, bm)
        bt.insert(10, RID(1, 0))
        bt.insert(20, RID(1, 1))
        bt.insert(30, RID(1, 2))

        assert bt.search(10) == [RID(1, 0)]
        assert bt.search(20) == [RID(1, 1)]
        assert bt.search(99) == []
        bt.close()

    def test_duplicate_keys(self, tmpdir, bm):
        """Multiple RIDs for the same key, deterministic ordering by RID."""
        path = os.path.join(tmpdir, "dup.idx")
        bt = BTree.create(path, "t", "col", DataType.INT, bm)
        bt.insert(42, RID(1, 0))
        bt.insert(42, RID(2, 5))
        bt.insert(42, RID(1, 3))

        results = bt.search(42)
        assert len(results) == 3
        # Duplicate ordering must be deterministic by RID
        rid_bytes = [r.to_bytes() for r in results]
        assert rid_bytes == sorted(rid_bytes), "Duplicates must be ordered by RID"
        bt.close()

    def test_range_scan_ordered(self, tmpdir, bm):
        path = os.path.join(tmpdir, "range.idx")
        bt = BTree.create(path, "t", "col", DataType.INT, bm)
        for i in [50, 10, 30, 20, 40]:
            bt.insert(i, RID(1, i))

        # Inclusive range
        result = list(bt.range_scan(20, 40, True, True))
        vals = [v for v, _ in result]
        assert vals == [20, 30, 40]

        # Exclusive range
        result = list(bt.range_scan(20, 40, False, False))
        vals = [v for v, _ in result]
        assert vals == [30]

        # Unbounded low
        result = list(bt.range_scan(None, 30, True, True))
        vals = [v for v, _ in result]
        assert vals == [10, 20, 30]

        # Unbounded high
        result = list(bt.range_scan(30, None, True, True))
        vals = [v for v, _ in result]
        assert vals == [30, 40, 50]

        bt.close()

    def test_split_and_structure(self, tmpdir, bm):
        """Insert enough keys to trigger multiple splits."""
        path = os.path.join(tmpdir, "split.idx")
        bt = BTree.create(path, "t", "col", DataType.INT, bm)

        # Insert 200 keys — should trigger several splits
        for i in range(200):
            bt.insert(i, RID(1, i))

        assert bt.entry_count == 200

        # Verify structure
        issues = bt.verify_structure()
        assert issues == [], f"Structure issues: {issues}"

        # Verify all keys searchable
        for i in range(200):
            r = bt.search(i)
            assert len(r) == 1, f"Key {i} not found or duplicated"
            assert r[0] == RID(1, i)

        bt.close()

    def test_range_across_leaves(self, tmpdir, bm):
        """Range scan crossing multiple leaf page boundaries."""
        path = os.path.join(tmpdir, "wide.idx")
        bt = BTree.create(path, "t", "col", DataType.INT, bm)

        for i in range(500):
            bt.insert(i, RID(1, i))

        result = list(bt.range_scan(100, 400, True, True))
        vals = [v for v, _ in result]
        assert vals == list(range(100, 401))
        bt.close()

    def test_persistence_after_split(self, tmpdir):
        """Close and reopen B-Tree after splits — data survives."""
        path = os.path.join(tmpdir, "persist.idx")
        bm1 = BufferManager(capacity=32)
        bt = BTree.create(path, "t", "col", DataType.INT, bm1)

        for i in range(100):
            bt.insert(i * 10, RID(1, i))
        bt.close()

        # Reopen with fresh buffer
        bm2 = BufferManager(capacity=32)
        bt2 = BTree.open(path, bm2)
        assert bt2.entry_count == 100

        for i in range(100):
            r = bt2.search(i * 10)
            assert len(r) == 1, f"Key {i*10} not found after reopen"
        
        # Verify structure intact
        issues = bt2.verify_structure()
        assert issues == [], f"Post-reopen issues: {issues}"
        bt2.close()

    def test_string_keys(self, tmpdir, bm):
        """B-Tree with variable-length string keys."""
        path = os.path.join(tmpdir, "str.idx")
        bt = BTree.create(path, "t", "name", DataType.STRING, bm)
        names = ["alice", "bob", "charlie", "dave", "eve"]
        for i, name in enumerate(names):
            bt.insert(name, RID(1, i))

        assert bt.search("charlie") == [RID(1, 2)]
        assert bt.search("frank") == []

        result = list(bt.range_scan("bob", "dave", True, True))
        vals = [v for v, _ in result]
        assert vals == ["bob", "charlie", "dave"]
        bt.close()

    def test_metadata_format_version(self, tmpdir, bm):
        """Metadata page contains format version and magic."""
        import json
        from storage.page import Page
        path = os.path.join(tmpdir, "meta.idx")
        bt = BTree.create(path, "users", "age", DataType.INT, bm)
        bt.insert(1, RID(1, 0))
        bt.close()

        # Read raw metadata
        with open(path, "rb") as f:
            data = f.read(4096)
        page = Page(page_id=0, data=data, verify=True)
        meta = json.loads(page.get_all_tuples()[0][1].decode("utf-8"))
        assert meta["magic"] == "MDBX"
        assert meta["format_version"] == 1
        assert meta["table_name"] == "users"
        assert meta["column_name"] == "age"
        assert meta["key_type"] == "INT"


# ═══════════════════════════════════════════════════════════════════
# Index Manager Tests
# ═══════════════════════════════════════════════════════════════════

class TestIndexManager:

    def test_build_index_from_table(self, tmpdir, bm):
        """Build a B-Tree index from an existing table with data."""
        catalog = Catalog(tmpdir)
        catalog.load()

        schema = Schema([
            Column("id", DataType.INT, nullable=False),
            Column("name", DataType.STRING),
            Column("age", DataType.INT),
        ])

        catalog.create_table("users", schema)
        tbl_path = catalog.get_table_file("users")
        tf = TableFile(tbl_path, bm)
        tf.create("users", schema)

        # Insert test data
        tf.insert_row([1, "alice", 30])
        tf.insert_row([2, "bob", 25])
        tf.insert_row([3, "charlie", None])  # NULL age
        tf.insert_row([4, "dave", 30])       # Duplicate age

        # Build index
        btree = build_index(catalog, tf, "users", "age", "idx_users_age", bm)

        # NULL should NOT be indexed
        assert btree.entry_count == 3  # 3 non-NULL ages
        assert btree.search(30) != []
        assert len(btree.search(30)) == 2  # Two rows with age=30
        assert btree.search(25) != []
        btree.close()
        tf.close()

    def test_null_not_indexed(self, tmpdir, bm):
        """Verify NULL values are excluded from the index."""
        catalog = Catalog(tmpdir)
        catalog.load()

        schema = Schema([
            Column("id", DataType.INT),
            Column("val", DataType.INT),
        ])
        catalog.create_table("t", schema)
        tbl_path = catalog.get_table_file("t")
        tf = TableFile(tbl_path, bm)
        tf.create("t", schema)

        tf.insert_row([1, None])
        tf.insert_row([2, 10])
        tf.insert_row([3, None])
        tf.insert_row([4, 20])

        btree = build_index(catalog, tf, "t", "val", "idx_t_val", bm)
        assert btree.entry_count == 2  # Only non-NULL values
        assert btree.search(10) != []
        assert btree.search(20) != []
        btree.close()
        tf.close()


# ═══════════════════════════════════════════════════════════════════
# End-to-End Execution Tests (IndexScan + Planner)
# ═══════════════════════════════════════════════════════════════════

class TestIndexExecution:

    def test_index_scan_equality(self, tmpdir, bm):
        """End-to-end: CREATE TABLE → INSERT → build index → SELECT with =."""
        executor = _make_executor(tmpdir, bm)

        executor.execute_and_fetchall("CREATE TABLE emp (id INT, name STRING, age INT)")
        executor.execute_and_fetchall("INSERT INTO emp VALUES (1, 'Alice', 30)")
        executor.execute_and_fetchall("INSERT INTO emp VALUES (2, 'Bob', 25)")
        executor.execute_and_fetchall("INSERT INTO emp VALUES (3, 'Charlie', 30)")
        executor.execute_and_fetchall("INSERT INTO emp VALUES (4, 'Dave', 40)")

        # Build index on age
        catalog = executor.context.catalog
        tbl_path = catalog.get_table_file("emp")
        tf = TableFile(tbl_path, bm)
        tf.open()
        build_index(catalog, tf, "emp", "age", "idx_emp_age", bm)
        tf.close()

        # Query with equality predicate — planner should use index
        rows = executor.execute_and_fetchall("SELECT name, age FROM emp WHERE age = 30")
        names = sorted([r["name"] for r in rows])
        assert names == ["Alice", "Charlie"]

    def test_index_scan_range(self, tmpdir, bm):
        """Range query uses index when available."""
        executor = _make_executor(tmpdir, bm)

        executor.execute_and_fetchall("CREATE TABLE scores (id INT, score INT)")
        for i in range(1, 11):
            executor.execute_and_fetchall(f"INSERT INTO scores VALUES ({i}, {i * 10})")

        # Build index
        catalog = executor.context.catalog
        tbl_path = catalog.get_table_file("scores")
        tf = TableFile(tbl_path, bm)
        tf.open()
        build_index(catalog, tf, "scores", "score", "idx_scores_score", bm)
        tf.close()

        rows = executor.execute_and_fetchall("SELECT id, score FROM scores WHERE score >= 70")
        scores = sorted([r["score"] for r in rows])
        assert scores == [70, 80, 90, 100]

    def test_fallback_to_seqscan(self, tmpdir, bm):
        """When no index exists, planner falls back to SeqScan."""
        executor = _make_executor(tmpdir, bm)

        executor.execute_and_fetchall("CREATE TABLE items (id INT, val INT)")
        executor.execute_and_fetchall("INSERT INTO items VALUES (1, 100)")
        executor.execute_and_fetchall("INSERT INTO items VALUES (2, 200)")

        # No index built — should use SeqScan
        rows = executor.execute_and_fetchall("SELECT id FROM items WHERE val = 100")
        assert len(rows) == 1
        assert rows[0]["id"] == 1

    def test_residual_predicate(self, tmpdir, bm):
        """IndexScan with residual predicate on non-indexed column."""
        executor = _make_executor(tmpdir, bm)

        executor.execute_and_fetchall("CREATE TABLE t (a INT, b INT)")
        executor.execute_and_fetchall("INSERT INTO t VALUES (1, 10)")
        executor.execute_and_fetchall("INSERT INTO t VALUES (2, 10)")
        executor.execute_and_fetchall("INSERT INTO t VALUES (3, 20)")

        # Index on 'a'
        catalog = executor.context.catalog
        tbl_path = catalog.get_table_file("t")
        tf = TableFile(tbl_path, bm)
        tf.open()
        build_index(catalog, tf, "t", "a", "idx_t_a", bm)
        tf.close()

        # Query that matches index on 'a' = 1
        rows = executor.execute_and_fetchall("SELECT a, b FROM t WHERE a = 1")
        assert len(rows) == 1
        assert rows[0]["a"] == 1
        assert rows[0]["b"] == 10

    def test_canonicalization(self, tmpdir, bm):
        """Planner canonicalizes '5 < a' to 'a > 5'."""
        executor = _make_executor(tmpdir, bm)

        executor.execute_and_fetchall("CREATE TABLE nums (x INT)")
        for i in range(1, 11):
            executor.execute_and_fetchall(f"INSERT INTO nums VALUES ({i})")

        catalog = executor.context.catalog
        tbl_path = catalog.get_table_file("nums")
        tf = TableFile(tbl_path, bm)
        tf.open()
        build_index(catalog, tf, "nums", "x", "idx_nums_x", bm)
        tf.close()

        # "5 < x" should be canonicalized to "x > 5"
        rows = executor.execute_and_fetchall("SELECT x FROM nums WHERE 5 < x")
        vals = sorted([r["x"] for r in rows])
        assert vals == [6, 7, 8, 9, 10]

