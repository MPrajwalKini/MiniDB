"""
MiniDB Phase 2 Storage Engine Tests
====================================
Comprehensive test suite covering all Phase 2 verification criteria:
  ✔ page create / read / write
  ✔ tuple insert / fetch by RID
  ✔ tuple delete
  ✔ page full handling
  ✔ free space reuse
  ✔ persistence after restart
  ✔ buffer flush correctness
  ✔ multiple pages per table
  ✔ catalog survives restart
"""

import json
import os
import sys
import tempfile
import shutil
from datetime import date

import pytest

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage.types import (
    DataType, serialize_value, deserialize_value, validate, coerce,
    type_from_string,
)
from storage.schema import Column, Schema
from storage.serializer import serialize_row, deserialize_row, serialized_row_size
from storage.page import Page, RID, PAGE_SIZE, FORMAT_VERSION, DELETED_SLOT, PageCorruptionError
from storage.buffer import BufferManager
from storage.table import TableFile, reset_buffer_manager
from catalog.catalog import Catalog


# ═══════════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════════

@pytest.fixture
def tmp_dir():
    """Create a temporary directory for test files."""
    path = tempfile.mkdtemp(prefix="minidb_test_")
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture(autouse=True)
def clean_buffer():
    """Reset global buffer manager between tests."""
    reset_buffer_manager()
    yield
    reset_buffer_manager()


@pytest.fixture
def user_schema():
    """Sample schema: users(id INT, name STRING, active BOOLEAN)."""
    return Schema(columns=[
        Column("id", DataType.INT, nullable=False),
        Column("name", DataType.STRING, nullable=True),
        Column("active", DataType.BOOLEAN, nullable=True),
    ])


@pytest.fixture
def full_schema():
    """Schema with all 5 data types."""
    return Schema(columns=[
        Column("id", DataType.INT, nullable=False),
        Column("score", DataType.FLOAT, nullable=True),
        Column("name", DataType.STRING, nullable=True),
        Column("flag", DataType.BOOLEAN, nullable=True),
        Column("created", DataType.DATE, nullable=True),
    ])


# ═══════════════════════════════════════════════════════════════════════════
# 1. Data Type Tests
# ═══════════════════════════════════════════════════════════════════════════

class TestDataTypes:
    """Test the data type system: serialize, deserialize, validate, coerce."""

    def test_int_roundtrip(self):
        data = serialize_value(42, DataType.INT)
        val, off = deserialize_value(data, 0, DataType.INT)
        assert val == 42 and off == 4

    def test_int_negative(self):
        data = serialize_value(-1000, DataType.INT)
        val, _ = deserialize_value(data, 0, DataType.INT)
        assert val == -1000

    def test_float_roundtrip(self):
        data = serialize_value(3.14159, DataType.FLOAT)
        val, off = deserialize_value(data, 0, DataType.FLOAT)
        assert abs(val - 3.14159) < 1e-10 and off == 8

    def test_string_roundtrip(self):
        data = serialize_value("hello 🌍", DataType.STRING)
        val, off = deserialize_value(data, 0, DataType.STRING)
        assert val == "hello 🌍"

    def test_string_empty(self):
        data = serialize_value("", DataType.STRING)
        val, _ = deserialize_value(data, 0, DataType.STRING)
        assert val == ""

    def test_boolean_true(self):
        data = serialize_value(True, DataType.BOOLEAN)
        val, _ = deserialize_value(data, 0, DataType.BOOLEAN)
        assert val is True

    def test_boolean_false(self):
        data = serialize_value(False, DataType.BOOLEAN)
        val, _ = deserialize_value(data, 0, DataType.BOOLEAN)
        assert val is False

    def test_date_roundtrip(self):
        d = date(2026, 2, 16)
        data = serialize_value(d, DataType.DATE)
        val, _ = deserialize_value(data, 0, DataType.DATE)
        assert val == d

    def test_date_from_string(self):
        data = serialize_value("2000-01-01", DataType.DATE)
        val, _ = deserialize_value(data, 0, DataType.DATE)
        assert val == date(2000, 1, 1)

    def test_validate_int(self):
        assert validate(42, DataType.INT) is True
        assert validate("42", DataType.INT) is False
        assert validate(True, DataType.INT) is False  # bool is not int

    def test_validate_null(self):
        for dt in DataType:
            assert validate(None, dt) is True

    def test_coerce_string_to_int(self):
        assert coerce("123", DataType.INT) == 123

    def test_coerce_string_to_bool(self):
        assert coerce("true", DataType.BOOLEAN) is True
        assert coerce("FALSE", DataType.BOOLEAN) is False

    def test_coerce_string_to_date(self):
        assert coerce("2026-02-16", DataType.DATE) == date(2026, 2, 16)

    def test_type_from_string(self):
        assert type_from_string("INT") == DataType.INT
        assert type_from_string("string") == DataType.STRING
        with pytest.raises(ValueError):
            type_from_string("INVALID")


# ═══════════════════════════════════════════════════════════════════════════
# 2. Schema Tests
# ═══════════════════════════════════════════════════════════════════════════

class TestSchema:
    """Test schema definition, validation, and serialization."""

    def test_column_count(self, user_schema):
        assert user_schema.column_count == 3

    def test_column_names(self, user_schema):
        assert user_schema.column_names() == ["id", "name", "active"]

    def test_column_index(self, user_schema):
        assert user_schema.column_index("name") == 1

    def test_column_index_case_insensitive(self, user_schema):
        assert user_schema.column_index("NAME") == 1

    def test_column_not_found(self, user_schema):
        with pytest.raises(KeyError):
            user_schema.column_index("nonexistent")

    def test_validate_row_ok(self, user_schema):
        errors = user_schema.validate_row([1, "Alice", True])
        assert errors == []

    def test_validate_row_wrong_count(self, user_schema):
        errors = user_schema.validate_row([1, "Alice"])
        assert len(errors) == 1

    def test_validate_row_null_violation(self, user_schema):
        errors = user_schema.validate_row([None, "Alice", True])
        assert len(errors) == 1 and "NULL" in errors[0]

    def test_validate_row_null_allowed(self, user_schema):
        errors = user_schema.validate_row([1, None, None])
        assert errors == []

    def test_schema_serialization_roundtrip(self, user_schema):
        data = user_schema.to_bytes()
        restored = Schema.from_bytes(data)
        assert restored.column_count == user_schema.column_count
        for orig, rest in zip(user_schema.columns, restored.columns):
            assert orig.name == rest.name
            assert orig.data_type == rest.data_type
            assert orig.nullable == rest.nullable


# ═══════════════════════════════════════════════════════════════════════════
# 3. Serializer Tests
# ═══════════════════════════════════════════════════════════════════════════

class TestSerializer:
    """Test tuple serialization with null bitmap."""

    def test_basic_roundtrip(self, user_schema):
        row = [1, "Alice", True]
        data = serialize_row(row, user_schema)
        restored, end = deserialize_row(data, user_schema)
        assert restored == row

    def test_null_values(self, user_schema):
        row = [1, None, None]
        data = serialize_row(row, user_schema)
        restored, _ = deserialize_row(data, user_schema)
        assert restored == [1, None, None]

    def test_all_types(self, full_schema):
        row = [42, 3.14, "test", True, date(2026, 1, 1)]
        data = serialize_row(row, full_schema)
        restored, _ = deserialize_row(data, full_schema)
        assert restored[0] == 42
        assert abs(restored[1] - 3.14) < 1e-10
        assert restored[2] == "test"
        assert restored[3] is True
        assert restored[4] == date(2026, 1, 1)

    def test_size_calculation(self, user_schema):
        row = [1, "Alice", True]
        expected_size = len(serialize_row(row, user_schema))
        calculated = serialized_row_size(row, user_schema)
        assert calculated == expected_size

    def test_empty_string(self, user_schema):
        row = [1, "", True]
        data = serialize_row(row, user_schema)
        restored, _ = deserialize_row(data, user_schema)
        assert restored == [1, "", True]


# ═══════════════════════════════════════════════════════════════════════════
# 4. Page Tests — Core Verification Criteria
# ═══════════════════════════════════════════════════════════════════════════

class TestPage:
    """Test page create / read / write, tuple operations, free space."""

    # ── Page Create / Read / Write ──────────────────────────────────

    def test_create_empty_page(self):
        page = Page(page_id=1)
        assert page.page_id == 1
        assert page.num_slots == 0
        assert page.free_space > 0

    def test_page_serialization(self):
        """Page create → serialize → deserialize."""
        page = Page(page_id=5)
        data = page.to_bytes()
        assert len(data) == PAGE_SIZE
        restored = Page(page_id=5, data=data)
        assert restored.page_id == 5
        assert restored.num_slots == 0

    def test_page_wrong_size_rejected(self):
        with pytest.raises(ValueError):
            Page(page_id=0, data=b"\x00" * 100)

    # ── Tuple Insert / Fetch by RID ─────────────────────────────────

    def test_insert_and_get_tuple(self):
        page = Page(page_id=1)
        slot = page.insert_tuple(b"hello world")
        assert slot == 0
        data = page.get_tuple(slot)
        assert data == b"hello world"

    def test_insert_multiple_tuples(self):
        page = Page(page_id=1)
        ids = []
        for i in range(5):
            sid = page.insert_tuple(f"tuple_{i}".encode())
            ids.append(sid)
        assert ids == [0, 1, 2, 3, 4]
        for i, sid in enumerate(ids):
            assert page.get_tuple(sid) == f"tuple_{i}".encode()

    def test_get_invalid_slot(self):
        page = Page(page_id=1)
        assert page.get_tuple(0) is None
        assert page.get_tuple(-1) is None
        assert page.get_tuple(99) is None

    # ── Tuple Delete ────────────────────────────────────────────────

    def test_delete_tuple(self):
        page = Page(page_id=1)
        sid = page.insert_tuple(b"data")
        assert page.delete_tuple(sid) is True
        assert page.get_tuple(sid) is None

    def test_delete_already_deleted(self):
        page = Page(page_id=1)
        sid = page.insert_tuple(b"data")
        page.delete_tuple(sid)
        assert page.delete_tuple(sid) is False

    def test_delete_invalid_slot(self):
        page = Page(page_id=1)
        assert page.delete_tuple(0) is False

    # ── Page Full Handling ──────────────────────────────────────────

    def test_page_full(self):
        page = Page(page_id=1)
        # Fill the page with tuples until it's full
        count = 0
        while page.can_fit(100):
            page.insert_tuple(b"x" * 100)
            count += 1
        assert count > 0
        with pytest.raises(ValueError):
            page.insert_tuple(b"x" * 100)

    def test_can_fit_reports_correctly(self):
        page = Page(page_id=1)
        # A fresh page has ~4072 bytes of free space
        assert page.can_fit(4000) is True
        assert page.can_fit(PAGE_SIZE) is False  # Can never fit a full page

    # ── Free Space Reuse ────────────────────────────────────────────

    def test_slot_reuse_after_delete(self):
        page = Page(page_id=1)
        s0 = page.insert_tuple(b"first")
        s1 = page.insert_tuple(b"second")
        page.delete_tuple(s0)

        # New insert should reuse slot 0
        s2 = page.insert_tuple(b"third")
        assert s2 == s0  # Reused deleted slot
        assert page.get_tuple(s2) == b"third"
        assert page.get_tuple(s1) == b"second"

    def test_compaction(self):
        page = Page(page_id=1)
        for i in range(10):
            page.insert_tuple(f"tuple_{i:04d}".encode())

        free_before = page.free_space
        # Delete half the tuples
        for i in range(0, 10, 2):
            page.delete_tuple(i)

        # Compact
        page.compact()

        # Free space should increase after compaction
        assert page.free_space > free_before

        # Remaining tuples should still be accessible
        for i in range(1, 10, 2):
            data = page.get_tuple(i)
            assert data == f"tuple_{i:04d}".encode()

    # ── Checksum ────────────────────────────────────────────────────

    def test_checksum_verification(self):
        page = Page(page_id=1)
        page.insert_tuple(b"test data")
        data = page.to_bytes()
        restored = Page(page_id=1, data=data)
        assert restored.verify_checksum() is True

    # ── Live Tuple Count ────────────────────────────────────────────

    def test_live_tuple_count(self):
        page = Page(page_id=1)
        page.insert_tuple(b"a")
        page.insert_tuple(b"b")
        page.insert_tuple(b"c")
        assert page.live_tuple_count() == 3
        page.delete_tuple(1)
        assert page.live_tuple_count() == 2

    def test_get_all_tuples(self):
        page = Page(page_id=1)
        page.insert_tuple(b"a")
        page.insert_tuple(b"b")
        page.insert_tuple(b"c")
        page.delete_tuple(1)
        tuples = page.get_all_tuples()
        assert len(tuples) == 2
        assert tuples[0] == (0, b"a")
        assert tuples[1] == (2, b"c")


# ═══════════════════════════════════════════════════════════════════════════
# 5. RID Tests
# ═══════════════════════════════════════════════════════════════════════════

class TestRID:
    """Test Record ID creation, serialization, equality."""

    def test_rid_creation(self):
        rid = RID(page_id=1, slot_id=5)
        assert rid.page_id == 1
        assert rid.slot_id == 5

    def test_rid_serialization(self):
        rid = RID(page_id=100, slot_id=42)
        data = rid.to_bytes()
        assert len(data) == 6
        restored = RID.from_bytes(data)
        assert restored == rid

    def test_rid_equality(self):
        assert RID(1, 2) == RID(1, 2)
        assert RID(1, 2) != RID(1, 3)
        assert RID(1, 2) != RID(2, 2)

    def test_rid_hash(self):
        s = {RID(1, 0), RID(1, 1), RID(1, 0)}
        assert len(s) == 2


# ═══════════════════════════════════════════════════════════════════════════
# 6. Buffer Manager Tests
# ═══════════════════════════════════════════════════════════════════════════

class TestBufferManager:
    """Test LRU page cache, pin/unpin, dirty tracking, eviction."""

    def test_put_and_get(self):
        buf = BufferManager(capacity=4)
        page = Page(page_id=1)
        buf.put_page("test.tbl", 1, page)
        cached = buf.get_page("test.tbl", 1)
        assert cached is page

    def test_cache_miss(self):
        buf = BufferManager(capacity=4)
        assert buf.get_page("test.tbl", 99) is None

    def test_dirty_tracking(self):
        buf = BufferManager(capacity=4)
        page = Page(page_id=1)
        buf.put_page("test.tbl", 1, page, dirty=True)
        assert buf.is_dirty("test.tbl", 1) is True

    def test_pin_prevents_eviction(self):
        buf = BufferManager(capacity=2)
        p1 = Page(page_id=1)
        p2 = Page(page_id=2)
        buf.put_page("t.tbl", 1, p1)
        buf.pin("t.tbl", 1)
        buf.put_page("t.tbl", 2, p2)
        buf.pin("t.tbl", 2)

        # Now try to add a 3rd page — should fail since both are pinned
        with pytest.raises(RuntimeError, match="all pages are pinned"):
            buf.put_page("t.tbl", 3, Page(page_id=3))

    def test_lru_eviction(self):
        buf = BufferManager(capacity=2)
        buf.put_page("t.tbl", 1, Page(page_id=1))
        buf.put_page("t.tbl", 2, Page(page_id=2))

        # Access page 1 to make it most-recent
        buf.get_page("t.tbl", 1)

        # Add page 3 — page 2 should be evicted (LRU)
        buf.put_page("t.tbl", 3, Page(page_id=3))
        assert buf.get_page("t.tbl", 2) is None
        assert buf.get_page("t.tbl", 1) is not None

    def test_flush_all(self):
        buf = BufferManager(capacity=4)
        buf.put_page("t.tbl", 1, Page(page_id=1), dirty=True)
        buf.put_page("t.tbl", 2, Page(page_id=2), dirty=True)
        buf.put_page("t.tbl", 3, Page(page_id=3), dirty=False)
        dirty = buf.flush_all()
        assert len(dirty) == 2
        # After flush, pages should not be dirty
        assert buf.is_dirty("t.tbl", 1) is False

    def test_eviction_returns_dirty_page(self):
        buf = BufferManager(capacity=1)
        buf.put_page("t.tbl", 1, Page(page_id=1), dirty=True)
        evicted = buf.put_page("t.tbl", 2, Page(page_id=2))
        assert evicted is not None  # dirty page evicted
        assert evicted[1] == 1  # page_id of evicted page

    def test_stats(self):
        buf = BufferManager(capacity=8)
        buf.put_page("t.tbl", 1, Page(page_id=1), dirty=True)
        buf.pin("t.tbl", 1)
        s = buf.stats()
        assert s["used"] == 1
        assert s["pinned"] == 1
        assert s["dirty"] == 1
        assert s["capacity"] == 8


# ═══════════════════════════════════════════════════════════════════════════
# 7. Table File Tests — Full CRUD + Persistence
# ═══════════════════════════════════════════════════════════════════════════

class TestTableFile:
    """Test table file operations: create, insert, fetch, delete, scan, persistence."""

    def _make_table(self, tmp_dir, schema):
        """Helper: create and open a table."""
        path = os.path.join(tmp_dir, "test.tbl")
        tbl = TableFile(path)
        tbl.create("test_table", schema)
        return tbl

    # ── Insert and Fetch by RID ─────────────────────────────────────

    def test_insert_and_get(self, tmp_dir, user_schema):
        tbl = self._make_table(tmp_dir, user_schema)
        rid = tbl.insert_row([1, "Alice", True])
        row = tbl.get_row(rid)
        assert row == [1, "Alice", True]
        tbl.close()

    def test_insert_with_nulls(self, tmp_dir, user_schema):
        tbl = self._make_table(tmp_dir, user_schema)
        rid = tbl.insert_row([2, None, None])
        row = tbl.get_row(rid)
        assert row == [2, None, None]
        tbl.close()

    def test_insert_validation(self, tmp_dir, user_schema):
        tbl = self._make_table(tmp_dir, user_schema)
        with pytest.raises(ValueError, match="validation"):
            tbl.insert_row([1, "Alice"])  # Too few columns
        tbl.close()

    def test_insert_null_violation(self, tmp_dir, user_schema):
        tbl = self._make_table(tmp_dir, user_schema)
        with pytest.raises(ValueError, match="NULL"):
            tbl.insert_row([None, "Alice", True])  # id is NOT NULL
        tbl.close()

    # ── Delete ──────────────────────────────────────────────────────

    def test_delete_row(self, tmp_dir, user_schema):
        tbl = self._make_table(tmp_dir, user_schema)
        rid = tbl.insert_row([1, "Alice", True])
        assert tbl.delete_row(rid) is True
        assert tbl.get_row(rid) is None
        tbl.close()

    def test_delete_nonexistent(self, tmp_dir, user_schema):
        tbl = self._make_table(tmp_dir, user_schema)
        assert tbl.delete_row(RID(99, 0)) is False
        tbl.close()

    # ── Full Table Scan ─────────────────────────────────────────────

    def test_scan(self, tmp_dir, user_schema):
        tbl = self._make_table(tmp_dir, user_schema)
        expected = []
        for i in range(10):
            rid = tbl.insert_row([i, f"user_{i}", i % 2 == 0])
            expected.append((rid, [i, f"user_{i}", i % 2 == 0]))

        results = list(tbl.scan())
        assert len(results) == 10

        for (exp_rid, exp_vals), (act_rid, act_vals) in zip(expected, results):
            assert exp_rid == act_rid
            assert exp_vals == act_vals
        tbl.close()

    def test_scan_with_deletes(self, tmp_dir, user_schema):
        tbl = self._make_table(tmp_dir, user_schema)
        rids = [tbl.insert_row([i, f"u{i}", True]) for i in range(5)]
        tbl.delete_row(rids[1])
        tbl.delete_row(rids[3])
        results = list(tbl.scan())
        assert len(results) == 3
        tbl.close()

    def test_row_count(self, tmp_dir, user_schema):
        tbl = self._make_table(tmp_dir, user_schema)
        for i in range(7):
            tbl.insert_row([i, f"u{i}", True])
        assert tbl.row_count() == 7
        tbl.close()

    # ── Multiple Pages Per Table ────────────────────────────────────

    def test_multiple_pages(self, tmp_dir, user_schema):
        """Insert enough rows to span multiple pages."""
        tbl = self._make_table(tmp_dir, user_schema)
        n = 200  # Should easily span 2+ pages with STRING data
        rids = []
        for i in range(n):
            rid = tbl.insert_row([i, f"user_{i:04d}_padding_data", i % 2 == 0])
            rids.append(rid)

        # Verify data pages > 1
        assert tbl.num_data_pages > 1, f"Expected >1 data pages, got {tbl.num_data_pages}"

        # Verify all rows accessible
        for i, rid in enumerate(rids):
            row = tbl.get_row(rid)
            assert row is not None, f"Row {i} at {rid} is None"
            assert row[0] == i

        tbl.close()

    # ── Persistence After Restart ───────────────────────────────────

    def test_persistence(self, tmp_dir, user_schema):
        """Data survives close → reopen."""
        path = os.path.join(tmp_dir, "persist.tbl")

        # Write
        tbl = TableFile(path)
        tbl.create("persist_test", user_schema)
        rids = []
        for i in range(20):
            rid = tbl.insert_row([i, f"user_{i}", True])
            rids.append(rid)
        tbl.close()

        # Reset buffer to simulate fresh start
        reset_buffer_manager()

        # Read
        tbl2 = TableFile(path)
        tbl2.open()

        assert tbl2.table_name == "persist_test"
        assert tbl2.schema.column_count == 3

        for i, rid in enumerate(rids):
            row = tbl2.get_row(rid)
            assert row is not None, f"Row {i} missing after restart"
            assert row[0] == i
            assert row[1] == f"user_{i}"

        tbl2.close()

    def test_persistence_with_deletes(self, tmp_dir, user_schema):
        """Deletes persist across restart."""
        path = os.path.join(tmp_dir, "deletes.tbl")

        tbl = TableFile(path)
        tbl.create("del_test", user_schema)
        r1 = tbl.insert_row([1, "Alice", True])
        r2 = tbl.insert_row([2, "Bob", False])
        r3 = tbl.insert_row([3, "Charlie", True])
        tbl.delete_row(r2)
        tbl.close()

        reset_buffer_manager()

        tbl2 = TableFile(path)
        tbl2.open()
        assert tbl2.get_row(r1) == [1, "Alice", True]
        assert tbl2.get_row(r2) is None  # deleted
        assert tbl2.get_row(r3) == [3, "Charlie", True]
        assert tbl2.row_count() == 2
        tbl2.close()

    # ── Buffer Flush Correctness ────────────────────────────────────

    def test_buffer_flush(self, tmp_dir, user_schema):
        """Explicit flush makes data persistent without closing."""
        path = os.path.join(tmp_dir, "flush.tbl")

        tbl = TableFile(path)
        tbl.create("flush_test", user_schema)
        rid = tbl.insert_row([1, "test", True])
        tbl.flush()

        # Read directly from disk to verify
        reset_buffer_manager()
        tbl2 = TableFile(path)
        tbl2.open()
        row = tbl2.get_row(rid)
        assert row == [1, "test", True]
        tbl2.close()

    # ── Update ──────────────────────────────────────────────────────

    def test_update_row(self, tmp_dir, user_schema):
        tbl = self._make_table(tmp_dir, user_schema)
        rid = tbl.insert_row([1, "Alice", True])
        assert tbl.update_row(rid, [1, "Updated", False]) is True
        row = tbl.get_row(rid)
        assert row == [1, "Updated", False]
        tbl.close()

    # ── All Five Data Types ─────────────────────────────────────────

    def test_all_data_types(self, tmp_dir, full_schema):
        """Test persistence with all 5 data types."""
        path = os.path.join(tmp_dir, "alltype.tbl")
        tbl = TableFile(path)
        tbl.create("alltype", full_schema)

        rid = tbl.insert_row([1, 3.14, "hello", True, date(2026, 1, 1)])
        tbl.close()

        reset_buffer_manager()

        tbl2 = TableFile(path)
        tbl2.open()
        row = tbl2.get_row(rid)
        assert row[0] == 1
        assert abs(row[1] - 3.14) < 1e-10
        assert row[2] == "hello"
        assert row[3] is True
        assert row[4] == date(2026, 1, 1)
        tbl2.close()


# ═══════════════════════════════════════════════════════════════════════════
# 8. Catalog Tests — Survives Restart
# ═══════════════════════════════════════════════════════════════════════════

class TestCatalog:
    """Test catalog metadata persistence."""

    def test_create_and_list_table(self, tmp_dir, user_schema):
        cat = Catalog(tmp_dir)
        cat.load()
        cat.create_table("users", user_schema)
        assert "users" in cat.list_tables()

    def test_table_exists(self, tmp_dir, user_schema):
        cat = Catalog(tmp_dir)
        cat.load()
        cat.create_table("users", user_schema)
        assert cat.table_exists("users") is True
        assert cat.table_exists("nonexistent") is False

    def test_duplicate_table(self, tmp_dir, user_schema):
        cat = Catalog(tmp_dir)
        cat.load()
        cat.create_table("users", user_schema)
        with pytest.raises(ValueError, match="already exists"):
            cat.create_table("users", user_schema)

    def test_drop_table(self, tmp_dir, user_schema):
        cat = Catalog(tmp_dir)
        cat.load()
        cat.create_table("users", user_schema)
        result = cat.drop_table("users")
        assert result == "users.tbl"
        assert "users" not in cat.list_tables()

    def test_get_schema(self, tmp_dir, user_schema):
        cat = Catalog(tmp_dir)
        cat.load()
        cat.create_table("users", user_schema)
        schema = cat.get_table_schema("users")
        assert schema is not None
        assert schema.column_count == 3
        assert schema.column_names() == ["id", "name", "active"]

    def test_catalog_survives_restart(self, tmp_dir, user_schema):
        """Catalog data persists after close and reopen."""
        cat1 = Catalog(tmp_dir)
        cat1.load()
        cat1.create_table("users", user_schema)
        cat1.create_table("orders", Schema(columns=[
            Column("order_id", DataType.INT, nullable=False),
            Column("total", DataType.FLOAT, nullable=True),
        ]))

        # "Restart" — new catalog instance
        cat2 = Catalog(tmp_dir)
        cat2.load()
        assert sorted(cat2.list_tables()) == ["orders", "users"]
        schema = cat2.get_table_schema("users")
        assert schema.column_count == 3
        assert schema.column_names() == ["id", "name", "active"]

    def test_index_operations(self, tmp_dir, user_schema):
        cat = Catalog(tmp_dir)
        cat.load()
        cat.create_table("users", user_schema)
        cat.create_index("idx_users_id", "users", "id")
        indexes = cat.get_indexes_for_table("users")
        assert len(indexes) == 1
        assert indexes[0]["column"] == "id"

    def test_drop_table_removes_indexes(self, tmp_dir, user_schema):
        cat = Catalog(tmp_dir)
        cat.load()
        cat.create_table("users", user_schema)
        cat.create_index("idx_users_id", "users", "id")
        cat.drop_table("users")
        assert cat.list_indexes() == []


# ═══════════════════════════════════════════════════════════════════════════
# 9. Hardening Tests — Engineering Review Checklist
# ═══════════════════════════════════════════════════════════════════════════

class TestPageHardening:
    """Tests for page consistency guarantees added during engineering review."""

    def test_crc_validated_on_read(self):
        """Corrupt page data should raise PageCorruptionError on load."""
        page = Page(page_id=1)
        page.insert_tuple(b"important data")
        data = bytearray(page.to_bytes())

        # Corrupt a byte in the tuple region
        data[PAGE_SIZE - 1] ^= 0xFF  # flip bits

        with pytest.raises(PageCorruptionError, match="CRC mismatch"):
            Page(page_id=1, data=bytes(data))

    def test_crc_skip_verification(self):
        """Pages can be loaded without CRC check if verify=False."""
        page = Page(page_id=1)
        page.insert_tuple(b"data")
        data = bytearray(page.to_bytes())
        data[PAGE_SIZE - 1] ^= 0xFF  # corrupt

        # Should NOT raise with verify=False
        loaded = Page(page_id=1, data=bytes(data), verify=False)
        assert loaded.page_id == 1

    def test_structural_invariant_free_space_overlap(self):
        """Page rejects data where free_start is corrupted."""
        page = Page(page_id=1)
        data = bytearray(page.to_bytes())

        # Manually corrupt: set free_start to PAGE_SIZE (impossible value)
        import struct as st
        # free_start is at header offset: H(2)+I(4)+H(2) = offset 8, size 2
        st.pack_into(">H", data, 8, PAGE_SIZE)  # free_start = PAGE_SIZE
        # Zero out the CRC so it's skipped
        st.pack_into(">I", data, 14, 0)

        with pytest.raises(PageCorruptionError):
            Page(page_id=1, data=bytes(data))

    def test_compaction_preserves_rids(self):
        """Compaction must not change slot IDs — only physical offsets."""
        page = Page(page_id=1)
        page.insert_tuple(b"aaa")  # slot 0
        page.insert_tuple(b"bbb")  # slot 1
        page.insert_tuple(b"ccc")  # slot 2
        page.delete_tuple(1)       # delete slot 1

        page.compact()

        # Slots 0 and 2 must still work
        assert page.get_tuple(0) == b"aaa"
        assert page.get_tuple(1) is None  # still deleted
        assert page.get_tuple(2) == b"ccc"

    def test_endianness_big_endian(self):
        """All multi-byte fields use big-endian (network byte order)."""
        import struct as st
        rid = RID(page_id=256, slot_id=1)
        raw = rid.to_bytes()
        # Big-endian: 256 = 0x00000100, so byte[2] should be 0x01
        assert raw == st.pack(">IH", 256, 1)

    def test_update_preserves_rid_on_growth(self):
        """Update that needs more space still keeps the same RID."""
        page = Page(page_id=1)
        s = page.insert_tuple(b"short")
        assert page.update_tuple(s, b"this is a much longer string that requires more space") is True
        assert page.get_tuple(s) == b"this is a much longer string that requires more space"
        # Slot ID unchanged
        assert s == 0


class TestBufferHardening:
    """Tests for buffer manager safety guarantees."""

    def test_single_frame_invariant(self):
        """Same (file, page) put twice updates in place, no duplication."""
        buf = BufferManager(capacity=4)
        p1 = Page(page_id=1)
        p2 = Page(page_id=1)  # same page_id, different object

        buf.put_page("t.tbl", 1, p1)
        buf.put_page("t.tbl", 1, p2)  # should update, not add

        assert buf.size == 1
        assert buf.get_page("t.tbl", 1) is p2

    def test_flush_all_and_clear(self):
        """Shutdown method returns dirty pages and empties cache."""
        buf = BufferManager(capacity=4)
        buf.put_page("t.tbl", 1, Page(page_id=1), dirty=True)
        buf.put_page("t.tbl", 2, Page(page_id=2), dirty=False)

        dirty = buf.flush_all_and_clear()
        assert len(dirty) == 1
        assert dirty[0][1] == 1  # page_id
        assert buf.size == 0

    def test_dirty_merged_on_put(self):
        """If a page is already dirty and put again, dirty stays True."""
        buf = BufferManager(capacity=4)
        buf.put_page("t.tbl", 1, Page(page_id=1), dirty=True)
        buf.put_page("t.tbl", 1, Page(page_id=1), dirty=False)  # update
        assert buf.is_dirty("t.tbl", 1) is True  # dirty flag preserved


class TestCatalogHardening:
    """Tests for catalog safety guarantees."""

    def test_catalog_atomic_write_survives(self, tmp_dir, user_schema):
        """Catalog file is written atomically — no temp files left behind."""
        cat = Catalog(tmp_dir)
        cat.load()
        cat.create_table("users", user_schema)

        # Check no temp files left
        files = os.listdir(tmp_dir)
        tmp_files = [f for f in files if f.endswith(".tmp")]
        assert len(tmp_files) == 0, f"Temp files left: {tmp_files}"

        # Verify catalog.dat exists and is valid
        assert os.path.exists(os.path.join(tmp_dir, "catalog.dat"))

    def test_catalog_format_version(self, tmp_dir, user_schema):
        """Catalog contains format version for forward compatibility."""
        cat = Catalog(tmp_dir)
        cat.load()
        cat.create_table("users", user_schema)

        with open(os.path.join(tmp_dir, "catalog.dat"), "r") as f:
            data = json.load(f)
        assert data["format_version"] == 1
        assert "schema_evolution" in data


class TestScanDeterminism:
    """Verify full table scan produces deterministic order."""

    def test_deterministic_scan_order(self, tmp_dir, user_schema):
        """Two scans produce identical ordering."""
        path = os.path.join(tmp_dir, "order.tbl")
        tbl = TableFile(path)
        tbl.create("order_test", user_schema)

        for i in range(50):
            tbl.insert_row([i, f"user_{i}", True])

        scan1 = [(r.page_id, r.slot_id, v[0]) for r, v in tbl.scan()]
        scan2 = [(r.page_id, r.slot_id, v[0]) for r, v in tbl.scan()]
        assert scan1 == scan2

        tbl.close()

# ═══════════════════════════════════════════════════════════════════════════
# 9. Integration Test — Full Workflow
# ═══════════════════════════════════════════════════════════════════════════

class TestIntegration:
    """End-to-end test: catalog + table + CRUD + persistence."""

    def test_full_workflow(self, tmp_dir):
        """Create catalog, create table, insert, query, delete, restart."""
        schema = Schema(columns=[
            Column("id", DataType.INT, nullable=False),
            Column("name", DataType.STRING, nullable=False),
            Column("score", DataType.FLOAT, nullable=True),
            Column("active", DataType.BOOLEAN, nullable=True),
            Column("joined", DataType.DATE, nullable=True),
        ])

        # Phase 1: Create and populate
        cat = Catalog(tmp_dir)
        cat.load()
        file_name = cat.create_table("students", schema)
        file_path = cat.get_table_file("students")

        tbl = TableFile(file_path)
        tbl.create("students", schema)

        rids = []
        rows_data = [
            [1, "Alice", 95.5, True, date(2025, 9, 1)],
            [2, "Bob", 82.3, True, date(2025, 9, 1)],
            [3, "Charlie", None, False, None],
            [4, "Diana", 91.0, True, date(2026, 1, 15)],
        ]
        for row in rows_data:
            rids.append(tbl.insert_row(row))

        # Verify scan
        results = list(tbl.scan())
        assert len(results) == 4

        # Delete one
        tbl.delete_row(rids[2])  # Charlie
        assert tbl.row_count() == 3

        tbl.close()

        # Phase 2: Restart and verify
        reset_buffer_manager()

        cat2 = Catalog(tmp_dir)
        cat2.load()
        assert cat2.table_exists("students")
        schema2 = cat2.get_table_schema("students")
        assert schema2.column_count == 5

        tbl2 = TableFile(file_path)
        tbl2.open()

        # Verify data persisted
        assert tbl2.get_row(rids[0]) == rows_data[0]
        assert tbl2.get_row(rids[1]) == rows_data[1]
        assert tbl2.get_row(rids[2]) is None  # deleted
        assert tbl2.get_row(rids[3]) == rows_data[3]

        # Verify scan after restart
        results2 = list(tbl2.scan())
        assert len(results2) == 3

        tbl2.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
