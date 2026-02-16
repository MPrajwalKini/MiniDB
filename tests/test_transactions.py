"""
MiniDB Phase 6 Transaction Tests
=================================
Comprehensive test suite for WAL, TransactionManager, and RecoveryManager.
"""

import os
import sys
import shutil
import struct
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage.page import Page, PAGE_SIZE, RID
from storage.buffer import BufferManager
from storage.table import TableFile
from storage.schema import Schema, Column
from storage.types import DataType
from storage.serializer import serialize_row, deserialize_row
from catalog.catalog import Catalog
from transactions.wal import LogManager, WALRecordType, WALEntry, NULL_LSN, WAL_PADDING
from transactions.transaction import TransactionManager, TransactionState
from transactions.recovery import RecoveryManager
from execution.context import ExecutionContext
from execution.executor import Executor


def _make_tmp():
    return tempfile.mkdtemp(prefix="minidb_txn_test_")


def _make_schema():
    return Schema(columns=[
        Column("id", DataType.INT, nullable=False),
        Column("name", DataType.STRING, max_length=50),
    ])


def _setup_db(tmp_dir):
    """Create a full database environment for testing."""
    cat = Catalog(tmp_dir)
    bm = BufferManager(capacity=32)
    lm = LogManager(tmp_dir)
    tm = TransactionManager(lm, bm, tmp_dir)
    ctx = ExecutionContext(
        catalog=cat,
        buffer_manager=bm,
        base_path=tmp_dir,
        txn_manager=tm,
        log_manager=lm,
    )
    return cat, bm, lm, tm, ctx


# ═══════════════════════════════════════════════════════════════════════════
# 1. WAL Unit Tests
# ═══════════════════════════════════════════════════════════════════════════

class TestWALBasics(unittest.TestCase):
    """Test LogManager record writing, scanning, and CRC verification."""

    def setUp(self):
        self.tmp = _make_tmp()
        self.lm = LogManager(self.tmp)

    def tearDown(self):
        self.lm.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_begin_commit_records(self):
        """BEGIN and COMMIT produce valid records with correct types."""
        lsn1 = self.lm.append_begin(1)
        lsn2 = self.lm.append_commit(1, lsn1)
        recs = list(self.lm.scan())
        self.assertEqual(len(recs), 2)
        self.assertEqual(recs[0].record_type, WALRecordType.BEGIN)
        self.assertEqual(recs[1].record_type, WALRecordType.COMMIT)
        self.assertEqual(recs[0].txn_id, 1)
        self.assertEqual(recs[1].prev_lsn, lsn1)

    def test_insert_delete_payloads(self):
        """INSERT/DELETE payloads round-trip correctly."""
        lsn = self.lm.append_insert(1, NULL_LSN, "users", 5, 3, b"test_data")
        rec = self.lm.read_record(lsn)
        tname, pid, sid, data = LogManager.parse_dml_payload(rec.payload)
        self.assertEqual(tname, "users")
        self.assertEqual(pid, 5)
        self.assertEqual(sid, 3)
        self.assertEqual(data, b"test_data")

    def test_update_payload(self):
        """UPDATE payload stores both old and new images."""
        lsn = self.lm.append_update(1, NULL_LSN, "t", 1, 0,
                                     b"old_val", b"new_val")
        rec = self.lm.read_record(lsn)
        tname, pid, sid, old, new = LogManager.parse_update_payload(rec.payload)
        self.assertEqual(old, b"old_val")
        self.assertEqual(new, b"new_val")

    def test_lsn_is_byte_offset(self):
        """LSN equals byte offset in WAL file."""
        lsn1 = self.lm.append_begin(1)
        self.assertEqual(lsn1, WAL_PADDING)
        rec = self.lm.read_record(lsn1)
        self.assertEqual(rec.lsn, lsn1)
        lsn2 = self.lm.append_commit(1, lsn1)
        self.assertEqual(lsn2, lsn1 + rec.total_len)

    def test_crc_corruption_detected(self):
        """Corrupted CRC raises ValueError on read."""
        lsn = self.lm.append_begin(1)
        self.lm.flush()
        # Corrupt last byte of the record (CRC region)
        wal_path = os.path.join(self.tmp, "wal.log")
        with open(wal_path, "r+b") as f:
            f.seek(lsn + 20)  # Some byte in the record
            f.write(b"\xFF")
        self.lm.close()
        lm2 = LogManager(self.tmp)
        with self.assertRaises(ValueError):
            lm2.read_record(lsn)
        lm2.close()

    def test_clr_payload(self):
        """CLR stores undo_next_lsn and inner operation."""
        lsn = self.lm.append_clr(1, NULL_LSN, 42,
                                  WALRecordType.DELETE, b"clr_inner")
        rec = self.lm.read_record(lsn)
        self.assertEqual(rec.record_type, WALRecordType.CLR)
        undo_next, inner_type, inner_payload = LogManager.parse_clr_payload(rec.payload)
        self.assertEqual(undo_next, 42)
        self.assertEqual(inner_type, WALRecordType.DELETE)
        self.assertEqual(inner_payload, b"clr_inner")

    def test_checkpoint_payload(self):
        """CHECKPOINT stores active transaction list."""
        lsn = self.lm.append_checkpoint([(10, 100), (20, 200)])
        rec = self.lm.read_record(lsn)
        txns = LogManager.parse_checkpoint_payload(rec.payload)
        self.assertEqual(txns, [(10, 100), (20, 200)])

    def test_reopen_persistence(self):
        """Records survive close + reopen."""
        self.lm.append_begin(1)
        self.lm.append_insert(1, NULL_LSN, "t", 0, 0, b"data")
        self.lm.close()
        lm2 = LogManager(self.tmp)
        recs = list(lm2.scan())
        self.assertEqual(len(recs), 2)
        lm2.close()

    def test_truncate(self):
        """Truncating WAL removes records."""
        self.lm.append_begin(1)
        self.lm.append_commit(1, NULL_LSN)
        self.lm.truncate(WAL_PADDING)
        recs = list(self.lm.scan())
        self.assertEqual(len(recs), 0)

    def test_durable_lsn_tracking(self):
        """durable_lsn updates after flush."""
        initial = self.lm.durable_lsn
        self.lm.append_begin(1)
        # Before flush, durable_lsn hasn't changed
        # (append_begin doesn't flush)
        # After commit (which flushes), durable_lsn should advance
        self.lm.append_commit(1, NULL_LSN)
        self.assertGreater(self.lm.durable_lsn, initial)


# ═══════════════════════════════════════════════════════════════════════════
# 2. Page LSN Tests
# ═══════════════════════════════════════════════════════════════════════════

class TestPageLSN(unittest.TestCase):
    """Test page_lsn field in page header."""

    def test_fresh_page_lsn_zero(self):
        """New page has page_lsn = 0."""
        p = Page(page_id=1)
        self.assertEqual(p.page_lsn, 0)

    def test_set_page_lsn(self):
        """Setting page_lsn updates the header."""
        p = Page(page_id=1)
        p.page_lsn = 42
        self.assertEqual(p.page_lsn, 42)
        # Verify it survives serialization
        data = p.to_bytes()
        p2 = Page(data=data)
        self.assertEqual(p2.page_lsn, 42)

    def test_page_lsn_survives_insert(self):
        """page_lsn persists through tuple operations."""
        p = Page(page_id=1)
        p.page_lsn = 100
        p.insert_tuple(b"hello")
        self.assertEqual(p.page_lsn, 100)
        data = p.to_bytes()
        p2 = Page(data=data)
        self.assertEqual(p2.page_lsn, 100)

    def test_restore_tuple(self):
        """restore_tuple re-inserts at a deleted slot."""
        p = Page(page_id=1)
        sid = p.insert_tuple(b"original")
        p.delete_tuple(sid)
        self.assertIsNone(p.get_tuple(sid))
        ok = p.restore_tuple(sid, b"restored")
        self.assertTrue(ok)
        self.assertEqual(p.get_tuple(sid), b"restored")


# ═══════════════════════════════════════════════════════════════════════════
# 3. Transaction Manager Tests
# ═══════════════════════════════════════════════════════════════════════════

class TestTransactionManager(unittest.TestCase):
    """Test begin/commit/abort with WAL."""

    def setUp(self):
        self.tmp = _make_tmp()
        self.lm = LogManager(self.tmp)
        self.tm = TransactionManager(self.lm, data_dir=self.tmp)

    def tearDown(self):
        self.lm.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_begin_returns_unique_ids(self):
        """Each begin() returns a unique, increasing txn_id."""
        t1 = self.tm.begin()
        self.tm.commit(t1)
        t2 = self.tm.begin()
        self.assertGreater(t2, t1)
        self.tm.commit(t2)

    def test_multi_writer_allowed(self):
        """Multiple concurrent transactions are allowed (Phase 7 upgrade)."""
        t1 = self.tm.begin()
        t2 = self.tm.begin()  # Should NOT raise
        self.assertNotEqual(t1, t2)
        self.assertTrue(self.tm.is_active(t1))
        self.assertTrue(self.tm.is_active(t2))
        self.tm.commit(t1)
        self.tm.commit(t2)

    def test_commit_writes_wal(self):
        """Commit produces BEGIN + COMMIT in WAL."""
        tid = self.tm.begin()
        self.tm.commit(tid)
        recs = list(self.lm.scan())
        types = [r.record_type for r in recs]
        self.assertEqual(types, [WALRecordType.BEGIN, WALRecordType.COMMIT])

    def test_abort_writes_wal(self):
        """Abort produces BEGIN + ABORT in WAL."""
        tid = self.tm.begin()
        self.tm.abort(tid)
        recs = list(self.lm.scan())
        types = [r.record_type for r in recs]
        self.assertEqual(types, [WALRecordType.BEGIN, WALRecordType.ABORT])

    def test_log_insert_chains_lsn(self):
        """Multiple ops chain prev_lsn correctly."""
        tid = self.tm.begin()
        lsn1 = self.tm.log_insert(tid, "test", 0, 0, b"data1")
        lsn2 = self.tm.log_insert(tid, "test", 0, 1, b"data2")
        rec2 = self.lm.read_record(lsn2)
        self.assertEqual(rec2.prev_lsn, lsn1)
        self.tm.commit(tid)

    def test_abort_with_inserts_produces_clrs(self):
        """Aborting after inserts produces CLR records in WAL."""
        tid = self.tm.begin()
        self.tm.log_insert(tid, "test", 0, 0, b"data")
        self.tm.abort(tid)
        recs = list(self.lm.scan())
        types = [r.record_type for r in recs]
        # BEGIN, INSERT, CLR (undo INSERT), ABORT
        self.assertIn(WALRecordType.CLR, types)
        self.assertEqual(types[-1], WALRecordType.ABORT)


# ═══════════════════════════════════════════════════════════════════════════
# 4. End-to-End Transaction Tests with Executor
# ═══════════════════════════════════════════════════════════════════════════

class TestTransactionE2E(unittest.TestCase):
    """End-to-end tests with full executor stack."""

    def setUp(self):
        self.tmp = _make_tmp()
        self.cat, self.bm, self.lm, self.tm, self.ctx = _setup_db(self.tmp)
        self.executor = Executor(self.ctx)

    def tearDown(self):
        self.lm.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_insert_produces_wal_records(self):
        """INSERT with active txn produces WAL INSERT record."""
        list(self.executor.execute("CREATE TABLE t1 (id INT, val STRING)"))
        # Begin a transaction
        tid = self.tm.begin()
        self.ctx.active_txn_id = tid
        list(self.executor.execute("INSERT INTO t1 VALUES (1, 'hello')"))
        self.tm.commit(tid)
        self.ctx.active_txn_id = None

        recs = list(self.lm.scan())
        types = [r.record_type for r in recs]
        self.assertIn(WALRecordType.BEGIN, types)
        self.assertIn(WALRecordType.INSERT, types)
        self.assertIn(WALRecordType.COMMIT, types)

    def test_insert_without_txn_no_wal(self):
        """INSERT without active txn produces no WAL records."""
        list(self.executor.execute("CREATE TABLE t2 (id INT)"))
        list(self.executor.execute("INSERT INTO t2 VALUES (1)"))
        recs = list(self.lm.scan())
        # No transaction records (just padding)
        self.assertEqual(len(recs), 0)

    def test_data_visible_after_commit(self):
        """Data inserted within txn is visible after commit."""
        list(self.executor.execute("CREATE TABLE t3 (id INT, name STRING)"))
        tid = self.tm.begin()
        self.ctx.active_txn_id = tid
        list(self.executor.execute("INSERT INTO t3 VALUES (1, 'alice')"))
        list(self.executor.execute("INSERT INTO t3 VALUES (2, 'bob')"))
        self.tm.commit(tid)
        self.ctx.active_txn_id = None

        rows = list(self.executor.execute_and_fetchall(
            "SELECT id, name FROM t3"))
        self.assertEqual(len(rows), 2)
        names = sorted(r['name'] for r in rows)
        self.assertEqual(names, ['alice', 'bob'])


# ═══════════════════════════════════════════════════════════════════════════
# 5. Recovery Tests
# ═══════════════════════════════════════════════════════════════════════════

class TestRecovery(unittest.TestCase):
    """Test ARIES-style recovery."""

    def test_analysis_identifies_committed(self):
        """Analysis phase correctly identifies committed txns."""
        tmp = _make_tmp()
        try:
            lm = LogManager(tmp)
            lm.append_begin(1)
            lsn_ins = lm.append_insert(1, NULL_LSN, "t", 1, 0, b"data")
            lm.append_commit(1, lsn_ins)
            lm.append_begin(2)
            lm.append_insert(2, NULL_LSN, "t", 1, 1, b"data2")
            # txn 2 has no COMMIT — it's uncommitted

            bm = BufferManager(capacity=16)
            tm = TransactionManager(lm, bm, tmp)
            rm = RecoveryManager(lm, tm, bm, tmp)

            committed, uncommitted, max_tid, _ = rm._analysis()
            self.assertIn(1, committed)
            self.assertIn(2, uncommitted)
            self.assertEqual(max_tid, 2)
            lm.close()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_empty_wal_recovery(self):
        """Recovery on empty WAL is a no-op."""
        tmp = _make_tmp()
        try:
            lm = LogManager(tmp)
            bm = BufferManager(capacity=16)
            tm = TransactionManager(lm, bm, tmp)
            rm = RecoveryManager(lm, tm, bm, tmp)
            stats = rm.recover()
            self.assertEqual(stats["committed_txns"], 0)
            self.assertEqual(stats["uncommitted_txns"], 0)
            lm.close()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
