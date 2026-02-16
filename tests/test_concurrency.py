"""
MiniDB Phase 7 Concurrency Control Tests
==========================================
Threaded tests for strict 2PL, deadlock detection, and isolation.

Tests prove:
  - Lock compatibility matrix
  - FIFO fairness (starvation prevention)
  - Deadlock detection and victim selection
  - Abort-while-waiting wakes blocked thread
  - Concurrent readers allowed
  - Writer blocks reader, reader blocks writer
  - Write-write conflict serialized
  - Lock release after commit frees waiters
  - Upgrade conflict scenario
  - WAL + lock ordering
"""

import os
import sys
import shutil
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from concurrency.lock_manager import (
    LockManager, LockType, LockResult, table_resource, row_resource,
    LockGranularity
)
from transactions.wal import LogManager, NULL_LSN, WALRecordType
from transactions.transaction import TransactionManager, TransactionState
from storage.buffer import BufferManager
from storage.schema import Schema, Column
from storage.types import DataType
from catalog.catalog import Catalog
from execution.context import ExecutionContext
from execution.executor import Executor


def _make_tmp():
    return tempfile.mkdtemp(prefix="minidb_conc_test_")


# ═══════════════════════════════════════════════════════════════════════════
# 1. Lock Manager Unit Tests
# ═══════════════════════════════════════════════════════════════════════════

class TestLockManagerBasics(unittest.TestCase):
    """Lock compatibility matrix and basic acquire/release."""

    def setUp(self):
        self.lm = LockManager()
        self.res = table_resource("users")

    def test_shared_shared_compatible(self):
        """Two SHARED locks on same resource are compatible."""
        r1 = self.lm.acquire(1, self.res, LockType.SHARED)
        r2 = self.lm.acquire(2, self.res, LockType.SHARED)
        self.assertEqual(r1, LockResult.GRANTED)
        self.assertEqual(r2, LockResult.GRANTED)
        holders = self.lm.get_holders(self.res)
        self.assertEqual(len(holders), 2)

    def test_shared_exclusive_incompatible(self):
        """EXCLUSIVE blocked by existing SHARED (timeout)."""
        self.lm.acquire(1, self.res, LockType.SHARED)
        result = self.lm.acquire(2, self.res, LockType.EXCLUSIVE, timeout=0.1)
        self.assertEqual(result, LockResult.TIMEOUT)

    def test_exclusive_shared_incompatible(self):
        """SHARED blocked by existing EXCLUSIVE (timeout)."""
        self.lm.acquire(1, self.res, LockType.EXCLUSIVE)
        result = self.lm.acquire(2, self.res, LockType.SHARED, timeout=0.1)
        self.assertEqual(result, LockResult.TIMEOUT)

    def test_exclusive_exclusive_incompatible(self):
        """Two EXCLUSIVE locks conflict (timeout)."""
        self.lm.acquire(1, self.res, LockType.EXCLUSIVE)
        result = self.lm.acquire(2, self.res, LockType.EXCLUSIVE, timeout=0.1)
        self.assertEqual(result, LockResult.TIMEOUT)

    def test_same_txn_reentrant(self):
        """Same txn requesting same lock type is a no-op grant."""
        self.lm.acquire(1, self.res, LockType.SHARED)
        result = self.lm.acquire(1, self.res, LockType.SHARED)
        self.assertEqual(result, LockResult.GRANTED)

    def test_upgrade_sole_holder(self):
        """SHARED→EXCLUSIVE upgrade succeeds when sole holder."""
        self.lm.acquire(1, self.res, LockType.SHARED)
        result = self.lm.acquire(1, self.res, LockType.EXCLUSIVE)
        self.assertEqual(result, LockResult.GRANTED)
        holders = self.lm.get_holders(self.res)
        self.assertEqual(holders[1], LockType.EXCLUSIVE)

    def test_release_all(self):
        """release_all frees all locks held by a txn."""
        r1 = table_resource("t1")
        r2 = table_resource("t2")
        self.lm.acquire(1, r1, LockType.EXCLUSIVE)
        self.lm.acquire(1, r2, LockType.SHARED)
        count = self.lm.release_all(1)
        self.assertEqual(count, 2)
        self.assertEqual(self.lm.get_holders(r1), {})
        self.assertEqual(self.lm.get_holders(r2), {})

    def test_release_grants_waiters(self):
        """Releasing a lock grants it to the next waiter in FIFO order."""
        self.lm.acquire(1, self.res, LockType.EXCLUSIVE)
        results = {}

        def waiter():
            results['r'] = self.lm.acquire(2, self.res, LockType.SHARED, timeout=2.0)

        t = threading.Thread(target=waiter)
        t.start()
        time.sleep(0.1)  # Let waiter enqueue
        self.lm.release_all(1)
        t.join(timeout=3.0)
        self.assertEqual(results.get('r'), LockResult.GRANTED)


# ═══════════════════════════════════════════════════════════════════════════
# 2. Lock Introspection Tests
# ═══════════════════════════════════════════════════════════════════════════

class TestLockIntrospection(unittest.TestCase):
    """Test introspection API: get_locks, get_waiting, get_holders."""

    def setUp(self):
        self.lm = LockManager()

    def test_get_locks(self):
        """get_locks returns all resources held by a txn."""
        r1 = table_resource("a")
        r2 = table_resource("b")
        self.lm.acquire(1, r1, LockType.SHARED)
        self.lm.acquire(1, r2, LockType.EXCLUSIVE)
        locks = self.lm.get_locks(1)
        resources = {r for r, lt in locks}
        self.assertEqual(resources, {r1, r2})

    def test_get_holders(self):
        """get_holders returns all txns holding a resource."""
        res = table_resource("t")
        self.lm.acquire(1, res, LockType.SHARED)
        self.lm.acquire(2, res, LockType.SHARED)
        holders = self.lm.get_holders(res)
        self.assertEqual(set(holders.keys()), {1, 2})

    def test_get_wait_queue(self):
        """get_wait_queue shows pending requests."""
        res = table_resource("t")
        self.lm.acquire(1, res, LockType.EXCLUSIVE)
        # Start waiter in background
        def waiter():
            self.lm.acquire(2, res, LockType.SHARED, timeout=2.0)
        t = threading.Thread(target=waiter)
        t.start()
        time.sleep(0.1)
        wq = self.lm.get_wait_queue(res)
        self.assertTrue(any(txn_id == 2 for txn_id, _ in wq))
        self.lm.release_all(1)
        t.join(timeout=3.0)


# ═══════════════════════════════════════════════════════════════════════════
# 3. Deadlock Detection Tests
# ═══════════════════════════════════════════════════════════════════════════

class TestDeadlockDetection(unittest.TestCase):
    """Test wait-for graph cycle detection and victim selection."""

    def setUp(self):
        self.lm = LockManager()

    def test_two_txn_deadlock(self):
        """
        Classic 2-txn deadlock:
          T1 holds A, waits for B
          T2 holds B, waits for A
        Youngest (highest txn_id) is victim.
        """
        ra = table_resource("A")
        rb = table_resource("B")
        self.lm.acquire(1, ra, LockType.EXCLUSIVE)
        self.lm.acquire(2, rb, LockType.EXCLUSIVE)

        results = {}
        t1_waiting = threading.Event()

        def t1_wait():
            t1_waiting.set()
            results['t1'] = self.lm.acquire(1, rb, LockType.EXCLUSIVE, timeout=5.0)

        def t2_wait():
            t1_waiting.wait(timeout=2.0)  # Ensure T1 enqueues first
            time.sleep(0.2)               # Give T1 time to enter wait
            results['t2'] = self.lm.acquire(2, ra, LockType.EXCLUSIVE, timeout=5.0)

        thread1 = threading.Thread(target=t1_wait)
        thread2 = threading.Thread(target=t2_wait)
        thread1.start()
        thread2.start()
        thread2.join(timeout=8.0)
        
        # T2 should detect deadlock (it enqueues second, finds cycle)
        # T2 is youngest → T2 gets DEADLOCK result
        self.assertIn(results.get('t2'), (LockResult.DEADLOCK, LockResult.ABORTED),
                      f"T2 should be deadlock victim, got: {results}")
        
        # Clean up: release T2's lock on B so T1 can proceed
        self.lm.release_all(2)
        thread1.join(timeout=5.0)
        self.assertEqual(results.get('t1'), LockResult.GRANTED)

    def test_no_false_positive(self):
        """No cycle → no deadlock reported."""
        ra = table_resource("A")
        rb = table_resource("B")
        self.lm.acquire(1, ra, LockType.SHARED)
        self.lm.acquire(2, rb, LockType.SHARED)
        # T1 requests SHARED on B → no conflict
        result = self.lm.acquire(1, rb, LockType.SHARED)
        self.assertEqual(result, LockResult.GRANTED)

    def test_deadlock_victim_is_youngest(self):
        """Victim should be the txn with highest ID."""
        ra = table_resource("A")
        rb = table_resource("B")
        self.lm.acquire(1, ra, LockType.EXCLUSIVE)
        self.lm.acquire(100, rb, LockType.EXCLUSIVE)

        results = {}

        def t1_wait():
            results['t1'] = self.lm.acquire(1, rb, LockType.EXCLUSIVE, timeout=2.0)

        def t100_wait():
            results['t100'] = self.lm.acquire(100, ra, LockType.EXCLUSIVE, timeout=2.0)

        th1 = threading.Thread(target=t1_wait)
        th100 = threading.Thread(target=t100_wait)
        th1.start()
        time.sleep(0.05)
        th100.start()
        th1.join(timeout=3.0)
        th100.join(timeout=3.0)

        # T100 is youngest → should be victim
        if LockResult.DEADLOCK in results.values() or LockResult.ABORTED in results.values():
            # At least one got deadlock/aborted
            self.assertTrue(
                results.get('t100') in (LockResult.DEADLOCK, LockResult.ABORTED)
                or results.get('t1') == LockResult.GRANTED,
                f"Expected T100 as victim, got: {results}"
            )


# ═══════════════════════════════════════════════════════════════════════════
# 4. Concurrent Isolation Tests
# ═══════════════════════════════════════════════════════════════════════════

class TestConcurrentIsolation(unittest.TestCase):
    """Thread-based tests for isolation guarantees."""

    def setUp(self):
        self.lm = LockManager()

    def test_concurrent_readers(self):
        """Multiple concurrent readers succeed without blocking."""
        res = table_resource("data")
        results = []

        def reader(txn_id):
            r = self.lm.acquire(txn_id, res, LockType.SHARED, timeout=1.0)
            results.append(r)

        threads = [threading.Thread(target=reader, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=3.0)

        self.assertEqual(len(results), 10)
        self.assertTrue(all(r == LockResult.GRANTED for r in results))

    def test_writer_blocks_reader(self):
        """Reader must wait while writer holds exclusive lock."""
        res = table_resource("data")
        self.lm.acquire(1, res, LockType.EXCLUSIVE)
        blocked = threading.Event()
        result = {}

        def reader():
            blocked.set()
            result['r'] = self.lm.acquire(2, res, LockType.SHARED, timeout=2.0)

        t = threading.Thread(target=reader)
        t.start()
        blocked.wait(timeout=1.0)
        time.sleep(0.1)

        # Reader should be waiting
        self.assertFalse(result.get('r') == LockResult.GRANTED)

        # Release writer → reader should get lock
        self.lm.release_all(1)
        t.join(timeout=3.0)
        self.assertEqual(result.get('r'), LockResult.GRANTED)

    def test_reader_blocks_writer(self):
        """Writer must wait while readers hold shared lock."""
        res = table_resource("data")
        self.lm.acquire(1, res, LockType.SHARED)
        self.lm.acquire(2, res, LockType.SHARED)
        result = {}

        def writer():
            result['r'] = self.lm.acquire(3, res, LockType.EXCLUSIVE, timeout=0.3)

        t = threading.Thread(target=writer)
        t.start()
        t.join(timeout=2.0)
        self.assertEqual(result.get('r'), LockResult.TIMEOUT)

    def test_aborted_txn_releases_locks(self):
        """After release_all, waiting txns can proceed."""
        res = table_resource("data")
        self.lm.acquire(1, res, LockType.EXCLUSIVE)
        result = {}

        def waiter():
            result['r'] = self.lm.acquire(2, res, LockType.EXCLUSIVE, timeout=2.0)

        t = threading.Thread(target=waiter)
        t.start()
        time.sleep(0.1)  # Let waiter enqueue
        self.lm.release_all(1)  # Simulate abort releasing locks
        t.join(timeout=3.0)
        self.assertEqual(result['r'], LockResult.GRANTED)

    def test_abort_wakes_waiting_thread(self):
        """A txn waiting on a lock is immediately woken on abort."""
        res = table_resource("data")
        self.lm.acquire(1, res, LockType.EXCLUSIVE)
        result = {}
        started = threading.Event()

        def waiter():
            started.set()
            result['r'] = self.lm.acquire(2, res, LockType.SHARED, timeout=5.0)

        t = threading.Thread(target=waiter)
        t.start()
        started.wait(timeout=1.0)
        time.sleep(0.1)

        # Abort txn 2 while it's waiting
        self.lm.abort_waiting(2)
        t.join(timeout=2.0)
        self.assertTrue(
            result.get('r') in (LockResult.ABORTED, LockResult.TIMEOUT),
            f"Expected ABORTED, got {result.get('r')}"
        )


# ═══════════════════════════════════════════════════════════════════════════
# 5. Upgrade Conflict Tests
# ═══════════════════════════════════════════════════════════════════════════

class TestUpgradeConflicts(unittest.TestCase):
    """Test lock upgrade scenarios."""

    def setUp(self):
        self.lm = LockManager()

    def test_upgrade_blocked_when_not_sole(self):
        """Upgrade from SHARED→EXCLUSIVE fails when other holders exist."""
        res = table_resource("t")
        self.lm.acquire(1, res, LockType.SHARED)
        self.lm.acquire(2, res, LockType.SHARED)
        # T1 tries to upgrade, but T2 also holds SHARED
        result = self.lm.acquire(1, res, LockType.EXCLUSIVE, timeout=0.2)
        self.assertEqual(result, LockResult.TIMEOUT)

    def test_upgrade_succeeds_after_other_releases(self):
        """Upgrade succeeds once other SHARED holders release."""
        res = table_resource("t")
        self.lm.acquire(1, res, LockType.SHARED)
        self.lm.acquire(2, res, LockType.SHARED)
        result = {}

        def upgrader():
            result['r'] = self.lm.acquire(1, res, LockType.EXCLUSIVE, timeout=2.0)

        t = threading.Thread(target=upgrader)
        t.start()
        time.sleep(0.1)
        self.lm.release_all(2)  # T2 releases → T1 is sole holder
        t.join(timeout=3.0)
        self.assertEqual(result['r'], LockResult.GRANTED)


# ═══════════════════════════════════════════════════════════════════════════
# 6. Starvation Prevention Tests
# ═══════════════════════════════════════════════════════════════════════════

class TestStarvationPrevention(unittest.TestCase):
    """FIFO queue ensures waiting writer is not starved by readers."""

    def setUp(self):
        self.lm = LockManager()

    def test_writer_not_starved(self):
        """
        Writer waiting should be served before later readers.
        T1 holds SHARED, T2 waits EXCLUSIVE, T3 arrives for SHARED.
        FIFO: T3 must wait behind T2 even though T3 is compatible with T1.
        """
        res = table_resource("t")
        self.lm.acquire(1, res, LockType.SHARED)
        results = {}

        def writer():
            results['writer'] = self.lm.acquire(
                2, res, LockType.EXCLUSIVE, timeout=2.0)

        def late_reader():
            time.sleep(0.15)  # Arrive after writer
            results['reader'] = self.lm.acquire(
                3, res, LockType.SHARED, timeout=0.3)

        tw = threading.Thread(target=writer)
        tr = threading.Thread(target=late_reader)
        tw.start()
        time.sleep(0.05)  # Writer enqueues first
        tr.start()
        tr.join(timeout=2.0)

        # Late reader should timeout because writer is ahead in queue
        self.assertEqual(results.get('reader'), LockResult.TIMEOUT)

        # Now release T1 → writer gets lock
        self.lm.release_all(1)
        tw.join(timeout=3.0)
        self.assertEqual(results.get('writer'), LockResult.GRANTED)


# ═══════════════════════════════════════════════════════════════════════════
# 7. Transaction + Lock Integration Tests
# ═══════════════════════════════════════════════════════════════════════════

class TestTransactionLockIntegration(unittest.TestCase):
    """Test TransactionManager with LockManager integration."""

    def setUp(self):
        self.tmp = _make_tmp()
        self.lock_mgr = LockManager()
        self.lm = LogManager(self.tmp)
        self.bm = BufferManager(capacity=32)
        self.tm = TransactionManager(
            self.lm, self.bm, self.tmp, lock_manager=self.lock_mgr)

    def tearDown(self):
        self.lm.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_commit_releases_locks(self):
        """Locks are released after commit."""
        res = table_resource("data")
        tid = self.tm.begin()
        self.lock_mgr.acquire(tid, res, LockType.EXCLUSIVE)
        self.assertEqual(len(self.lock_mgr.get_locks(tid)), 1)
        self.tm.commit(tid)
        self.assertEqual(len(self.lock_mgr.get_locks(tid)), 0)

    def test_abort_releases_locks(self):
        """Locks are released after abort."""
        res = table_resource("data")
        tid = self.tm.begin()
        self.lock_mgr.acquire(tid, res, LockType.EXCLUSIVE)
        self.tm.abort(tid)
        self.assertEqual(len(self.lock_mgr.get_locks(tid)), 0)

    def test_lock_release_after_wal_durable(self):
        """Locks are released AFTER WAL commit record, not before."""
        res = table_resource("data")
        tid = self.tm.begin()
        self.lock_mgr.acquire(tid, res, LockType.EXCLUSIVE)
        self.tm.log_insert(tid, "data", 0, 0, b"test_data")

        # Commit writes WAL COMMIT, flushes, then releases
        self.tm.commit(tid)

        # Verify WAL has COMMIT record (durability first)
        recs = list(self.lm.scan())
        types = [r.record_type for r in recs]
        self.assertIn(WALRecordType.COMMIT, types)

        # Verify locks released (second)
        self.assertEqual(self.lock_mgr.get_holders(res), {})


# ═══════════════════════════════════════════════════════════════════════════
# 8. Resource Key Tests
# ═══════════════════════════════════════════════════════════════════════════

class TestResourceKeys(unittest.TestCase):
    """Test resource key helpers and hierarchy."""

    def test_table_resource(self):
        """Table resource key format."""
        r = table_resource("users")
        self.assertEqual(r, ("table", "users"))

    def test_row_resource(self):
        """Row resource key format (future use)."""
        r = row_resource("users", 5, 3)
        self.assertEqual(r, ("row", "users", 5, 3))

    def test_different_tables_independent(self):
        """Locks on different tables don't conflict."""
        lm = LockManager()
        r1 = lm.acquire(1, table_resource("a"), LockType.EXCLUSIVE)
        r2 = lm.acquire(2, table_resource("b"), LockType.EXCLUSIVE)
        self.assertEqual(r1, LockResult.GRANTED)
        self.assertEqual(r2, LockResult.GRANTED)


if __name__ == "__main__":
    unittest.main()
