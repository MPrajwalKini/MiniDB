"""
MiniDB Transaction Manager
===========================
Multi-writer ACID transactions with WAL-based undo/redo.

Phase 7 upgrades:
  - Multiple concurrent transactions (was single-writer)
  - Lock integration: release_all() after WAL commit durable
  - Thread-safe txn_id generation
  - Physical undo via WAL backward traversal with CLR logging
"""

import os
import threading
from enum import Enum
from typing import Optional, Dict

from transactions.wal import LogManager, WALRecordType, NULL_LSN
from storage.page import RID


class TransactionState(Enum):
    ACTIVE = "ACTIVE"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"


class _TxnInfo:
    """Internal bookkeeping for one transaction."""
    __slots__ = ('txn_id', 'state', 'last_lsn', 'commit_hooks', 'rollback_hooks')

    def __init__(self, txn_id: int):
        self.txn_id = txn_id
        self.state = TransactionState.ACTIVE
        self.last_lsn = NULL_LSN  # chain head for prev_txn_lsn
        self.commit_hooks = []
        self.rollback_hooks = []


class TransactionManager:
    """
    Manages transaction lifecycle.

    Phase 7 invariants:
      - Multiple concurrent transactions allowed (guarded by LockManager)
      - Lock release AFTER WAL commit record is durable
      - Thread-safe txn_id allocation via mutex
      - Strict ordering: log WAL → apply change → set page_lsn → mark dirty
      - Abort undoes all changes in reverse LSN order, writing CLRs
    """

    def __init__(self, log_manager: LogManager, buffer_manager=None,
                 data_dir: str = "", lock_manager=None):
        self._log = log_manager
        self._buffer = buffer_manager
        self._data_dir = data_dir
        self._lock = lock_manager       # Optional: concurrency.LockManager
        self._next_txn_id = 1
        self._txns: Dict[int, _TxnInfo] = {}
        self._mutex = threading.Lock()   # Protects _next_txn_id and _txns

    # ─── Lifecycle ───────────────────────────────────────────────────────

    def begin(self) -> int:
        """
        Start a new transaction. Returns txn_id.
        Thread-safe: multiple concurrent transactions allowed.
        """
        with self._mutex:
            txn_id = self._next_txn_id
            self._next_txn_id += 1
            info = _TxnInfo(txn_id)
            self._txns[txn_id] = info

        # WAL logging (LogManager has its own internal synchronization)
        lsn = self._log.append_begin(txn_id)
        with self._mutex:
            self._txns[txn_id].last_lsn = lsn

        return txn_id

    def commit(self, txn_id: int) -> None:
        """
        Commit a transaction:
          1. Write WAL COMMIT record
          2. Flush WAL (makes commit durable)
          3. Release all locks (AFTER durable — critical for isolation)
          4. Flush dirty data pages
        """
        info = self._get_active(txn_id)

        # 1. Write COMMIT record + flush WAL (makes commit durable)
        lsn = self._log.append_commit(txn_id, info.last_lsn)
        with self._mutex:
            info.last_lsn = lsn
            info.state = TransactionState.COMMITTED

        # 2. Release all locks AFTER WAL is durable
        if self._lock:
            self._lock.release_all(txn_id)

        if self._buffer:
            self._flush_dirty_pages()

        # 4. Execute commit hooks (e.g., catalog persistence)
        print(f"DEBUG_TXN: Executing {len(info.commit_hooks)} commit hooks for txn {txn_id}")
        for hook in info.commit_hooks:
            try:
                hook()
            except Exception as e:
                # Log error but don't fail commit (already durable)
                print(f"Warning: Commit hook failed for txn {txn_id}: {e}")

    def abort(self, txn_id: int) -> None:
        """
        Abort a transaction:
          1. Undo all changes in reverse order (CLRs for crash safety)
          2. Write WAL ABORT record
          3. Release all locks AFTER undo + ABORT durable
        """
        info = self._get_active(txn_id)

        # 1. Undo phase: walk backward through this txn's records
        self._undo_txn(txn_id, info.last_lsn)

        # 2. Write ABORT record
        lsn = self._log.append_abort(txn_id, info.last_lsn)
        with self._mutex:
            info.last_lsn = lsn
            info.state = TransactionState.ABORTED

        if self._lock:
            self._lock.release_all(txn_id)

        # 4. Execute rollback hooks (e.g., revert catalog changes, delete files)
        print(f"DEBUG_TXN: Executing {len(info.rollback_hooks)} rollback hooks for txn {txn_id}")
        for hook in info.rollback_hooks:
            try:
                hook()
            except Exception as e:
                print(f"Warning: Rollback hook failed for txn {txn_id}: {e}")

    # ─── Query ───────────────────────────────────────────────────────────

    def get_active_txn(self) -> Optional[int]:
        """Return any active txn_id, or None. For backward compat."""
        with self._mutex:
            for tid, info in self._txns.items():
                if info.state == TransactionState.ACTIVE:
                    return tid
        return None

    def get_active_txns(self) -> list:
        """Return list of all active txn_ids."""
        with self._mutex:
            return [tid for tid, info in self._txns.items()
                    if info.state == TransactionState.ACTIVE]

    def is_active(self, txn_id: int) -> bool:
        with self._mutex:
            info = self._txns.get(txn_id)
            return info is not None and info.state == TransactionState.ACTIVE

    def get_last_lsn(self, txn_id: int) -> int:
        with self._mutex:
            info = self._txns.get(txn_id)
            return info.last_lsn if info else NULL_LSN

    # ─── WAL logging helpers (called by executor DML ops) ────────────────

    def log_insert(self, txn_id: int, table_name: str,
                   page_id: int, slot_id: int, tuple_data: bytes) -> int:
        """Log an INSERT. Returns LSN."""
        info = self._get_active(txn_id)
        lsn = self._log.append_insert(
            txn_id, info.last_lsn, table_name, page_id, slot_id, tuple_data)
        with self._mutex:
            info.last_lsn = lsn
        return lsn

    def log_delete(self, txn_id: int, table_name: str,
                   page_id: int, slot_id: int, tuple_data: bytes) -> int:
        """Log a DELETE (with before-image). Returns LSN."""
        info = self._get_active(txn_id)
        lsn = self._log.append_delete(
            txn_id, info.last_lsn, table_name, page_id, slot_id, tuple_data)
        with self._mutex:
            info.last_lsn = lsn
        return lsn

    def log_update(self, txn_id: int, table_name: str,
                   page_id: int, slot_id: int,
                   old_data: bytes, new_data: bytes) -> int:
        """Log an UPDATE (with both images). Returns LSN."""
        info = self._get_active(txn_id)
        lsn = self._log.append_update(
            txn_id, info.last_lsn, table_name, page_id, slot_id,
            old_data, new_data)
        with self._mutex:
            info.last_lsn = lsn
        return lsn

    def update_last_lsn(self, txn_id: int, lsn: int) -> None:
        """Update the last_lsn for a transaction (used by recovery)."""
        with self._mutex:
            info = self._txns.get(txn_id)
            if info:
                info.last_lsn = lsn

    # ─── Internal ────────────────────────────────────────────────────────

    def _get_active(self, txn_id: int) -> '_TxnInfo':
        with self._mutex:
            info = self._txns.get(txn_id)
        if info is None:
            raise RuntimeError(f"Unknown transaction {txn_id}")
        if info.state != TransactionState.ACTIVE:
            raise RuntimeError(
                f"Transaction {txn_id} is {info.state.value}, not ACTIVE")
        return info

    def _undo_txn(self, txn_id: int, from_lsn: int) -> None:
        """Walk backward through txn's records and undo each mutation."""
        lsn = from_lsn
        while lsn != NULL_LSN:
            entry = self._log.read_record(lsn)
            if entry.txn_id != txn_id:
                raise RuntimeError(
                    f"LSN chain corrupt: expected txn {txn_id}, got {entry.txn_id}")

            next_lsn = entry.prev_lsn  # next record to undo

            if entry.record_type == WALRecordType.CLR:
                # CLR: skip to undo_next_lsn (already compensated)
                undo_next, _, _ = LogManager.parse_clr_payload(entry.payload)
                next_lsn = undo_next

            elif entry.record_type == WALRecordType.INSERT:
                tname, pid, sid, tdata = LogManager.parse_dml_payload(entry.payload)
                self._undo_insert(txn_id, tname, pid, sid, tdata, next_lsn)

            elif entry.record_type == WALRecordType.DELETE:
                tname, pid, sid, tdata = LogManager.parse_dml_payload(entry.payload)
                self._undo_delete(txn_id, tname, pid, sid, tdata, next_lsn)

            elif entry.record_type == WALRecordType.UPDATE:
                tname, pid, sid, old_d, new_d = LogManager.parse_update_payload(entry.payload)
                self._undo_update(txn_id, tname, pid, sid, old_d, new_d, next_lsn)

            # BEGIN, COMMIT, ABORT — nothing to undo
            lsn = next_lsn

    def _undo_insert(self, txn_id, tname, page_id, slot_id, tuple_data, undo_next_lsn):
        """Undo INSERT by deleting the tuple, then logging CLR."""
        page = self._get_page(tname, page_id)
        if page is not None:
            page.delete_tuple(slot_id)
            self._mark_dirty(tname, page_id)

        info = self._txns[txn_id]
        clr_payload = LogManager._pack_table_rid(tname, page_id, slot_id)
        clr_payload += b'\x00\x00'
        clr_lsn = self._log.append_clr(
            txn_id, info.last_lsn, undo_next_lsn,
            WALRecordType.DELETE, clr_payload)
        with self._mutex:
            info.last_lsn = clr_lsn

        if page is not None:
            page.page_lsn = clr_lsn

    def _undo_delete(self, txn_id, tname, page_id, slot_id, tuple_data, undo_next_lsn):
        """Undo DELETE by restoring the tuple."""
        page = self._get_page(tname, page_id)
        if page is not None:
            page.restore_tuple(slot_id, tuple_data)
            self._mark_dirty(tname, page_id)

        info = self._txns[txn_id]
        prefix = LogManager._pack_table_rid(tname, page_id, slot_id)
        import struct
        clr_payload = prefix + struct.pack(">H", len(tuple_data)) + tuple_data
        clr_lsn = self._log.append_clr(
            txn_id, info.last_lsn, undo_next_lsn,
            WALRecordType.INSERT, clr_payload)
        with self._mutex:
            info.last_lsn = clr_lsn

        if page is not None:
            page.page_lsn = clr_lsn

    def _undo_update(self, txn_id, tname, page_id, slot_id, old_data, new_data, undo_next_lsn):
        """Undo UPDATE by restoring old data."""
        page = self._get_page(tname, page_id)
        if page is not None:
            page.update_tuple(slot_id, old_data)
            self._mark_dirty(tname, page_id)

        info = self._txns[txn_id]
        import struct
        prefix = LogManager._pack_table_rid(tname, page_id, slot_id)
        clr_payload = (prefix +
                       struct.pack(">H", len(new_data)) + new_data +
                       struct.pack(">H", len(old_data)) + old_data)
        clr_lsn = self._log.append_clr(
            txn_id, info.last_lsn, undo_next_lsn,
            WALRecordType.UPDATE, clr_payload)
        with self._mutex:
            info.last_lsn = clr_lsn

        if page is not None:
            page.page_lsn = clr_lsn

    def _get_page(self, table_name, page_id):
        """Get a page from the buffer manager for physical undo."""
        if not self._buffer:
            return None
        # Handle absolute paths (new catalog) vs relative names (legacy)
        if os.path.isabs(table_name):
             file_path = table_name
        else:
             file_path = os.path.join(self._data_dir, f"{table_name}.tbl")
        return self._buffer.get_page(file_path, page_id)

    def _mark_dirty(self, table_name, page_id):
        """Mark a page dirty in the buffer manager."""
        if not self._buffer:
            return
        if os.path.isabs(table_name):
             file_path = table_name
        else:
             file_path = os.path.join(self._data_dir, f"{table_name}.tbl")
        self._buffer.mark_dirty(file_path, page_id)

    def register_hook(self, txn_id: int, commit_fn=None, rollback_fn=None) -> None:
        """Register a callback for commit/rollback."""
        print(f"DEBUG_TXN: Registering hooks for txn {txn_id} (commit={bool(commit_fn)}, rollback={bool(rollback_fn)})")
        info = self._get_active(txn_id)
        if commit_fn:
            info.commit_hooks.append(commit_fn)
        if rollback_fn:
            info.rollback_hooks.append(rollback_fn)

    def _flush_dirty_pages(self):
        """Flush all dirty data pages to disk."""
        if self._buffer:
            dirty = self._buffer.flush_all()
            for file_path, pid, page in dirty:
                with open(file_path, "r+b") as f:
                    from storage.page import PAGE_SIZE
                    f.seek(pid * PAGE_SIZE)
                    f.write(page.to_bytes())
                    f.flush()
                    os.fsync(f.fileno())

    # ─── Recovery support ────────────────────────────────────────────────

    def set_next_txn_id(self, txn_id: int):
        """Set by recovery manager after analysis."""
        with self._mutex:
            self._next_txn_id = txn_id

    def register_txn(self, txn_id: int, state: TransactionState, last_lsn: int):
        """Register a transaction found during recovery analysis."""
        with self._mutex:
            info = _TxnInfo(txn_id)
            info.state = state
            info.last_lsn = last_lsn
            self._txns[txn_id] = info
