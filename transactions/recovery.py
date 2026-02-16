"""
MiniDB Recovery Manager
========================
ARIES-inspired crash recovery: Analysis → Redo → Undo.

Runs at database startup before any queries are accepted.
Guarantees:
  - Committed transactions are durable (redo)
  - Uncommitted transactions are undone (undo with CLRs)
  - Idempotent: running recovery twice produces the same result
"""

import os
from typing import Dict, Set

from transactions.wal import LogManager, WALRecordType, WALEntry, NULL_LSN
from transactions.transaction import TransactionManager, TransactionState
from storage.page import PAGE_SIZE


class RecoveryManager:
    """
    Crash recovery using WAL.

    Algorithm:
      1. Ensure WAL file is durable (fsync before reading)
      2. Analysis: scan WAL, identify committed vs. uncommitted txns
      3. Redo: replay committed ops where record.lsn > page.page_lsn
      4. Undo: reverse uncommitted ops (writes CLRs)
      5. Post-recovery: flush pages, checkpoint, truncate WAL
    """

    def __init__(self, log_manager: LogManager, txn_manager: TransactionManager,
                 buffer_manager, data_dir: str):
        self._log = log_manager
        self._txn = txn_manager
        self._buffer = buffer_manager
        self._data_dir = data_dir

    def recover(self) -> dict:
        """
        Run full recovery. Returns stats dict for diagnostics.
        """
        # 0. Ensure WAL is durable before reading
        self._log.flush()

        # 1. Analysis phase
        committed, uncommitted, max_txn_id, max_lsn = self._analysis()

        # Update managers with recovered state
        self._log.set_next_lsn(max_lsn)
        self._txn.set_next_txn_id(max_txn_id + 1)

        stats = {
            "committed_txns": len(committed),
            "uncommitted_txns": len(uncommitted),
            "redo_count": 0,
            "undo_count": 0,
        }

        if not committed and not uncommitted:
            return stats  # Clean WAL, nothing to do

        # 2. Redo phase — replay committed operations
        stats["redo_count"] = self._redo(committed)

        # 3. Undo phase — reverse uncommitted operations
        stats["undo_count"] = self._undo(uncommitted)

        # 4. Post-recovery: flush everything, checkpoint, truncate
        self._post_recovery()

        return stats

    # ─── Analysis ────────────────────────────────────────────────────────

    def _analysis(self):
        """
        Scan WAL forward. Build committed/uncommitted sets.
        Returns (committed: set[txn_id], uncommitted: dict[txn_id->last_lsn],
                 max_txn_id, max_lsn).
        """
        committed: Set[int] = set()
        aborted: Set[int] = set()
        active: Dict[int, int] = {}   # txn_id -> last_lsn
        max_txn_id = 0
        max_lsn = self._log.next_lsn  # default: current end

        for entry in self._log.scan():
            tid = entry.txn_id
            if tid > max_txn_id:
                max_txn_id = tid

            record_end = entry.lsn + entry.total_len
            if record_end > max_lsn:
                max_lsn = record_end

            if entry.record_type == WALRecordType.BEGIN:
                active[tid] = entry.lsn

            elif entry.record_type == WALRecordType.COMMIT:
                committed.add(tid)
                active.pop(tid, None)

            elif entry.record_type == WALRecordType.ABORT:
                aborted.add(tid)
                active.pop(tid, None)

            elif entry.record_type == WALRecordType.CHECKPOINT:
                # Checkpoint lists active txns at time of checkpoint
                # For now we just continue scanning
                pass

            # Track last_lsn for all active txns
            if tid in active:
                active[tid] = entry.lsn

        # uncommitted = active txns that didn't commit or abort
        uncommitted = {tid: lsn for tid, lsn in active.items()
                       if tid not in committed and tid not in aborted}

        return committed, uncommitted, max_txn_id, max_lsn

    # ─── Redo ────────────────────────────────────────────────────────────

    def _redo(self, committed: Set[int]) -> int:
        """Replay committed ops where record.lsn > page.page_lsn."""
        count = 0
        for entry in self._log.scan():
            if entry.txn_id not in committed:
                continue

            if entry.record_type == WALRecordType.INSERT:
                if self._redo_insert(entry):
                    count += 1

            elif entry.record_type == WALRecordType.DELETE:
                if self._redo_delete(entry):
                    count += 1

            elif entry.record_type == WALRecordType.UPDATE:
                if self._redo_update(entry):
                    count += 1

            elif entry.record_type == WALRecordType.CLR:
                if self._redo_clr(entry):
                    count += 1

        return count

    def _should_redo(self, table_name: str, page_id: int, lsn: int) -> bool:
        """Check if this operation needs to be redone (page LSN check)."""
        file_path = os.path.join(self._data_dir, f"{table_name}.tbl")
        if not os.path.exists(file_path):
            return False  # Table file doesn't exist
        page = self._buffer.get_page(file_path, page_id)
        if page is None:
            return False
        return lsn > page.page_lsn

    def _redo_insert(self, entry: WALEntry) -> bool:
        tname, pid, sid, tdata = LogManager.parse_dml_payload(entry.payload)
        if not self._should_redo(tname, pid, entry.lsn):
            return False
        file_path = os.path.join(self._data_dir, f"{tname}.tbl")
        page = self._buffer.get_page(file_path, pid)
        # Try restore at specific slot (if slot is deleted/available)
        page.restore_tuple(sid, tdata)
        page.page_lsn = entry.lsn
        self._buffer.mark_dirty(file_path, pid)
        return True

    def _redo_delete(self, entry: WALEntry) -> bool:
        tname, pid, sid, tdata = LogManager.parse_dml_payload(entry.payload)
        if not self._should_redo(tname, pid, entry.lsn):
            return False
        file_path = os.path.join(self._data_dir, f"{tname}.tbl")
        page = self._buffer.get_page(file_path, pid)
        page.delete_tuple(sid)
        page.page_lsn = entry.lsn
        self._buffer.mark_dirty(file_path, pid)
        return True

    def _redo_update(self, entry: WALEntry) -> bool:
        tname, pid, sid, old_d, new_d = LogManager.parse_update_payload(entry.payload)
        if not self._should_redo(tname, pid, entry.lsn):
            return False
        file_path = os.path.join(self._data_dir, f"{tname}.tbl")
        page = self._buffer.get_page(file_path, pid)
        page.update_tuple(sid, new_d)
        page.page_lsn = entry.lsn
        self._buffer.mark_dirty(file_path, pid)
        return True

    def _redo_clr(self, entry: WALEntry) -> bool:
        """Redo a CLR — these are compensation records from a previous abort."""
        undo_next, inner_type, inner_payload = LogManager.parse_clr_payload(entry.payload)

        if inner_type == WALRecordType.INSERT:
            tname, pid, sid, tdata = LogManager.parse_dml_payload(inner_payload)
            if not self._should_redo(tname, pid, entry.lsn):
                return False
            file_path = os.path.join(self._data_dir, f"{tname}.tbl")
            page = self._buffer.get_page(file_path, pid)
            page.restore_tuple(sid, tdata)
            page.page_lsn = entry.lsn
            self._buffer.mark_dirty(file_path, pid)
            return True

        elif inner_type == WALRecordType.DELETE:
            tname, pid, sid, _ = LogManager.parse_dml_payload(inner_payload)
            if not self._should_redo(tname, pid, entry.lsn):
                return False
            file_path = os.path.join(self._data_dir, f"{tname}.tbl")
            page = self._buffer.get_page(file_path, pid)
            page.delete_tuple(sid)
            page.page_lsn = entry.lsn
            self._buffer.mark_dirty(file_path, pid)
            return True

        elif inner_type == WALRecordType.UPDATE:
            tname, pid, sid, old_d, new_d = LogManager.parse_update_payload(inner_payload)
            if not self._should_redo(tname, pid, entry.lsn):
                return False
            file_path = os.path.join(self._data_dir, f"{tname}.tbl")
            page = self._buffer.get_page(file_path, pid)
            page.update_tuple(sid, new_d)
            page.page_lsn = entry.lsn
            self._buffer.mark_dirty(file_path, pid)
            return True

        return False

    # ─── Undo ────────────────────────────────────────────────────────────

    def _undo(self, uncommitted: Dict[int, int]) -> int:
        """Undo uncommitted transactions using TransactionManager."""
        count = 0
        for txn_id, last_lsn in uncommitted.items():
            # Register the txn so TransactionManager can work with it
            self._txn.register_txn(txn_id, TransactionState.ACTIVE, last_lsn)
            self._txn._undo_txn(txn_id, last_lsn)

            # Write ABORT record
            info = self._txn._txns[txn_id]
            self._log.append_abort(txn_id, info.last_lsn)
            info.state = TransactionState.ABORTED
            count += 1

        return count

    # ─── Post-recovery ───────────────────────────────────────────────────

    def _post_recovery(self):
        """Flush all dirty pages, write checkpoint, truncate WAL."""
        # Flush all dirty pages to disk
        if self._buffer:
            dirty = self._buffer.flush_all()
            for file_path, pid, page in dirty:
                if os.path.exists(file_path):
                    with open(file_path, "r+b") as f:
                        f.seek(pid * PAGE_SIZE)
                        f.write(page.to_bytes())
                        f.flush()
                        os.fsync(f.fileno())

        # Write checkpoint (no active txns after recovery)
        self._log.append_checkpoint([])

        # Truncate WAL — safe because:
        # 1. All dirty pages flushed
        # 2. No active transactions
        from transactions.wal import WAL_PADDING
        self._log.truncate(WAL_PADDING)
