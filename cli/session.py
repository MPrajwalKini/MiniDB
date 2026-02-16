"""
MiniDB Session
==============
Per-connection state object that wires all engine components together.

Owns:
  - Catalog, BufferManager, LogManager, TransactionManager, LockManager
  - RecoveryManager (runs once at startup)
  - ExecutionContext + Executor

Autocommit semantics:
  - Default: autocommit=True (each statement runs in its own txn)
  - BEGIN → autocommit=False (explicit txn until COMMIT/ROLLBACK)
  - COMMIT/ROLLBACK → autocommit=True
"""

import os
import threading
import time
from typing import Any, Dict, Iterator, List, Optional, Tuple

from catalog.catalog import Catalog
from storage.buffer import BufferManager
from transactions.wal import LogManager
from transactions.transaction import TransactionManager
from transactions.recovery import RecoveryManager
from concurrency.lock_manager import LockManager
from indexing import index_manager
from storage.table import TableFile
from execution.context import ExecutionContext
from execution.executor import Executor
from execution.physical_plan import ExecutionRow
from parser import parse
from parser.ast_nodes import (
    BeginStmt, CommitStmt, RollbackStmt, ExplainStmt,
    SelectStmt, InsertStmt, UpdateStmt, DeleteStmt, CreateTableStmt,
    CreateIndexStmt, DropIndexStmt,
    Statement,
)
from planning.planner import Planner
from execution.planner import PhysicalPlanner


class SessionError(Exception):
    """Session-level error (transaction lifecycle, etc.)."""
    pass


class Session:
    """
    Database session — owns all engine components for one connection.

    Usage:
        session = Session("path/to/db")
        for row in session.execute("SELECT * FROM users"):
            print(row)
        session.close()
    """

    def __init__(self, db_path: str, *, buffer_pool_size: int = 100):
        self.db_path = os.path.abspath(db_path)
        os.makedirs(self.db_path, exist_ok=True)

        # ── Engine components ──
        self.catalog = Catalog(self.db_path)
        self.catalog.load()

        self.buffer_manager = BufferManager(buffer_pool_size)
        self.log_manager = LogManager(self.db_path)
        self.lock_manager = LockManager()
        self.txn_manager = TransactionManager(
            self.log_manager,
            buffer_manager=self.buffer_manager,
            data_dir=self.db_path,
            lock_manager=self.lock_manager,
        )

        # ── Recovery ──
        recovery = RecoveryManager(
            self.log_manager, self.txn_manager,
            self.buffer_manager, self.db_path
        )
        self._recovery_stats = recovery.recover()

        # ── Execution context ──
        self.context = ExecutionContext(
            catalog=self.catalog,
            buffer_manager=self.buffer_manager,
            base_path=self.db_path,
            txn_manager=self.txn_manager,
            log_manager=self.log_manager,
            lock_manager=self.lock_manager,
        )
        self.executor = Executor(self.context)

        # ── Session state ──
        self.autocommit: bool = True
        self.active_txn_id: Optional[int] = None
        self._closed: bool = False
        self._cancelled = threading.Event()  # For query cancellation

        # ── Statistics ──
        self.stats = {
            "transactions_committed": 0,
            "transactions_aborted": 0,
            "statements_executed": 0,
        }

    # ─── Transaction Control ────────────────────────────────────────

    def begin(self) -> str:
        """Start an explicit transaction. Returns status message."""
        self._check_closed()
        if self.active_txn_id is not None:
            raise SessionError("Transaction already active — COMMIT or ROLLBACK first")
        self.active_txn_id = self.txn_manager.begin()
        self.context.active_txn_id = self.active_txn_id
        self.autocommit = False
        return f"BEGIN — transaction {self.active_txn_id}"

    def commit(self) -> str:
        """Commit current transaction. Returns status message."""
        self._check_closed()
        if self.active_txn_id is None:
            return "WARNING: no transaction in progress"
        txn_id = self.active_txn_id
        self.txn_manager.commit(txn_id)
        self.active_txn_id = None
        self.context.active_txn_id = None
        self.autocommit = True
        self.stats["transactions_committed"] += 1
        return f"COMMIT — transaction {txn_id}"

    def rollback(self) -> str:
        """Rollback current transaction. Returns status message."""
        self._check_closed()
        if self.active_txn_id is None:
            return "WARNING: no transaction in progress"
        txn_id = self.active_txn_id
        self.txn_manager.abort(txn_id)
        self.active_txn_id = None
        self.context.active_txn_id = None
        self.autocommit = True
        self.stats["transactions_aborted"] += 1
        return f"ROLLBACK — transaction {txn_id}"

    # ─── Query Execution ────────────────────────────────────────────

    def execute(self, sql: str) -> Tuple[Optional[Iterator[ExecutionRow]], str, Optional[List[str]]]:
        """
        Execute a SQL statement.

        Returns: (row_iterator_or_None, message, column_names_or_None)
          - SELECT: (iterator, "", [col_names])
          - DML/DDL: (None, "Inserted 1 row", None)
          - Txn control: (None, "BEGIN — transaction 1", None)
          - EXPLAIN: (None, plan_text, None)
        """
        self._check_closed()
        self._cancelled.clear()
        self.stats["statements_executed"] += 1

        # Parse
        stmt = parse(sql)

        # ─ Transaction control (not routed through executor) ─
        if isinstance(stmt, BeginStmt):
            return None, self.begin(), None
        if isinstance(stmt, CommitStmt):
            return None, self.commit(), None
        if isinstance(stmt, RollbackStmt):
            return None, self.rollback(), None

        # ─ EXPLAIN (no lock acquisition, no execution) ─
        if isinstance(stmt, ExplainStmt):
            plan_text = self._explain(stmt)
            return None, plan_text, None

        if isinstance(stmt, CreateIndexStmt):
            return None, self._create_index(stmt), None

        if isinstance(stmt, DropIndexStmt):
            return None, self._drop_index(stmt), None

        # ─ Regular SQL execution ─
        # Autocommit: wrap in implicit txn
        implicit_txn = False
        if self.autocommit and self.active_txn_id is None:
            self.active_txn_id = self.txn_manager.begin()
            self.context.active_txn_id = self.active_txn_id
            implicit_txn = True

        try:
            if isinstance(stmt, SelectStmt):
                # Streaming: return iterator, caller consumes
                rows = self.executor.execute(sql)
                col_names = None
                # Wrap iterator with autocommit finalization
                if implicit_txn:
                    rows = self._wrap_autocommit_iterator(rows, col_names)
                return rows, "", col_names
            else:
                # DML/DDL: consume all rows, build message
                result_rows = list(self.executor.execute(sql))
                msg = self._format_dml_message(stmt, result_rows)

                if implicit_txn:
                    self._commit_implicit()
                    implicit_txn = False

                return None, msg, None

        except Exception:
            if implicit_txn:
                self._rollback_implicit()
            raise

    def cancel(self):
        """Signal cancellation of the current query."""
        self._cancelled.set()

    # ─── EXPLAIN ────────────────────────────────────────────────────

    def _explain(self, stmt: ExplainStmt) -> str:
        """Generate plan text without executing."""
        planner = Planner(self.catalog)
        physical_planner = PhysicalPlanner(self.context)

        lines = []

        if stmt.level in ("both", "logical"):
            logical_plan = planner.plan(stmt.inner)
            lines.append("=== Logical Plan ===")
            lines.append(self._format_plan_tree(logical_plan))

        if stmt.level in ("both", "physical"):
            logical_plan = planner.plan(stmt.inner)
            physical_plan = physical_planner.plan(logical_plan)
            lines.append("=== Physical Plan ===")
            lines.append(self._format_plan_tree(physical_plan))

        return "\n".join(lines)

    def _create_index(self, stmt: CreateIndexStmt) -> str:
        """Handle CREATE INDEX."""
        # 1. Check table exists
        table_path = self.context.get_table_path(stmt.table_name)
        if not os.path.exists(table_path):
            raise SessionError(f"Table '{stmt.table_name}' does not exist")

        # 2. Open table
        # We use a temporary TableFile instance. 
        # Note: In a multi-user system, we would need a shared table handle or lock.
        # For now, we rely on the buffer manager being shared.
        table = TableFile(table_path, self.buffer_manager)
        table.open()

        try:
            # 3. Build index
            index_manager.build_index(
                self.catalog, table, stmt.table_name, stmt.column_name, 
                stmt.index_name, self.buffer_manager
            )
            return f"Index '{stmt.index_name}' created on {stmt.table_name}({stmt.column_name})"
        except Exception as e:
            raise SessionError(f"Failed to create index: {e}")
        finally:
            table.close()

    def _drop_index(self, stmt: DropIndexStmt) -> str:
        """Handle DROP INDEX."""
        if not self.catalog.get_index(stmt.index_name):
             raise SessionError(f"Index '{stmt.index_name}' not found")

        try:
            index_manager.drop_index(self.catalog, stmt.index_name)
            return f"Index '{stmt.index_name}' dropped"
        except Exception as e:
            raise SessionError(f"Failed to drop index: {e}")

    def _format_plan_tree(self, node, indent: int = 0) -> str:
        """Recursively format a plan node tree."""
        prefix = "  " * indent
        lines = [f"{prefix}{node.__class__.__name__}"]

        # Add node-specific info
        attrs = {}
        if hasattr(node, 'table_name'):
            attrs['table'] = node.table_name
        if hasattr(node, 'table') and hasattr(node.table, 'file_name'):
            attrs['file'] = node.table.file_name
        if hasattr(node, 'predicate') and node.predicate:
            attrs['predicate'] = str(node.predicate)
        if hasattr(node, 'scan_type'):
            attrs['scan_type'] = node.scan_type
        if hasattr(node, 'index_name') and node.index_name:
            attrs['index'] = node.index_name
        if hasattr(node, 'columns') and node.columns:
            attrs['columns'] = str(node.columns)

        if attrs:
            detail = ", ".join(f"{k}={v}" for k, v in attrs.items())
            lines[-1] += f" ({detail})"

        # Recurse into children
        for child_attr in ('child', 'left', 'right', 'children'):
            child = getattr(node, child_attr, None)
            if child is not None:
                if isinstance(child, (list, tuple)):
                    for c in child:
                        lines.append(self._format_plan_tree(c, indent + 1))
                else:
                    lines.append(self._format_plan_tree(child, indent + 1))

        return "\n".join(lines)

    # ─── Internal ───────────────────────────────────────────────────

    def _wrap_autocommit_iterator(self, rows, col_names):
        """Wrap SELECT iterator to auto-commit after consumption."""
        try:
            for row in rows:
                if self._cancelled.is_set():
                    raise KeyboardInterrupt("Query cancelled")
                yield row
            # All rows consumed — commit implicit txn
            self._commit_implicit()
        except Exception:
            self._rollback_implicit()
            raise

    def _commit_implicit(self):
        """Commit an implicit autocommit transaction."""
        if self.active_txn_id is not None:
            self.txn_manager.commit(self.active_txn_id)
            self.active_txn_id = None
            self.context.active_txn_id = None
            self.stats["transactions_committed"] += 1

    def _rollback_implicit(self):
        """Rollback an implicit autocommit transaction."""
        if self.active_txn_id is not None:
            try:
                self.txn_manager.abort(self.active_txn_id)
            except Exception:
                pass
            self.active_txn_id = None
            self.context.active_txn_id = None
            self.stats["transactions_aborted"] += 1

    def _format_dml_message(self, stmt: Statement, rows: list) -> str:
        """Format a user-friendly DML/DDL result message."""
        if isinstance(stmt, InsertStmt):
            return f"Inserted 1 row."
        if isinstance(stmt, UpdateStmt):
            count = rows[0].values.get("rows_affected", 0) if rows else 0
            return f"Updated {count} row(s)."
        if isinstance(stmt, DeleteStmt):
            count = rows[0].values.get("rows_affected", 0) if rows else 0
            return f"Deleted {count} row(s)."
        if isinstance(stmt, CreateTableStmt):
            return f"Table '{stmt.table_name}' created."
        return "OK"

    def _check_closed(self):
        if self._closed:
            raise SessionError("Session is closed")

    # ─── Lifecycle ──────────────────────────────────────────────────

    def close(self) -> Optional[str]:
        """
        Close the session. If a transaction is active, it is rolled back.
        Returns a warning message if a rollback occurred.
        """
        if self._closed:
            return None

        warning = None
        if self.active_txn_id is not None:
            txn_id = self.active_txn_id
            self.rollback()
            warning = f"WARNING: active transaction {txn_id} was rolled back on close"

        # Flush buffers
        if self.buffer_manager:
            self.buffer_manager.flush_all_and_clear()

        self._closed = True
        return warning

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
