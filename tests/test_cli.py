"""
MiniDB Phase 8 CLI Tests
=========================
Tests for Session, Renderer, Transaction control, Meta-commands, EXPLAIN,
and script execution.
"""

import io
import os
import shutil
import sys
import tempfile
import unittest

from cli.session import Session, SessionError
from cli.renderer import Renderer


class SessionTestBase(unittest.TestCase):
    """Base with temp directory for Session tests."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="minidb_cli_test_")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)


# ═══════════════════════════════════════════════════════════════════════════
# Session Lifecycle
# ═══════════════════════════════════════════════════════════════════════════

class TestSessionLifecycle(SessionTestBase):

    def test_open_close(self):
        """Session opens and closes cleanly."""
        session = Session(self.test_dir)
        self.assertFalse(session._closed)
        session.close()
        self.assertTrue(session._closed)

    def test_context_manager(self):
        """Session works as context manager."""
        with Session(self.test_dir) as session:
            self.assertFalse(session._closed)
        self.assertTrue(session._closed)

    def test_creates_directory(self):
        """Session creates database directory if not exists."""
        db_path = os.path.join(self.test_dir, "new_db")
        with Session(db_path) as session:
            self.assertTrue(os.path.isdir(db_path))

    def test_recovery_runs_on_startup(self):
        """Recovery runs automatically when session opens."""
        with Session(self.test_dir) as session:
            self.assertIsNotNone(session._recovery_stats)

    def test_close_rolls_back_active_txn(self):
        """Closing session with active txn rolls it back."""
        session = Session(self.test_dir)
        session.begin()
        self.assertIsNotNone(session.active_txn_id)
        warning = session.close()
        self.assertIn("rolled back", warning)
        self.assertIsNone(session.active_txn_id)

    def test_double_close_safe(self):
        """Closing an already-closed session is a no-op."""
        session = Session(self.test_dir)
        session.close()
        result = session.close()
        self.assertIsNone(result)


# ═══════════════════════════════════════════════════════════════════════════
# Transaction Commands
# ═══════════════════════════════════════════════════════════════════════════

class TestTransactionCommands(SessionTestBase):

    def test_begin_commit(self):
        """BEGIN → COMMIT lifecycle."""
        with Session(self.test_dir) as s:
            _, msg, _ = s.execute("BEGIN")
            self.assertIn("BEGIN", msg)
            self.assertFalse(s.autocommit)
            self.assertIsNotNone(s.active_txn_id)

            _, msg, _ = s.execute("COMMIT")
            self.assertIn("COMMIT", msg)
            self.assertTrue(s.autocommit)
            self.assertIsNone(s.active_txn_id)

    def test_begin_rollback(self):
        """BEGIN → ROLLBACK lifecycle."""
        with Session(self.test_dir) as s:
            s.execute("BEGIN")
            _, msg, _ = s.execute("ROLLBACK")
            self.assertIn("ROLLBACK", msg)
            self.assertTrue(s.autocommit)

    def test_nested_begin_error(self):
        """BEGIN when txn already active raises error."""
        with Session(self.test_dir) as s:
            s.execute("BEGIN")
            with self.assertRaises(SessionError):
                s.execute("BEGIN")

    def test_commit_no_txn_warning(self):
        """COMMIT with no active txn returns warning."""
        with Session(self.test_dir) as s:
            _, msg, _ = s.execute("COMMIT")
            self.assertIn("WARNING", msg)

    def test_rollback_no_txn_warning(self):
        """ROLLBACK with no active txn returns warning."""
        with Session(self.test_dir) as s:
            _, msg, _ = s.execute("ROLLBACK")
            self.assertIn("WARNING", msg)

    def test_begin_transaction_keyword(self):
        """BEGIN TRANSACTION (with optional TRANSACTION keyword)."""
        with Session(self.test_dir) as s:
            _, msg, _ = s.execute("BEGIN TRANSACTION")
            self.assertIn("BEGIN", msg)
            self.assertFalse(s.autocommit)

    def test_autocommit_default(self):
        """Default autocommit is True."""
        with Session(self.test_dir) as s:
            self.assertTrue(s.autocommit)


# ═══════════════════════════════════════════════════════════════════════════
# SQL Execution via Session
# ═══════════════════════════════════════════════════════════════════════════

class TestSessionExecution(SessionTestBase):

    def test_create_and_select(self):
        """CREATE TABLE + INSERT + SELECT works through Session."""
        with Session(self.test_dir) as s:
            _, msg, _ = s.execute("CREATE TABLE users (id INT, name STRING)")
            self.assertIn("created", msg)

            _, msg, _ = s.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
            self.assertIn("Inserted", msg)

            rows, _, _ = s.execute("SELECT * FROM users")
            result = [r.values for r in rows]
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]['id'], 1)
            self.assertEqual(result[0]['name'], 'Alice')

    def test_autocommit_insert(self):
        """Autocommit: INSERT auto-commits so data persists across sessions."""
        with Session(self.test_dir) as s:
            s.execute("CREATE TABLE t (x INT)")
            s.execute("INSERT INTO t (x) VALUES (42)")

        # Re-open session — data should persist
        with Session(self.test_dir) as s:
            rows, _, _ = s.execute("SELECT * FROM t")
            result = [r.values for r in rows]
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]['x'], 42)

    def test_explicit_txn_rollback(self):
        """Data inserted in explicit txn is rolled back."""
        with Session(self.test_dir) as s:
            s.execute("CREATE TABLE t (x INT)")

        with Session(self.test_dir) as s:
            s.execute("BEGIN")
            s.execute("INSERT INTO t (x) VALUES (99)")
            s.execute("ROLLBACK")

            rows, _, _ = s.execute("SELECT * FROM t")
            result = [r.values for r in rows]
            self.assertEqual(len(result), 0)

    def test_explicit_txn_commit(self):
        """Data inserted in explicit txn is committed."""
        with Session(self.test_dir) as s:
            s.execute("CREATE TABLE t (x INT)")

        with Session(self.test_dir) as s:
            s.execute("BEGIN")
            s.execute("INSERT INTO t (x) VALUES (99)")
            s.execute("COMMIT")

        with Session(self.test_dir) as s:
            rows, _, _ = s.execute("SELECT * FROM t")
            result = [r.values for r in rows]
            self.assertEqual(len(result), 1)

    def test_dml_messages(self):
        """DML operations return proper messages."""
        with Session(self.test_dir) as s:
            s.execute("CREATE TABLE t (id INT, val STRING)")
            _, msg, _ = s.execute("INSERT INTO t (id, val) VALUES (1, 'a')")
            self.assertIn("Inserted", msg)

            _, msg, _ = s.execute("UPDATE t SET val = 'b' WHERE id = 1")
            self.assertIn("Updated", msg)

            _, msg, _ = s.execute("DELETE FROM t WHERE id = 1")
            self.assertIn("Deleted", msg)

    def test_closed_session_error(self):
        """Operating on closed session raises error."""
        s = Session(self.test_dir)
        s.close()
        with self.assertRaises(SessionError):
            s.execute("SELECT 1")

    def test_stats_tracking(self):
        """Session statistics are tracked."""
        with Session(self.test_dir) as s:
            s.execute("CREATE TABLE t (x INT)")
            s.execute("INSERT INTO t (x) VALUES (1)")
            s.execute("BEGIN")
            s.execute("COMMIT")
            self.assertGreater(s.stats["statements_executed"], 0)
            self.assertGreater(s.stats["transactions_committed"], 0)


# ═══════════════════════════════════════════════════════════════════════════
# EXPLAIN
# ═══════════════════════════════════════════════════════════════════════════

class TestExplain(SessionTestBase):

    def test_explain_select(self):
        """EXPLAIN SELECT shows plan without executing."""
        with Session(self.test_dir) as s:
            s.execute("CREATE TABLE t (x INT)")
            _, plan, _ = s.execute("EXPLAIN SELECT * FROM t")
            self.assertIn("Logical Plan", plan)
            self.assertIn("Physical Plan", plan)

    def test_explain_logical_only(self):
        """EXPLAIN LOGICAL shows only logical plan."""
        with Session(self.test_dir) as s:
            s.execute("CREATE TABLE t (x INT)")
            _, plan, _ = s.execute("EXPLAIN LOGICAL SELECT * FROM t")
            self.assertIn("Logical Plan", plan)
            self.assertNotIn("Physical Plan", plan)

    def test_explain_physical_only(self):
        """EXPLAIN PHYSICAL shows only physical plan."""
        with Session(self.test_dir) as s:
            s.execute("CREATE TABLE t (x INT)")
            _, plan, _ = s.execute("EXPLAIN PHYSICAL SELECT * FROM t")
            self.assertNotIn("Logical Plan", plan)
            self.assertIn("Physical Plan", plan)

    def test_explain_does_not_modify_data(self):
        """EXPLAIN does not actually execute the statement."""
        with Session(self.test_dir) as s:
            s.execute("CREATE TABLE t (x INT)")
            s.execute("INSERT INTO t (x) VALUES (1)")
            s.execute("EXPLAIN SELECT * FROM t")
            # verify no side effects — just the one row
            rows, _, _ = s.execute("SELECT * FROM t")
            result = [r.values for r in rows]
            self.assertEqual(len(result), 1)


# ═══════════════════════════════════════════════════════════════════════════
# Renderer
# ═══════════════════════════════════════════════════════════════════════════

class TestRenderer(unittest.TestCase):

    def _make_rows(self, dicts):
        """Create mock rows from list of dicts."""
        class MockRow:
            def __init__(self, d):
                self.values = d
        return [MockRow(d) for d in dicts]

    def test_table_mode_basic(self):
        """Table mode renders aligned columns."""
        buf = io.StringIO()
        r = Renderer(output=buf)
        r.show_timer = False

        rows = self._make_rows([
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ])
        count = r.render_rows(iter(rows))
        output = buf.getvalue()
        self.assertEqual(count, 2)
        self.assertIn("Alice", output)
        self.assertIn("Bob", output)
        self.assertIn("+", output)  # Separator
        self.assertIn("|", output)  # Column delimiter

    def test_null_display(self):
        """NULL values displayed as 'NULL'."""
        buf = io.StringIO()
        r = Renderer(output=buf)
        r.show_timer = False

        rows = self._make_rows([{"val": None}])
        r.render_rows(iter(rows))
        self.assertIn("NULL", buf.getvalue())

    def test_vertical_mode(self):
        """Vertical mode renders key: value pairs."""
        buf = io.StringIO()
        r = Renderer(output=buf)
        r.mode = "vertical"
        r.show_timer = False

        rows = self._make_rows([{"id": 1, "name": "Alice"}])
        r.render_rows(iter(rows))
        output = buf.getvalue()
        self.assertIn("Row 1", output)
        self.assertIn("name: Alice", output)

    def test_raw_mode(self):
        """Raw mode renders pipe-separated values."""
        buf = io.StringIO()
        r = Renderer(output=buf)
        r.mode = "raw"
        r.show_timer = False

        rows = self._make_rows([{"id": 1, "name": "Alice"}])
        r.render_rows(iter(rows))
        output = buf.getvalue()
        self.assertIn("1|Alice", output)

    def test_display_limit(self):
        """Display limit truncates output."""
        buf = io.StringIO()
        r = Renderer(output=buf)
        r.show_timer = False
        r.display_limit = 2

        rows = self._make_rows([{"x": i} for i in range(10)])
        count = r.render_rows(iter(rows))
        self.assertEqual(count, 2)

    def test_error_classification(self):
        """Error types are classified with user-friendly prefixes."""
        buf = io.StringIO()
        r = Renderer(output=buf)

        r.render_error(RuntimeError("test"))
        self.assertIn("ExecutionError", buf.getvalue())

    def test_streaming_large_result(self):
        """Large result sets stream without full materialization."""
        buf = io.StringIO()
        r = Renderer(output=buf)
        r.show_timer = False

        # Generator — not a list (streaming)
        def gen_rows():
            for i in range(1000):
                yield type('Row', (), {'values': {'x': i}})()

        count = r.render_rows(gen_rows())
        self.assertEqual(count, 1000)

    def test_headers_off(self):
        """Headers can be disabled."""
        buf = io.StringIO()
        r = Renderer(output=buf)
        r.show_headers = False
        r.show_timer = False

        rows = self._make_rows([{"id": 1}])
        r.render_rows(iter(rows))
        output = buf.getvalue()
        # Should not contain header separator at top
        lines = output.strip().split("\n")
        # No +----+ line at start
        self.assertFalse(lines[0].startswith("+"))


# ═══════════════════════════════════════════════════════════════════════════
# Script Execution
# ═══════════════════════════════════════════════════════════════════════════

class TestScriptExecution(SessionTestBase):

    def test_script_execution(self):
        """Execute a multi-statement SQL script."""
        script = """
        CREATE TABLE t (id INT, name STRING);
        INSERT INTO t (id, name) VALUES (1, 'Alice');
        INSERT INTO t (id, name) VALUES (2, 'Bob');
        """
        script_path = os.path.join(self.test_dir, "test.sql")
        with open(script_path, "w") as f:
            f.write(script)

        # Use main module's execute_script
        from main import execute_script
        db_path = os.path.join(self.test_dir, "script_db")
        execute_script(db_path, script_path)

        # Verify data persisted
        with Session(db_path) as s:
            rows, _, _ = s.execute("SELECT * FROM t")
            result = [r.values for r in rows]
            self.assertEqual(len(result), 2)

    def test_script_with_transactions(self):
        """Script with explicit BEGIN/COMMIT."""
        script = """
        CREATE TABLE t (x INT);
        BEGIN;
        INSERT INTO t (x) VALUES (1);
        INSERT INTO t (x) VALUES (2);
        COMMIT;
        """
        script_path = os.path.join(self.test_dir, "txn_script.sql")
        with open(script_path, "w") as f:
            f.write(script)

        from main import execute_script
        db_path = os.path.join(self.test_dir, "txn_db")
        execute_script(db_path, script_path)

        with Session(db_path) as s:
            rows, _, _ = s.execute("SELECT * FROM t")
            result = [r.values for r in rows]
            self.assertEqual(len(result), 2)


# ═══════════════════════════════════════════════════════════════════════════
# Parser Extensions
# ═══════════════════════════════════════════════════════════════════════════

class TestParserExtensions(unittest.TestCase):

    def test_parse_begin(self):
        from parser import parse
        from parser.ast_nodes import BeginStmt
        stmt = parse("BEGIN")
        self.assertIsInstance(stmt, BeginStmt)

    def test_parse_begin_transaction(self):
        from parser import parse
        from parser.ast_nodes import BeginStmt
        stmt = parse("BEGIN TRANSACTION")
        self.assertIsInstance(stmt, BeginStmt)

    def test_parse_commit(self):
        from parser import parse
        from parser.ast_nodes import CommitStmt
        stmt = parse("COMMIT")
        self.assertIsInstance(stmt, CommitStmt)

    def test_parse_rollback(self):
        from parser import parse
        from parser.ast_nodes import RollbackStmt
        stmt = parse("ROLLBACK")
        self.assertIsInstance(stmt, RollbackStmt)

    def test_parse_explain(self):
        from parser import parse
        from parser.ast_nodes import ExplainStmt, SelectStmt
        stmt = parse("EXPLAIN SELECT * FROM t")
        self.assertIsInstance(stmt, ExplainStmt)
        self.assertIsInstance(stmt.inner, SelectStmt)
        self.assertEqual(stmt.level, "both")

    def test_parse_explain_logical(self):
        from parser import parse
        from parser.ast_nodes import ExplainStmt
        stmt = parse("EXPLAIN LOGICAL SELECT * FROM t")
        self.assertIsInstance(stmt, ExplainStmt)
        self.assertEqual(stmt.level, "logical")

    def test_parse_explain_physical(self):
        from parser import parse
        from parser.ast_nodes import ExplainStmt
        stmt = parse("EXPLAIN PHYSICAL SELECT * FROM t")
        self.assertIsInstance(stmt, ExplainStmt)
        self.assertEqual(stmt.level, "physical")


if __name__ == "__main__":
    unittest.main()
