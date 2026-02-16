"""
MiniDB Phase 8 CREATE INDEX Tests
=================================
Tests for CREATE INDEX and DROP INDEX via CLI Session.
"""

import shutil
import tempfile
import unittest
import os

from cli.session import Session, SessionError
from parser import parse
from parser.ast_nodes import CreateIndexStmt, DropIndexStmt

class TestCLIIndex(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="minidb_cli_idx_")
        self.session = Session(self.test_dir)
        
        # Create a base table with data
        self.session.execute("CREATE TABLE users (id INT, name STRING, age INT)")
        self.session.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
        self.session.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")
        self.session.execute("INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)")

    def tearDown(self):
        self.session.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_create_index_syntax(self):
        """Verify CREATE INDEX syntax parsing."""
        stmt = parse("CREATE INDEX idx_age ON users (age)")
        self.assertIsInstance(stmt, CreateIndexStmt)
        self.assertEqual(stmt.index_name, "idx_age")
        self.assertEqual(stmt.table_name, "users")
        self.assertEqual(stmt.column_name, "age")

    def test_drop_index_syntax(self):
        """Verify DROP INDEX syntax parsing."""
        stmt = parse("DROP INDEX idx_age")
        self.assertIsInstance(stmt, DropIndexStmt)
        self.assertEqual(stmt.index_name, "idx_age")

    def test_create_index_execution(self):
        """Execute CREATE INDEX and verify success message."""
        _, msg, _ = self.session.execute("CREATE INDEX idx_age ON users (age)")
        self.assertIn("created on users(age)", msg)
        
        # Verify it shows up in catalog (via internal check)
        # Session exposes catalog
        idx_info = self.session.catalog.get_index("idx_age")
        self.assertIsNotNone(idx_info)
        self.assertEqual(idx_info["table"], "users")
        self.assertEqual(idx_info["column"], "age")

    def test_create_index_and_explain(self):
        """Verify EXPLAIN shows usage of the new index."""
        self.session.execute("CREATE INDEX idx_age ON users (age)")
        
        # Query with filter on indexed column
        sql = "EXPLAIN PHYSICAL SELECT * FROM users WHERE age > 28"
        _, plan, _ = self.session.execute(sql)
        
        # Should see IndexScan
        self.assertIn("IndexScanExec", plan)
        self.assertIn("idx_age", plan)

    def test_drop_index_execution(self):
        """Execute DROP INDEX and verify."""
        self.session.execute("CREATE INDEX idx_age ON users (age)")
        _, msg, _ = self.session.execute("DROP INDEX idx_age")
        self.assertIn("dropped", msg)
        
        # Verify gone from catalog
        idx_info = self.session.catalog.get_index("idx_age")
        self.assertIsNone(idx_info)

    def test_create_index_on_non_existent_table(self):
        """CREATE INDEX on missing table raises error."""
        with self.assertRaisesRegex(SessionError, "Table 'missing' does not exist"):
            self.session.execute("CREATE INDEX idx_bad ON missing (id)")

    def test_create_index_on_missing_column(self):
        """CREATE INDEX on missing column raises error."""
        with self.assertRaisesRegex(SessionError, "Column 'salary' not found"):
            self.session.execute("CREATE INDEX idx_bad ON users (salary)")

    def test_drop_missing_index(self):
        """DROP INDEX on missing index raises error."""
        with self.assertRaisesRegex(SessionError, "Index 'missing_idx' not found"):
            self.session.execute("DROP INDEX missing_idx")

if __name__ == "__main__":
    unittest.main()
