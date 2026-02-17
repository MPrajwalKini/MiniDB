
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from catalog.system_catalog import SystemCatalog

class TestSystemCatalog(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.catalog = SystemCatalog(self.test_dir)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_oid_allocation_persistence(self):
        """Test that OIDs are allocated monotonically and persisted."""
        oid1 = self.catalog.allocate_oid()
        oid2 = self.catalog.allocate_oid()
        self.assertLess(oid1, oid2)
        
        # Verify persistence by reloading
        new_catalog = SystemCatalog(self.test_dir)
        oid3 = new_catalog.allocate_oid()
        self.assertLess(oid2, oid3)
        self.assertEqual(oid3, oid2 + 1)

    def test_oid_allocation_continuity(self):
        """Test allocating many OIDs."""
        start_oid = self.catalog.next_oid
        for i in range(100):
            self.catalog.allocate_oid()
        
        self.assertEqual(self.catalog.next_oid, start_oid + 100)

    def test_create_get_drop_database(self):
        """Test database registry operations."""
        db = self.catalog.create_database("test_db")
        self.assertEqual(db.name, "test_db")
        self.assertTrue(db.oid >= 1000)
        self.assertEqual(db.path, f"db_{db.oid}")

        # Persistence check
        new_catalog = SystemCatalog(self.test_dir)
        loaded_db = new_catalog.get_database("test_db")
        self.assertIsNotNone(loaded_db)
        self.assertEqual(loaded_db.oid, db.oid)

        # Drop
        self.catalog.drop_database("test_db")
        self.assertIsNone(self.catalog.get_database("test_db"))
        
        # Persistence of drop due to _save in drop
        new_catalog_2 = SystemCatalog(self.test_dir)
        self.assertIsNone(new_catalog_2.get_database("test_db"))

    def test_duplicate_database_error(self):
        self.catalog.create_database("db1")
        with self.assertRaises(ValueError):
            self.catalog.create_database("db1")

if __name__ == '__main__':
    unittest.main()
