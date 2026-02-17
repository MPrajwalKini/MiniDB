
import os
import shutil
import tempfile
import unittest
import json
from pathlib import Path
from catalog.bootstrap import bootstrap_system
from catalog.system_catalog import SystemCatalog

class TestBootstrap(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_fresh_bootstrap(self):
        """Test bootstrapping a new empty system."""
        system = bootstrap_system(self.test_dir)
        
        # Verify SystemCatalog
        self.assertTrue(os.path.exists(os.path.join(self.test_dir, "system_catalog.json")))
        
        # Verify Default Database
        db_info = system.get_database("default")
        self.assertIsNotNone(db_info)
        self.assertTrue(db_info.oid >= 1000)
        # SystemCatalog starts at 1000 usually, but maybe bootstrap logic differs?
        # My bootstrap.py doesn't force OID 1. It calls create_database.
        # So it might be 1000.
        
        # Verify Database structure
        db_path = os.path.join(self.test_dir, db_info.path)
        self.assertTrue(os.path.exists(os.path.join(db_path, "database.json")))
        
        # Verify Schemas
        # We need to load Database to check schemas
        from catalog.database import Database
        db = Database(db_path, db_info.name, db_info.oid)
        self.assertIn("public", db.schemas)
        self.assertIn("information_schema", db.schemas)
        
        # Verify public schema directory
        public_info = db.schemas["public"]
        public_path = os.path.join(db_path, public_info.path)
        self.assertTrue(os.path.exists(os.path.join(public_path, "schema.json")))

    def test_legacy_migration(self):
        """Test migrating a legacy catalog.dat."""
        # Setup legacy environment
        legacy_file = os.path.join(self.test_dir, "catalog.dat")
        legacy_data = {
            "format_version": 1,
            "tables": {
                "users": {
                    "name": "users",
                    "file": "users.tbl",
                    "schema": {"columns": [{"name": "id", "type": "INTEGER"}]},
                    "created_at": "2023-01-01T00:00:00+00:00"
                }
            },
            "indexes": {}
        }
        with open(legacy_file, 'w') as f:
            json.dump(legacy_data, f)
            
        # Create dummy table file
        table_file = os.path.join(self.test_dir, "users.tbl")
        with open(table_file, 'w') as f:
            f.write("dummy content")
            
        # Run bootstrap
        system = bootstrap_system(self.test_dir)
        
        # Check migration
        db_info = system.get_database("default")
        from catalog.database import Database
        db = Database(os.path.join(self.test_dir, db_info.path), db_info.name, db_info.oid)
        
        public = db.get_schema("public")
        self.assertIsNotNone(public.get_table("users"))
        
        # Check file moved
        table_path = public.schema_dir / f"table_{public.get_table('users').oid}.tbl"
        self.assertTrue(table_path.exists())
        self.assertFalse(os.path.exists(table_file)) # Old file gone
        
        # Check legacy catalog renamed
        self.assertTrue(os.path.exists(os.path.join(self.test_dir, "catalog.dat.migrated")))

if __name__ == '__main__':
    unittest.main()
