
import os
import shutil
import tempfile
import unittest
from pathlib import Path

from catalog.bootstrap import bootstrap_system
from catalog.resolver import Resolver, ObjectNotFoundError
from catalog.database import Database
from catalog.schema import Schema

class TestResolver(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.system = bootstrap_system(self.test_dir)
        self.resolver = Resolver(self.system)
        
        # Setup data
        # Default DB (OID 1000)
        #   Schemas: public (OID 1001), information_schema (OID 1002)
        # Create 'users' table in public
        db_info = self.system.get_database("default")
        self.default_db_oid = db_info.oid
        path = self.system.data_root / db_info.path
        self.db = Database(str(path), db_info.name, db_info.oid)
        
        public = self.db.get_schema("public")
        self.public_oid = public.oid
        public.create_table("users", 2001)
        
        # Create 'sales' schema and 'orders' table
        sales_info = self.db.create_schema("sales", 1003)
        self.sales_oid = sales_info.oid
        # We need to get Schema object to create table
        sales = self.db.get_schema("sales")
        sales.create_table("orders", 2002)
        
        # Create 'users' table in 'sales' (shadowing test)
        sales.create_table("users", 2003)
        
        # Create another DB 'analytics'
        ana_info = self.system.create_database("analytics")
        self.ana_oid = ana_info.oid
        ana_path = self.system.data_root / ana_info.path
        self.ana_db = Database(str(ana_path), ana_info.name, ana_info.oid)
        
        # Access 'analytics' needs bootstrap of public?
        # Manually create schema
        ana_public_info = self.ana_db.create_schema("public", 3001)
        ana_public = self.ana_db.get_schema("public")
        ana_public.create_table("logs", 3002)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_fqn_resolution(self):
        """Test db.schema.table resolution."""
        # Resolve default.public.users
        db, schema, table = self.resolver.resolve_table("default.public.users", 
                                                        self.default_db_oid, [])
        self.assertEqual(db.name, "default")
        self.assertEqual(schema.name, "public")
        self.assertEqual(table.name, "users")

        # Resolve analytics.public.logs
        db, schema, table = self.resolver.resolve_table("analytics.public.logs",
                                                        self.default_db_oid, [])
        self.assertEqual(db.name, "analytics")
        self.assertEqual(table.name, "logs")

    def test_partial_qual_resolution(self):
        """Test schema.table resolution in current DB."""
        # public.users
        db, schema, table = self.resolver.resolve_table("public.users", 
                                                        self.default_db_oid, [])
        self.assertEqual(db.name, "default")
        self.assertEqual(table.name, "users")
        self.assertEqual(table.oid, 2001)

        # sales.orders
        db, schema, table = self.resolver.resolve_table("sales.orders", 
                                                        self.default_db_oid, [])
        self.assertEqual(schema.name, "sales")
        self.assertEqual(table.name, "orders")

    def test_unqualified_resolution_search_path(self):
        """Test table resolution using search path."""
        # Path: [public]
        search_path = [self.public_oid]
        db, schema, table = self.resolver.resolve_table("users", 
                                                        self.default_db_oid, search_path)
        self.assertEqual(schema.name, "public")
        self.assertEqual(table.oid, 2001) # public.users

        # Path: [sales, public] -> should find sales.users (shadowing)
        search_path = [self.sales_oid, self.public_oid]
        db, schema, table = self.resolver.resolve_table("users", 
                                                        self.default_db_oid, search_path)
        self.assertEqual(schema.name, "sales")
        self.assertEqual(table.oid, 2003) # sales.users

        # Path: [sales] -> orders found
        search_path = [self.sales_oid]
        db, schema, table = self.resolver.resolve_table("orders", 
                                                        self.default_db_oid, search_path)
        self.assertEqual(table.name, "orders")

    def test_not_found(self):
        search_path = [self.public_oid]
        with self.assertRaises(ObjectNotFoundError):
            self.resolver.resolve_table("nonexistent", self.default_db_oid, search_path)
            
        with self.assertRaises(ObjectNotFoundError):
            self.resolver.resolve_table("public.nonexistent", self.default_db_oid, [])

        with self.assertRaises(ObjectNotFoundError):
            self.resolver.resolve_table("default.sales.nonexistent", self.default_db_oid, [])

if __name__ == '__main__':
    unittest.main()
