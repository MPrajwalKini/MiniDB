
"""
System Bootstrap & Migration
============================
Initializes the MiniDB catalog structure.
Handles migration from legacy single-file catalog to multi-database hierarchy.
"""

import os
import shutil
import json
from pathlib import Path
from typing import Optional

from catalog.system_catalog import SystemCatalog
from catalog.database import Database
from catalog.schema import Schema
# Import LegacyCatalog only if needed for migration to avoid circular deps if any
# but legacy.py imports storage.schema which is fine.

def bootstrap_system(data_root: str) -> SystemCatalog:
    """
    Ensure the system catalog exists and is initialized.
    1. Initialize SystemCatalog.
    2. Ensure 'default' database exists (OID 1).
    3. Ensure 'public' (OID 2) and 'information_schema' (OID 3) exist in default DB.
    4. If legacy 'catalog.dat' exists, migrate contents to 'default.public'.
    """
    root = Path(data_root)
    root.mkdir(parents=True, exist_ok=True)
    
    system = SystemCatalog(str(root))
    
    # 1. Default Database
    default_db_name = "default"
    db_info = system.get_database(default_db_name)
    
    if not db_info:
        # Bootstrap identifiers if fresh system
        # We want default OIDs to be consistent if empty?
        # SystemCatalog init starts at 1000.
        # We can reserve 1 for default DB?
        # allocate_oid() gives unique.
        
        print(f"[Bootstrap] Initializing '{default_db_name}' database...")
        db_info = system.create_database(default_db_name)
        
    db_path = root / db_info.path
    db = Database(str(db_path), db_info.name, db_info.oid)
    
    # 2. Standard Schemas
    _ensure_schema(system, db, "public")
    _ensure_schema(system, db, "information_schema")
    
    # 3. Legacy Migration
    legacy_path = root / "catalog.dat"
    if legacy_path.exists():
        _migrate_legacy(root, system, db, legacy_path)
        
    return system

def _ensure_schema(system: SystemCatalog, db: Database, schema_name: str):
    """Ensure a schema exists in the database."""
    if schema_name not in db.schemas:
        oid = system.allocate_oid()
        print(f"[Bootstrap] Creating schema '{schema_name}' (OID {oid})...")
        db.create_schema(schema_name, oid)

def _migrate_legacy(root: Path, system: SystemCatalog, db: Database, legacy_path: Path):
    """Migrate legacy catalog to default.public."""
    print("[Bootstrap] Found legacy catalog. Not supported in this version.")
    # For robust migration we would:
    # 1. Load LegacyCatalog
    # 2. Get 'public' schema from db
    # 3. For each table:
    #    - Allocate new OID
    #    - Move .tbl file to schema dir with new name (table_{oid}.tbl)
    #    - Register in public schema
    #    - Move/Register indexes
    # 4. Rename catalog.dat -> catalog.dat.migrated
    
    # Implementation of full migration:
    try:
        from catalog.legacy import Catalog as LegacyCatalog
        
        legacy = LegacyCatalog(str(root))
        legacy.load()
        
        # Get public schema
        public_schema = db.get_schema("public")
        if not public_schema:
           # Should exist from _ensure_schema
           raise RuntimeError("Public schema missing during migration")
           
        print(f"[Bootstrap] Migrating {len(legacy._tables)} tables to 'default.public'...")
        
        for table_name in legacy.list_tables():
            # Get legacy metadata
            old_meta = legacy.get_table(table_name)
            old_file = legacy.get_table_file(table_name) # Absolute path
            
            if not old_file or not os.path.exists(old_file):
                print(f"[Bootstrap] Warning: Table file for '{table_name}' missing. Skipping.")
                continue
                
            # Allocate new OID
            table_oid = system.allocate_oid()
            
            # Create new table metadata
            public_schema.create_table(table_name, table_oid)
            
            # Move file
            # New path: db_dir/schema_dir/table_{oid}.tbl
            # public_schema.schema_dir is strict path
            new_filename = f"table_{table_oid}.tbl"
            new_path = public_schema.schema_dir / new_filename
            
            shutil.move(old_file, new_path)
            print(f"  - Migrated table '{table_name}' -> {new_filename}")
            
            # Migrate Indexes
            indexes = legacy.get_indexes_for_table(table_name)
            for idx in indexes:
                idx_name = idx["name"]
                idx_col = idx["column"]
                old_idx_file = root / idx["file"] # Legacy implies relative to data_root?
                # catalog.legacy.get_index returns file name relative to data_dir?
                # legacy stores file name.
                
                if old_idx_file.exists():
                    idx_oid = system.allocate_oid()
                    public_schema.create_index(idx_name, idx_oid, table_name, idx_col)
                    new_idx_path = public_schema.schema_dir / f"index_{idx_oid}.idx"
                    shutil.move(old_idx_file, new_idx_path)
                    print(f"    - Migrated index '{idx_name}' -> index_{idx_oid}.idx")

        # Rename legacy catalog to prevent re-migration
        legacy_path.rename(legacy_path.with_suffix(".dat.migrated"))
        print("[Bootstrap] Migration complete. Legacy catalog renamed.")
        
    except Exception as e:
        print(f"[Bootstrap] Migration FAILED: {e}")
        # Optionally abort or continue
        raise e
