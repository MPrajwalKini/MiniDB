
"""
System Catalog Manager
======================
Manages the root level of the MiniDB catalog hierarchy:
- System-wide OID allocation
- Database management (create/drop/list)
- System persistence (system_catalog.json)
"""

import json
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pathlib import Path

# Reserve low OIDs for system objects
OID_SYSTEM_RESERVED_MAX = 999
OID_USER_START = 1000

@dataclass
class DatabaseInfo:
    """Metadata for a registered database."""
    oid: int
    name: str
    path: str  # Relative path to data root, e.g. "db_1001"

class SystemCatalog:
    """
    The root catalog for MiniDB.
    
    Responsibilities:
    1. Manage global OID allocation (monotonic, durable).
    2. Track all databases (name -> oid mapping).
    3. Persist system state to system_catalog.json.
    """

    def __init__(self, data_root: str):
        self.data_root = Path(data_root)
        self.catalog_path = self.data_root / "system_catalog.json"
        
        # Persistent state
        self.next_oid: int = OID_USER_START
        self.databases: Dict[str, DatabaseInfo] = {}  # name -> info
        self.databases_by_oid: Dict[int, DatabaseInfo] = {} # oid -> info
        
        # Versioning
        self.version: int = 1

        self._load()

    def allocate_oid(self) -> int:
        """
        Atomically allocate a new OID and persist the state.
        Returns the allocated OID.
        """
        oid = self.next_oid
        self.next_oid += 1
        self._save()
        return oid

    def create_database(self, name: str) -> DatabaseInfo:
        """
        Register a new database. 
        Note: Does not create directories, just metadata.
        """
        if name in self.databases:
            raise ValueError(f"Database '{name}' already exists.")
            
        oid = self.allocate_oid()
        # Physical path uses OID
        path = f"db_{oid}"
        
        db_info = DatabaseInfo(oid=oid, name=name, path=path)
        self.databases[name] = db_info
        self.databases_by_oid[oid] = db_info
        self._save()
        
        return db_info

    def get_database(self, name: str) -> Optional[DatabaseInfo]:
        return self.databases.get(name)
        
    def get_database_by_oid(self, oid: int) -> Optional[DatabaseInfo]:
        return self.databases_by_oid.get(oid)

    def list_databases(self) -> List[DatabaseInfo]:
        return list(self.databases.values())

    def drop_database(self, name: str):
        """Unregister a database."""
        if name not in self.databases:
            raise ValueError(f"Database '{name}' does not exist.")
            
        db = self.databases.pop(name)
        self.databases_by_oid.pop(db.oid)
        self._save()

    def _load(self):
        """Load state from disk if exists."""
        if not self.catalog_path.exists():
            # Initialize with default/bootstrap state if needed, 
            # or just start empty. The bootstrap logic will handle 
            # creating 'default' DB if missing.
            return

        try:
            with open(self.catalog_path, 'r') as f:
                data = json.load(f)
            
            self.version = data.get("version", 1)
            self.next_oid = data.get("next_oid", OID_USER_START)
            
            for db_data in data.get("databases", []):
                db = DatabaseInfo(
                    oid=db_data["oid"],
                    name=db_data["name"],
                    path=db_data.get("path", f"db_{db_data['oid']}")
                )
                self.databases[db.name] = db
                self.databases_by_oid[db.oid] = db
                
        except (json.JSONDecodeError, KeyError) as e:
            # Decide on corruption policy: Fail fast
            raise RuntimeError(f"Corrupted system catalog at {self.catalog_path}: {e}")

    def _save(self):
        """Atomically save state to disk."""
        data = {
            "version": self.version,
            "next_oid": self.next_oid,
            "databases": [
                {"oid": db.oid, "name": db.name, "path": db.path}
                for db in self.databases.values()
            ]
        }
        
        # Atomic write: write to temp -> fsync -> rename
        tmp_path = self.catalog_path.with_suffix(".tmp")
        with open(tmp_path, 'w') as f:
            json.dump(data, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
            
        try:
            tmp_path.replace(self.catalog_path)
        except OSError as e:
            # On Windows replace might fail if dest exists and is open (unlikely here)
            # or permission issues.
            # Using os.replace is atomic on POSIX, atomic on Py3.3+ Windows?
            # pathlib.Path.replace wraps os.replace.
            if os.name == 'nt' and self.catalog_path.exists():
                 # Should be fine on modern Python/Windows
                 pass
            raise e
