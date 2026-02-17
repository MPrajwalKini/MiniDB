
"""
Database Metadata Manager
=========================
Manages a single database's metadata:
- Schemas (name -> oid)
- Persistence (database.json)
"""

import json
import os
from dataclasses import dataclass
from typing import Dict, Optional, List
from pathlib import Path
from catalog.schema import Schema

@dataclass
class SchemaInfo:
    oid: int
    name: str
    path: str

class Database:
    """
    Manages metadata for a single database (list of schemas).
    Persisted in <db_dir>/database.json.
    """
    def __init__(self, db_dir: str, name: str, oid: int):
        self.db_dir = Path(db_dir)
        self.name = name
        self.oid = oid
        self.meta_path = self.db_dir / "database.json"
        
        self.schemas: Dict[str, SchemaInfo] = {}
        self._schemas_by_oid: Dict[int, SchemaInfo] = {}
        self._active_schemas: Dict[str, Schema] = {}  # Cache loaded schemas

        self._load()

    def create_schema(self, name: str, oid: int) -> SchemaInfo:
        if name in self.schemas:
            raise ValueError(f"Schema '{name}' already exists in database '{self.name}'.")
        
        path = f"schema_{oid}"
        info = SchemaInfo(oid=oid, name=name, path=path)
        self.schemas[name] = info
        self._schemas_by_oid[oid] = info
        
        # Ensure physical directory exists
        schema_path = self.db_dir / path
        schema_path.mkdir(parents=True, exist_ok=True)
        
        # Initialize schema metadata file
        Schema(str(schema_path), name, oid)._save()
        
        self._save()
        return info

    def drop_schema(self, name: str):
        if name not in self.schemas:
            raise ValueError(f"Schema '{name}' does not exist.")
        
        # TODO: Handle cascading drops of tables inside schema?
        # For now, just remove metadata. Physical files remain until implemented.
        info = self.schemas.pop(name)
        if info.oid in self._schemas_by_oid:
             del self._schemas_by_oid[info.oid]

        if name in self._active_schemas:
            del self._active_schemas[name]
        self._save()

    def get_schema(self, name: str) -> Optional[Schema]:
        """Lazy load schema object."""
        if name not in self.schemas:
            return None
        
        if name in self._active_schemas:
            return self._active_schemas[name]
        
        info = self.schemas[name]
        schema_path = self.db_dir / info.path
        schema = Schema(str(schema_path), info.name, info.oid)
        self._active_schemas[name] = schema
        return schema

    def list_schemas(self) -> List[SchemaInfo]:
        return list(self.schemas.values())

    def _load(self):
        if not self.meta_path.exists():
            return

        with open(self.meta_path, 'r') as f:
            data = json.load(f)
            
        if data.get("oid") != self.oid:
            # warning?
            pass

        self.schemas = {}
        self._schemas_by_oid = {}
        for s in data.get("schemas", []):
            info = SchemaInfo(
                oid=s["oid"], 
                name=s["name"],
                path=s.get("path", f"schema_{s['oid']}")
            )
            self.schemas[info.name] = info
            self._schemas_by_oid[info.oid] = info

    def _save(self):
        data = {
            "oid": self.oid,
            "name": self.name,
            "schemas": [
                {"oid": s.oid, "name": s.name, "path": s.path}
                for s in self.schemas.values()
            ]
        }
        
        tmp_path = self.meta_path.with_suffix(".tmp")
        with open(tmp_path, 'w') as f:
            json.dump(data, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        tmp_path.replace(self.meta_path)
