
"""
Schema Metadata Manager
=======================
Manages a single schema's metadata:
- Tables and Indexes (name -> oid)
- Persistence (schema.json)
"""

import json
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pathlib import Path

@dataclass
class TableInfo:
    oid: int
    name: str

@dataclass
class IndexInfo:
    oid: int
    name: str
    table_name: str
    column: str

class Schema:
    """
    Manages metadata for a single schema (tables, indexes, views).
    Persisted in <schema_dir>/schema.json.
    """
    def __init__(self, schema_dir: str, name: str, oid: int):
        self.schema_dir = Path(schema_dir)
        self.name = name
        self.oid = oid
        self.meta_path = self.schema_dir / "schema.json"
        
        self.tables: Dict[str, TableInfo] = {}
        self.indexes: Dict[str, IndexInfo] = {}

        self._load()

    def create_table(self, name: str, oid: int, save: bool = True) -> TableInfo:
        print(f"DEBUG_SCHEMA: create_table '{name}' (oid={oid}) in '{self.name}'. save={save}")
        if name in self.tables:
            raise ValueError(f"Table '{name}' already exists in schema '{self.name}'.")
        
        info = TableInfo(oid=oid, name=name)
        self.tables[name] = info
        if save:
            self._save()
        return info

    def drop_table(self, name: str, save: bool = True):
        print(f"DEBUG_SCHEMA: drop_table '{name}' in '{self.name}'. save={save}")
        info = self.tables.get(name)
        if not info:
             raise ValueError(f"Table '{name}' does not exist.")
        del self.tables[name]
        if save:
            self._save()
        return info # Return info for undo

    def get_table(self, name: str) -> Optional[TableInfo]:
        return self.tables.get(name)

    def create_index(self, name: str, oid: int, table_name: str, column: str, save: bool = True) -> IndexInfo:
        if name in self.indexes:
            raise ValueError(f"Index '{name}' already exists.")
        
        info = IndexInfo(oid=oid, name=name, table_name=table_name, column=column)
        self.indexes[name] = info
        if save:
            self._save()
        return info

    def drop_index(self, name: str, save: bool = True):
        if name in self.indexes:
            info = self.indexes[name]
            del self.indexes[name]
            if save:
                self._save()
            return info

    def get_index(self, name: str) -> Optional[IndexInfo]:
        return self.indexes.get(name)

    def _load(self):
        if not self.meta_path.exists():
            return
        
        with open(self.meta_path, 'r') as f:
            data = json.load(f)
            
        # Verify identity?
        if data.get("oid") != self.oid:
            # Metadata OID mismatch? 
            pass 
        
        self.tables = {
            t["name"]: TableInfo(oid=t["oid"], name=t["name"])
            for t in data.get("tables", [])
        }
        self.indexes = {
            i["name"]: IndexInfo(oid=i["oid"], name=i["name"], 
                                 table_name=i["table_name"], column=i["column"])
            for i in data.get("indexes", [])
        }

    def _save(self):
        data = {
            "oid": self.oid,
            "name": self.name,
            "tables": [{"oid": t.oid, "name": t.name} for t in self.tables.values()],
            "indexes": [
                {"oid": i.oid, "name": i.name, "table_name": i.table_name, "column": i.column}
                for i in self.indexes.values()
            ]
        }
        
        tmp_path = self.meta_path.with_suffix(".tmp")
        with open(tmp_path, 'w') as f:
            json.dump(data, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        tmp_path.replace(self.meta_path)
