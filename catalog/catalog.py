"""
MiniDB Catalog Manager
======================
Persists metadata about all tables and indexes in catalog.dat.
Tracks table name → file mapping, schemas, creation times.

Safety guarantees:
  - Atomic writes: catalog is written to a temp file first, then
    atomically renamed (os.replace). This prevents partial writes
    from corrupting the catalog on crash.
  - Table names are immutable after creation (lowercase normalized).
  - Schema evolution is NOT supported in v1. Altering a table schema
    requires drop + recreate. Placeholder exists for future versions.

Teaching note:
  PostgreSQL stores catalog data in system tables (pg_class, pg_attribute,
  pg_index) that are themselves stored as heap tables. We use a simpler
  approach: a single JSON file. This avoids bootstrap complexity while
  demonstrating the same concept — metadata about data.
"""

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from storage.schema import Schema


# Catalog format version — increment on breaking changes
CATALOG_FORMAT_VERSION = 1

# Schema evolution placeholder
SCHEMA_EVOLUTION_NOTE = (
    "Schema evolution is not supported in v1. "
    "To alter a table, drop and recreate it. "
    "A future version may support ALTER TABLE with migration."
)


class Catalog:
    """
    Database catalog — stores metadata about all tables.

    Persisted as catalog.dat (JSON) in the data directory.
    Loaded into memory on database start; written back on changes.

    Atomic write strategy:
      1. Write to catalog.dat.tmp
      2. os.replace(tmp, catalog.dat) — atomic on POSIX, near-atomic on Windows
    """

    def __init__(self, data_dir: str):
        self._data_dir = os.path.abspath(data_dir)
        self._catalog_file = os.path.join(self._data_dir, "catalog.dat")
        self._tables: Dict[str, Dict[str, Any]] = {}
        self._indexes: Dict[str, Dict[str, Any]] = {}
        self._loaded = False

    @property
    def data_dir(self) -> str:
        return self._data_dir

    # ─── Load / Save ────────────────────────────────────────────────

    def load(self) -> None:
        """Load catalog from disk. Creates empty catalog if file doesn't exist."""
        os.makedirs(self._data_dir, exist_ok=True)

        if os.path.exists(self._catalog_file):
            with open(self._catalog_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Validate format version
            stored_version = data.get("format_version", 0)
            if stored_version > CATALOG_FORMAT_VERSION:
                raise ValueError(
                    f"Catalog format version {stored_version} is newer than "
                    f"supported version {CATALOG_FORMAT_VERSION}")

            self._tables = data.get("tables", {})
            self._indexes = data.get("indexes", {})
        else:
            self._tables = {}
            self._indexes = {}

        self._loaded = True

    def save(self) -> None:
        """
        Persist catalog to disk using atomic write.

        Strategy: write to temp file in same directory, then os.replace().
        This ensures the catalog file is always complete — a crash during
        write leaves the old version intact.
        """
        os.makedirs(self._data_dir, exist_ok=True)
        data = {
            "magic": "MiniDB_Catalog",
            "format_version": CATALOG_FORMAT_VERSION,
            "schema_evolution": SCHEMA_EVOLUTION_NOTE,
            "tables": self._tables,
            "indexes": self._indexes,
        }

        # Atomic write: temp file + rename
        tmp_fd, tmp_path = tempfile.mkstemp(
            dir=self._data_dir, prefix="catalog_", suffix=".tmp"
        )
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
                f.flush()
                os.fsync(f.fileno())  # Force to disk
            os.replace(tmp_path, self._catalog_file)  # Atomic rename
        except Exception:
            # Clean up temp file on failure
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    def _ensure_loaded(self) -> None:
        if not self._loaded:
            self.load()

    # ─── Table Operations ───────────────────────────────────────────

    def create_table(self, table_name: str, schema: Schema,
                     file_name: Optional[str] = None) -> str:
        """
        Register a new table in the catalog.
        Returns the table file name (e.g., 'users.tbl').
        Raises ValueError if table already exists.

        Table name is normalized to lowercase and immutable after creation.
        """
        self._ensure_loaded()
        name_lower = table_name.lower()

        if name_lower in self._tables:
            raise ValueError(f"Table '{table_name}' already exists")

        if file_name is None:
            file_name = f"{name_lower}.tbl"

        self._tables[name_lower] = {
            "name": table_name,
            "file": file_name,
            "schema": schema.to_dict(),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "format_version": CATALOG_FORMAT_VERSION,
        }
        self.save()
        return file_name

    def drop_table(self, table_name: str) -> Optional[str]:
        """
        Remove a table from the catalog.
        Returns the table file name (caller should delete the file),
        or None if table doesn't exist.
        """
        self._ensure_loaded()
        name_lower = table_name.lower()

        entry = self._tables.pop(name_lower, None)
        if entry is None:
            return None

        # Remove associated indexes
        to_remove = [idx for idx, meta in self._indexes.items()
                     if meta.get("table", "").lower() == name_lower]
        for idx in to_remove:
            self._indexes.pop(idx)

        self.save()
        return entry["file"]

    def get_table(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get metadata for a table, or None if it doesn't exist."""
        self._ensure_loaded()
        return self._tables.get(table_name.lower())

    def get_table_schema(self, table_name: str) -> Optional[Schema]:
        """Get the schema for a table, or None if it doesn't exist."""
        entry = self.get_table(table_name)
        if entry is None:
            return None
        return Schema.from_dict(entry["schema"])

    def get_table_file(self, table_name: str) -> Optional[str]:
        """Get the absolute file path for a table's data file."""
        entry = self.get_table(table_name)
        if entry is None:
            return None
        return os.path.join(self._data_dir, entry["file"])

    def list_tables(self) -> List[str]:
        """List all table names (sorted, deterministic)."""
        self._ensure_loaded()
        return sorted(self._tables.keys())

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        self._ensure_loaded()
        return table_name.lower() in self._tables

    # ─── Index Operations ───────────────────────────────────────────

    def create_index(self, index_name: str, table_name: str,
                     column: str, index_type: str = "BTREE") -> str:
        """
        Register a new index in the catalog.
        Returns the index file name.
        """
        self._ensure_loaded()
        name_lower = index_name.lower()

        if name_lower in self._indexes:
            raise ValueError(f"Index '{index_name}' already exists")

        if not self.table_exists(table_name):
            raise ValueError(f"Table '{table_name}' does not exist")

        file_name = f"{name_lower}.idx"

        self._indexes[name_lower] = {
            "name": index_name,
            "table": table_name.lower(),
            "column": column,
            "file": file_name,
            "type": index_type,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self.save()
        return file_name

    def drop_index(self, index_name: str) -> Optional[str]:
        """Remove an index. Returns its file name, or None."""
        self._ensure_loaded()
        entry = self._indexes.pop(index_name.lower(), None)
        if entry is None:
            return None
        self.save()
        return entry["file"]

    def get_indexes_for_table(self, table_name: str) -> List[Dict[str, Any]]:
        """Get all indexes for a table."""
        self._ensure_loaded()
        name_lower = table_name.lower()
        return [meta for meta in self._indexes.values()
                if meta.get("table", "").lower() == name_lower]

    def get_index(self, index_name: str) -> Optional[Dict[str, Any]]:
        """Get metadata for an index, or None if it doesn't exist."""
        self._ensure_loaded()
        index_name = index_name.lower()
        if index_name in self._indexes:
            return {"name": index_name, **self._indexes[index_name]}
        return None

    def list_indexes(self) -> List[str]:
        """List all index names."""
        self._ensure_loaded()
        return sorted(self._indexes.keys())

    def __repr__(self) -> str:
        return (f"Catalog(tables={len(self._tables)}, "
                f"indexes={len(self._indexes)}, "
                f"dir='{self._data_dir}')")
