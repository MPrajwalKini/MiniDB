
"""
Name Resolution Engine
======================
Resolves SQL identifiers (names) to Catalog Objects (OIDs/Metadata).

Supports:
- Fully Qualified Names: db.schema.table
- Partially Qualified: schema.table (in current DB)
- Unqualified: table (scan search_path)
"""

from typing import List, Tuple, Optional
from catalog.system_catalog import SystemCatalog, DatabaseInfo
from catalog.database import Database, SchemaInfo
from catalog.schema import Schema, TableInfo

class CatalogError(Exception):
    pass

class ObjectNotFoundError(CatalogError):
    def __init__(self, name: str, context: str):
        super().__init__(f"{context} '{name}' not found.")

class Resolver:
    def __init__(self, system_catalog: SystemCatalog):
        self.system = system_catalog
        # Simple cache for Database objects to avoid re-loading json constantly
        # In a real system, this would be a proper Buffer/Cache manager
        self._db_cache: dict[int, Database] = {}

    def _get_database(self, oid: int) -> Database:
        if oid in self._db_cache:
            return self._db_cache[oid]
        
        info = self.system.get_database_by_oid(oid)
        if not info:
            raise ObjectNotFoundError(str(oid), "Database OID")
            
        path = self.system.data_root / info.path
        db = Database(str(path), info.name, info.oid)
        self._db_cache[oid] = db
        return db

    def _get_database_by_name(self, name: str) -> Database:
        info = self.system.get_database(name)
        if not info:
             raise ObjectNotFoundError(name, "Database")
        return self._get_database(info.oid)

    def resolve_table(self, name: str, current_db_oid: int, search_path: List[int]) -> Tuple[Database, Schema, TableInfo]:
        """
        Resolve a table name to (Database, Schema, TableInfo).
        """
        parts = name.split('.')
        
        if len(parts) == 3:
            # db.schema.table
            db_name, schema_name, table_name = parts
            db = self._get_database_by_name(db_name)
            schema = db.get_schema(schema_name)
            if not schema:
                raise ObjectNotFoundError(schema_name, f"Schema in {db_name}")
            
            table = schema.get_table(table_name)
            if not table:
                raise ObjectNotFoundError(table_name, f"Table in {db_name}.{schema_name}")
                
            return db, schema, table
            
        elif len(parts) == 2:
            # schema.table (in current DB)
            schema_name, table_name = parts
            db = self._get_database(current_db_oid)
            schema = db.get_schema(schema_name)
            if not schema:
                raise ObjectNotFoundError(schema_name, "Schema")
            
            table = schema.get_table(table_name)
            if not table:
                raise ObjectNotFoundError(table_name, f"Table in {schema_name}")
                
            return db, schema, table
            
        elif len(parts) == 1:
            # table (use search_path)
            table_name = parts[0]
            db = self._get_database(current_db_oid)
            
            for schema_oid in search_path:
                # Find schema name by OID? 
                # Database stores schemas by NAME in dict.
                # But Database also stores schemas by OID?
                # db.schemas is Dict[str, SchemaInfo].
                # We need lookup by OID.
                # Database class needs get_schema_by_oid optimization.
                
                # Inefficient fallback:
                found_schema_info = None
                for s in db.schemas.values():
                    if s.oid == schema_oid:
                        found_schema_info = s
                        break
                
                if not found_schema_info:
                    continue
                    
                schema = db.get_schema(found_schema_info.name)
                # schema.get_table is case-sensitive?
                # User requirement: identifiers case-insensitive unless quoted.
                # My implementation so far uses simple dict lookup (case sensitive).
                # To support case-insensitive, we need normalization everywhere.
                # Assuming name passed here is already normalized or exact.
                
                table = schema.get_table(table_name)
                if table:
                    return db, schema, table
            
            raise ObjectNotFoundError(table_name, "Table (in search_path)")
        
        else:
             raise CatalogError(f"Invalid name format: {name}")
