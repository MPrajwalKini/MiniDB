"""
MiniDB Logical Planner
======================
Transforms AST into a Logical Plan.
Binds table names to schema and validates columns.
"""

from typing import Optional, List, Any

from parser.ast_nodes import (
    Statement, SelectStmt, InsertStmt, UpdateStmt, DeleteStmt, CreateTableStmt,
    Expression, Literal, QualifiedName, BinaryExpr, UnaryExpr
)
from planning.logical_plan import (
    LogicalNode, LogicalScan, LogicalFilter, LogicalProject, LogicalLimit,
    LogicalSort, LogicalValues, LogicalInsert, LogicalUpdate, LogicalDelete,
    LogicalCreate
)
from catalog.catalog import Catalog
from catalog.system_catalog import SystemCatalog
from catalog.database import Database
from catalog.resolver import Resolver, ObjectNotFoundError
from storage.types import DataType
from storage.table import TableFile
from typing import List, Optional

class Planner:
    """
    Converts AST to Logical Plan.
    """
    def __init__(self, catalog: Catalog, system_catalog: Optional[SystemCatalog] = None, 
                 current_db: Optional[Database] = None,
                 search_path: List[int] = None):
        self.catalog = catalog
        self.system_catalog = system_catalog
        self.current_db = current_db
        self.search_path = search_path or []
        
        self.resolver = None
        if self.system_catalog:
             self.resolver = Resolver(self.system_catalog)
             # Critical: Pre-populate cache with current_db to see uncommitted DDL changes
             if self.current_db:
                 self.resolver._db_cache[self.current_db.oid] = self.current_db

    def _get_table_schema(self, table_name: str):
        if self.system_catalog and self.current_db:
            try:
                db, schema, table_info = self.resolver.resolve_table(
                    table_name, self.current_db.oid, self.search_path
                )
                # Open table file to get schema
                # path = schema.schema_dir / f"table_{table_info.oid}.tbl"
                # But schema.schema_dir is strict Path?
                # Schema object (Catalog) has schema_dir property?
                # Resolving table returns (Database, Schema(Catalog), TableInfo)
                # Schema object has .schema_dir (absolute Path)
                schema_dir = schema.schema_dir
                table_path = schema_dir / f"table_{table_info.oid}.tbl"
                
                tf = TableFile(str(table_path), None) # No buffer manager needed for just reading header?
                tf.open()
                schema = tf.schema
                tf.close()
                return schema
            except (ObjectNotFoundError, FileNotFoundError):
                 return None
            except Exception as e:
                 # Fallback or error?
                 print(f"Planner warning: failed to resolve {table_name}: {e}")
                 return None
        else:
            # Fallback to legacy
            return self.catalog.get_table_schema(table_name)

    def plan(self, stmt: Statement) -> LogicalNode:
        """Create logical plan from statement."""
        if isinstance(stmt, SelectStmt):
            return self._plan_select(stmt)
        if isinstance(stmt, InsertStmt):
            return self._plan_insert(stmt)
        if isinstance(stmt, UpdateStmt):
            return self._plan_update(stmt)
        if isinstance(stmt, DeleteStmt):
            return self._plan_delete(stmt)
        if isinstance(stmt, CreateTableStmt):
            return self._plan_create_table(stmt)
        raise NotImplementedError(f"Statement type {type(stmt)} not supported")

    def _plan_select(self, stmt: SelectStmt) -> LogicalNode:
        # 1. Source (FROM or Virtual)
        if stmt.from_table:
            # Handle qualified names?
            # stmt.from_table is QualifiedName.
            # We assume .parts joined by . or handle parts.
            if isinstance(stmt.from_table, QualifiedName):
                 table_name = ".".join(stmt.from_table.parts)
            else:
                 table_name = stmt.from_table # Should be str?
            
            # Verify table exists
            if not self._get_table_schema(table_name):
                 raise RuntimeError(f"Table '{table_name}' does not exist")
            node = LogicalScan(table_name, alias=None)
        else:
            # SELECT without FROM -> Single interaction
            # LogicalValues with 1 row, 0 columns
            node = LogicalValues(rows=[[]], columns=[])

        # 2. Filter (WHERE)
        if stmt.where:
            # Bind/Validate expression columns here?
            # For Phase 4, we assume loose binding or simple validation.
            # Ideally verify cols exist in source.
            node = LogicalFilter(node, stmt.where)

        # 3. Project (SELECT list)
        # Transform SelectItems into Expressions and Aliases
        exprs = []
        aliases = []
        exprs = []
        aliases = []
        for i, item in enumerate(stmt.columns):
            if isinstance(item.expr, QualifiedName) and item.expr.parts == ["*"]:
                # Expand *
                if not stmt.from_table:
                     raise RuntimeError("SELECT * without FROM clause is not supported (or empty)")
                
                # Fetch schema cols
                # stmt.from_table is QualifiedName.
                if isinstance(stmt.from_table, QualifiedName):
                     tname = ".".join(stmt.from_table.parts)
                else:
                     tname = stmt.from_table
                     
                schema = self._get_table_schema(tname)
                if not schema: raise RuntimeError(f"Table {tname} not found")
                
                for col in schema.columns:
                    exprs.append(QualifiedName([col.name]))
                    aliases.append(col.name)
            else:
                exprs.append(item.expr)
                if item.alias:
                    aliases.append(item.alias)
                elif isinstance(item.expr, QualifiedName):
                    aliases.append(item.expr.parts[-1])
                else:
                    # Fallback alias: Expression text
                    aliases.append(str(item.expr)) 

        node = LogicalProject(node, exprs, aliases)

        # 4. Sort (ORDER BY)
        if stmt.order_by:
            node = LogicalSort(node, stmt.order_by)

        # 5. Limit
        if stmt.limit:
            node = LogicalLimit(node, stmt.limit)

        return node

    def _plan_insert(self, stmt: InsertStmt) -> LogicalNode:
        # Validate table
        schema = self._get_table_schema(stmt.table_name)
        if not schema:
            raise RuntimeError(f"Table '{stmt.table_name}' does not exist")

        # Values -> LogicalValues
        # If stmt.columns is None, use schema columns in order
        target_cols = stmt.columns
        if not target_cols:
             target_cols = [c.name for c in schema.columns]
             
        # Validate value count? 
        if len(stmt.values) != len(target_cols):
             raise RuntimeError(f"INSERT values count ({len(stmt.values)}) does not match columns ({len(target_cols)})")

        node = LogicalValues(rows=[stmt.values], columns=target_cols)
        
        return LogicalInsert(stmt.table_name, node, stmt.columns)

    def _plan_update(self, stmt: UpdateStmt) -> LogicalNode:
        if not self._get_table_schema(stmt.table_name):
            raise RuntimeError(f"Table '{stmt.table_name}' does not exist")
            
        node = LogicalScan(stmt.table_name)
        if stmt.where:
            node = LogicalFilter(node, stmt.where)
            
        return LogicalUpdate(stmt.table_name, node, stmt.assignments)

    def _plan_delete(self, stmt: DeleteStmt) -> LogicalNode:
        if not self._get_table_schema(stmt.table_name):
            raise RuntimeError(f"Table '{stmt.table_name}' does not exist")
            
        node = LogicalScan(stmt.table_name)
        if stmt.where:
            node = LogicalFilter(node, stmt.where)
            
        return LogicalDelete(stmt.table_name, node)

    def _plan_create_table(self, stmt: CreateTableStmt) -> LogicalNode:
        if self._get_table_schema(stmt.table_name) and not stmt.if_not_exists:
             raise RuntimeError(f"Table '{stmt.table_name}' already exists")
        return LogicalCreate(stmt.table_name, stmt.columns, stmt.if_not_exists)
