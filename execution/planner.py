"""
MiniDB Physical Planner
=======================
Maps Logical Plan -> Physical Plan (Iterators).

Phase 5: Adds index selection — when a suitable B-Tree index exists
and the predicate is a simple column OP literal comparison, the planner
replaces SeqScan + Filter with IndexScan (+ residual filter if needed).
"""

from typing import Optional, List, Any, Tuple
import os

from planning.logical_plan import (
    LogicalNode, LogicalScan, LogicalFilter, LogicalProject, LogicalSort, 
    LogicalLimit, LogicalValues, LogicalInsert, LogicalUpdate, LogicalDelete, 
    LogicalCreate
)
from execution.physical_plan import (
    PhysicalNode, SeqScanExec, IndexScanExec, FilterExec, ProjectExec,
    SortExec, LimitExec, ValuesExec, InsertExec, UpdateExec, DeleteExec, DDLExec
)
from execution.context import ExecutionContext
from storage.table import TableFile
from storage.schema import Schema, Column
from parser.ast_nodes import (
    CreateTableStmt, Expression, BinaryExpr, QualifiedName, Literal,
    GroupingExpr
)
from parser.tokenizer import TokenType
from catalog.resolver import Resolver, ObjectNotFoundError
from catalog.system_catalog import SystemCatalog

# Comparison operators that can use an index
_INDEX_OPS = {TokenType.EQ, TokenType.LT, TokenType.GT, TokenType.LTE, TokenType.GTE}

# Inverse map for canonicalizing "literal OP column" → "column OP' literal"
_FLIP_OPS = {
    TokenType.LT: TokenType.GT,
    TokenType.GT: TokenType.LT,
    TokenType.LTE: TokenType.GTE,
    TokenType.GTE: TokenType.LTE,
    TokenType.EQ: TokenType.EQ,
}

class PhysicalPlanner:
    """
    Transforms Logical Plan tree into Physical Plan iterator tree.
    """
    def __init__(self, context: Any):
        self.context = context
        self.resolver = None
        if self.context.system_catalog:
             self.resolver = Resolver(self.context.system_catalog)
             if self.context.current_db:
                 self.resolver._db_cache[self.context.current_db.oid] = self.context.current_db

    def _resolve_table_info(self, table_name: str) -> Tuple[Optional[Schema], Optional[str]]:
        """Returns (Schema object, file_path_str) or (None, None)."""
        if self.resolver and self.context.current_db:
             try:
                 db, schema, table_info = self.resolver.resolve_table(
                     table_name, self.context.current_db.oid, self.context.search_path
                 )
                 # schema is Schema OBJECT, thus has absolute .schema_dir
                 schema_dir = schema.schema_dir
                 table_path = schema_dir / f"table_{table_info.oid}.tbl"
                 
                 # Get wrapper schema
                 tf = TableFile(str(table_path), self.context.buffer_manager)
                 if table_path.exists():
                     tf.open()
                     schema = tf.schema
                     tf.close()
                     return schema, str(table_path)
                 else:
                     return None, str(table_path)
             except (ObjectNotFoundError, FileNotFoundError):
                 return None, None
        else:
             # Legacy fallback
             schema = self.context.catalog.get_table_schema(table_name)
             if schema:
                  path = self.context.get_table_path(table_name)
                  return schema, path
             return None, None

    def plan(self, node: LogicalNode) -> PhysicalNode:
        """Create physical plan from logical node."""
        
        if isinstance(node, LogicalScan):
            return self._plan_scan(node)
        if isinstance(node, LogicalFilter):
            return self._plan_filter(node)
        if isinstance(node, LogicalProject):
            return self._plan_project(node)
        if isinstance(node, LogicalSort):
            return self._plan_sort(node)
        if isinstance(node, LogicalLimit):
            return self._plan_limit(node)
        if isinstance(node, LogicalValues):
            return self._plan_values(node)
        if isinstance(node, LogicalInsert):
            return self._plan_insert(node)
        if isinstance(node, LogicalUpdate):
            return self._plan_update(node)
        if isinstance(node, LogicalDelete):
            return self._plan_delete(node)
        if isinstance(node, LogicalCreate):
             return self._plan_create_table(node)
             
        raise NotImplementedError(f"Logical node type {type(node)} not supported")

    def _plan_scan(self, node: LogicalScan) -> PhysicalNode:
        schema, file_path = self._resolve_table_info(node.table_name)
        if not schema:
            raise RuntimeError(f"Table {node.table_name} not found")
            
        table = TableFile(file_path, self.context.buffer_manager)
        if os.path.exists(file_path):
             table.open()
        
        return SeqScanExec(table, schema, node.alias,
                           ctx=self.context, table_name=node.table_name)

    def _plan_filter(self, node: LogicalFilter) -> PhysicalNode:
        # Check if we can use an index scan instead of SeqScan + Filter
        if isinstance(node.child, LogicalScan):
            index_plan = self._try_index_scan(
                node.child, node.condition
            )
            if index_plan is not None:
                return index_plan

        # Fallback: standard filter
        return FilterExec(self.plan(node.child), node.condition)

    def _plan_project(self, node: LogicalProject) -> PhysicalNode:
        return ProjectExec(self.plan(node.child), node.expressions, node.aliases)

    def _plan_sort(self, node: LogicalSort) -> PhysicalNode:
        return SortExec(self.plan(node.child), node.order_by)
        
    def _plan_limit(self, node: LogicalLimit) -> PhysicalNode:
        return LimitExec(self.plan(node.child), node.limit_expr)

    def _plan_values(self, node: LogicalValues) -> PhysicalNode:
        return ValuesExec(node.rows, node.columns)

    def _plan_insert(self, node: LogicalInsert) -> PhysicalNode:
        schema, file_path = self._resolve_table_info(node.table_name)
        if not schema: raise RuntimeError(f"Table {node.table_name} not found")
        
        table = TableFile(file_path, self.context.buffer_manager)
        if os.path.exists(file_path):
             table.open()
             
        return InsertExec(table, self.plan(node.child), node.target_columns,
                          ctx=self.context, table_name=node.table_name)

    def _plan_update(self, node: LogicalUpdate) -> PhysicalNode:
        schema, file_path = self._resolve_table_info(node.table_name)
        if not schema: raise RuntimeError(f"Table {node.table_name} not found")
        
        table = TableFile(file_path, self.context.buffer_manager)
        if os.path.exists(file_path): table.open()
        
        return UpdateExec(table, self.plan(node.child), node.assignments,
                          ctx=self.context, table_name=node.table_name)

    def _plan_delete(self, node: LogicalDelete) -> PhysicalNode:
        schema, file_path = self._resolve_table_info(node.table_name)
        if not schema: raise RuntimeError(f"Table {node.table_name} not found")
        
        table = TableFile(file_path, self.context.buffer_manager)
        if os.path.exists(file_path): table.open()
        
        return DeleteExec(table, self.plan(node.child),
                          ctx=self.context, table_name=node.table_name)

    def _plan_create_table(self, node: LogicalCreate) -> PhysicalNode:
        cols = []
        for cdef in node.columns:
            cols.append(Column(cdef.name, cdef.data_type, cdef.nullable))
            
        schema = Schema(cols)
        
        target_schema_obj = None
        simple_table_name = node.table_name
        
        if self.context.system_catalog and self.context.current_db:
            # Resolve target schema
            parts = node.table_name.split(".")
            if len(parts) == 3:
                # db.schema.table - TODO: Support cross-DB create?
                # For now assume current DB
                if parts[0] != self.context.current_db.name:
                    raise RuntimeError("Cross-database CREATE TABLE not supported yet")
                schema_name = parts[1]
                simple_table_name = parts[2]
                target_schema_obj = self.context.current_db.get_schema(schema_name)
            elif len(parts) == 2:
                # schema.table
                schema_name = parts[0]
                simple_table_name = parts[1]
                target_schema_obj = self.context.current_db.get_schema(schema_name)
            else:
                # Unqualified -> use public
                # TODO: Use current_schema from context if available
                target_schema_obj = self.context.current_db.get_schema("public")
                
            if not target_schema_obj:
                raise RuntimeError(f"Target schema for '{node.table_name}' not found")

        # For legacy, target_schema_obj is None, DDLExec handles fallback
        
        path = "" # Not used in new path, resolved by DDLExec using OID
        
        # DDLExec(context, table_name, schema, target_catalog_schema)
        # Note: We pass simple_table_name!
        return DDLExec(self.context, simple_table_name, schema, target_schema_obj)

    # ─── Index Selection Logic ──────────────────────────────────────

    def _try_index_scan(self, scan_node: LogicalScan,
                        predicate: Expression) -> Optional[PhysicalNode]:
        """
        Attempt to use an index for a Filter(Scan(...), predicate).
        
        Returns an IndexScanExec if a suitable index exists, or None.
        The predicate is always kept as a residual filter on the IndexScanExec
        for correctness.
        """
        # Extract indexable predicate: column OP literal
        col_name, op, literal_val = self._extract_indexable_predicate(predicate)
        if col_name is None:
            return None

        table_name = scan_node.table_name

        # Check if an index exists on this column
        indexes = self.context.catalog.get_indexes_for_table(table_name)
        matching_index = None
        for idx in indexes:
            if idx["column"].lower() == col_name.lower():
                matching_index = idx
                break

        if matching_index is None:
            return None

        # We have a matching index — build IndexScanExec
        schema = self.context.catalog.get_table_schema(table_name)
        file_path = self.context.get_table_path(table_name)
        table = TableFile(file_path, self.context.buffer_manager)
        if os.path.exists(file_path):
            table.open()

        # Open the B-Tree
        idx_path = os.path.join(self.context.catalog.data_dir, matching_index["file"])
        from indexing.btree import BTree
        btree = BTree.open(idx_path, self.context.buffer_manager)

        # Determine key type from schema
        key_type = None
        for col in schema.columns:
            if col.name.lower() == col_name.lower():
                key_type = col.data_type
                break

        # Build scan parameters based on operator
        if op == TokenType.EQ:
            return IndexScanExec(
                table, schema, btree, key_type,
                scan_type='eq', eq_key=literal_val,
                residual_predicate=predicate,
                alias=scan_node.alias,
                ctx=self.context, table_name=table_name,
                index_name=matching_index["name"],
            )
        elif op == TokenType.GT:
            return IndexScanExec(
                table, schema, btree, key_type,
                scan_type='range',
                low_key=literal_val, low_inclusive=False,
                residual_predicate=predicate,
                alias=scan_node.alias,
                ctx=self.context, table_name=table_name,
                index_name=matching_index["name"],
            )
        elif op == TokenType.GTE:
            return IndexScanExec(
                table, schema, btree, key_type,
                scan_type='range',
                low_key=literal_val, low_inclusive=True,
                residual_predicate=predicate,
                alias=scan_node.alias,
                ctx=self.context, table_name=table_name,
                index_name=matching_index["name"],
            )
        elif op == TokenType.LT:
            return IndexScanExec(
                table, schema, btree, key_type,
                scan_type='range',
                high_key=literal_val, high_inclusive=False,
                residual_predicate=predicate,
                alias=scan_node.alias,
                ctx=self.context, table_name=table_name,
                index_name=matching_index["name"],
            )
        elif op == TokenType.LTE:
            return IndexScanExec(
                table, schema, btree, key_type,
                scan_type='range',
                high_key=literal_val, high_inclusive=True,
                residual_predicate=predicate,
                alias=scan_node.alias,
                ctx=self.context, table_name=table_name,
                index_name=matching_index["name"],
            )

        return None

    def _extract_indexable_predicate(self, expr: Expression):
        """
        Check if expression is a simple 'column OP literal' comparison.
        Handles canonicalization: '18 < age' → 'age > 18'.
        
        Returns (column_name, op, literal_value) or (None, None, None).
        """
        # Unwrap GroupingExpr
        while isinstance(expr, GroupingExpr):
            expr = expr.inner

        if not isinstance(expr, BinaryExpr):
            return None, None, None

        if expr.op not in _INDEX_OPS:
            return None, None, None

        left = expr.left
        right = expr.right

        # Unwrap grouping on sides
        while isinstance(left, GroupingExpr):
            left = left.inner
        while isinstance(right, GroupingExpr):
            right = right.inner

        # Case 1: column OP literal (canonical form)
        if isinstance(left, QualifiedName) and isinstance(right, Literal):
            col_name = left.parts[-1]  # Use last part (column name)
            return col_name, expr.op, right.value

        # Case 2: literal OP column (needs flip: 18 < age → age > 18)
        if isinstance(left, Literal) and isinstance(right, QualifiedName):
            col_name = right.parts[-1]
            flipped_op = _FLIP_OPS.get(expr.op)
            if flipped_op is not None:
                return col_name, flipped_op, left.value

        return None, None, None
