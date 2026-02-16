"""
MiniDB Physical Plan Operators
==============================
Volcano Iterator Model implementation.
Nodes implement open(), next(), close().
"""

from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any, Iterator
import heapq

from storage.table import TableFile
from storage.page import RID
from storage.types import DataType
from storage.serializer import serialize_row
from storage.schema import Schema, Column
from parser.ast_nodes import Expression, OrderItem, Assignment
from execution.expression_evaluator import ExpressionEvaluator, RowValues

# Row Structure Contract
# values: dict[col_name -> value]
# rid: RID usually, None for synthetic rows
class ExecutionRow:
    __slots__ = ('values', 'rid')
    def __init__(self, values: RowValues, rid: Optional[RID] = None):
        self.values = values
        self.rid = rid
    
    def __repr__(self):
        return f"Row(rid={self.rid}, values={self.values})"

class PhysicalNode(ABC):
    """Base class for execution operators."""
    
    def __init__(self):
        self._open = False

    @abstractmethod
    def open(self):
        """Initialize the operator state."""
        self._open = True

    @abstractmethod
    def next(self) -> Optional[ExecutionRow]:
        """Return the next row or None if exhausted."""
        pass

    @abstractmethod
    def close(self):
        """Clean up resources."""
        self._open = False

    def children(self) -> List['PhysicalNode']:
        return []

def _acquire_lock(ctx, table_name: str, lock_type):
    """Helper: acquire a table lock if concurrency is enabled."""
    if ctx and ctx.lock_manager and ctx.active_txn_id:
        from concurrency.lock_manager import table_resource, LockResult
        result = ctx.lock_manager.acquire(
            ctx.active_txn_id, table_resource(table_name), lock_type)
        if result == LockResult.DEADLOCK:
            raise RuntimeError(
                f"Deadlock detected: txn {ctx.active_txn_id} on {table_name}")
        if result == LockResult.TIMEOUT:
            raise RuntimeError(
                f"Lock timeout: txn {ctx.active_txn_id} on {table_name}")
        if result == LockResult.ABORTED:
            raise RuntimeError(
                f"Transaction {ctx.active_txn_id} aborted (deadlock victim)")

class SeqScanExec(PhysicalNode):
    """Sequential scan of a table."""
    def __init__(self, table: TableFile, schema: Schema, alias: Optional[str] = None,
                 ctx=None, table_name: str = ""):
        super().__init__()
        self.table = table
        self.schema = schema
        self.alias = alias
        self._ctx = ctx
        self._table_name = table_name
        self._iterator: Optional[Iterator[tuple[RID, List[Any]]]] = None
        
        # Pre-compute column names for performance
        self._col_names = [col.name for col in schema.columns]

    def open(self):
        super().open()
        # Acquire SHARED table lock before reading
        if self._ctx and self._table_name:
            from concurrency.lock_manager import LockType
            _acquire_lock(self._ctx, self._table_name, LockType.SHARED)
        self._iterator = self.table.scan()

    def next(self) -> Optional[ExecutionRow]:
        try:
            rid, row_tuple = next(self._iterator)
            # Map tuple to dict using schema
            # Handle aliases if needed? 
            # If alias is present, do we rename columns to "alias.col"?
            # Or just "col"? 
            # Standard SQL: "alias.col" is available, "col" might be ambiguous.
            # For Phase 4, let's keep simple names.
            # But if alias is set, maybe prefix?
            # Planner binds names. Evaluator looks up names.
            # If Planner expects "t.id", we need "t.id" in dict.
            # Let's support simple names for now.
            values = dict(zip(self._col_names, row_tuple))
            return ExecutionRow(values, rid)
        except StopIteration:
            return None

    def close(self):
        super().close()
        self._iterator = None


class IndexScanExec(PhysicalNode):
    """
    Index scan using a B-Tree index.
    Fetches candidate RIDs from the index, then looks up full rows
    from the table by RID. Always applies a residual predicate filter
    for correctness (index gives candidates, not guaranteed matches).
    """
    def __init__(self, table: TableFile, schema: Schema,
                 btree, key_type: DataType,
                 scan_type: str,  # 'eq', 'range'
                 eq_key=None,
                 low_key=None, high_key=None,
                 low_inclusive: bool = True, high_inclusive: bool = True,
                 residual_predicate: Optional[Expression] = None,
                 alias: Optional[str] = None,
                 ctx=None, table_name: str = "", index_name: str = ""):
        super().__init__()
        self.table = table
        self.schema = schema
        self.btree = btree
        self.key_type = key_type
        self.scan_type = scan_type
        self.eq_key = eq_key
        self.low_key = low_key
        self.high_key = high_key
        self.low_inclusive = low_inclusive
        self.high_inclusive = high_inclusive
        self._ctx = ctx
        self._table_name = table_name
        self.index_name = index_name
        self.residual_predicate = residual_predicate
        self.alias = alias
        self._rid_iter = None
        self._col_names = [col.name for col in schema.columns]
        self._evaluator = ExpressionEvaluator()

    def open(self):
        super().open()
        # Acquire SHARED table lock before reading via index
        if self._ctx and self._table_name:
            from concurrency.lock_manager import LockType
            _acquire_lock(self._ctx, self._table_name, LockType.SHARED)
        if self.scan_type == 'eq':
            # Exact match: get list of RIDs
            rids = self.btree.search(self.eq_key)
            self._rid_iter = iter(rids)
        else:
            # Range scan: get iterator of (key, RID)
            self._rid_iter = (
                rid for _, rid in self.btree.range_scan(
                    low=self.low_key, high=self.high_key,
                    low_inclusive=self.low_inclusive,
                    high_inclusive=self.high_inclusive,
                )
            )

    def next(self) -> Optional['ExecutionRow']:
        while True:
            try:
                rid = next(self._rid_iter)
            except StopIteration:
                return None

            # RID lookup
            row_values = self.table.get_row(rid)
            if row_values is None:
                continue  # Deleted row, skip

            values = dict(zip(self._col_names, row_values))
            row = ExecutionRow(values, rid)

            # Apply residual predicate if present
            if self.residual_predicate is not None:
                result = self._evaluator.evaluate(self.residual_predicate, row.values)
                if result is not True:
                    continue

            return row

    def close(self):
        super().close()
        self._rid_iter = None


class FilterExec(PhysicalNode):
    """Filters rows based on a predicate."""
    def __init__(self, child: PhysicalNode, predicate: Expression):
        super().__init__()
        self.child = child
        self.predicate = predicate
        self.evaluator = ExpressionEvaluator()

    def open(self):
        super().open()
        self.child.open()

    def next(self) -> Optional[ExecutionRow]:
        while True:
            row = self.child.next()
            if row is None:
                return None
            
            # 3VL: Only TRUE passes
            res = self.evaluator.evaluate(self.predicate, row.values)
            if res is True:
                return row
            # Discard False/Unknown

    def close(self):
        super().close()
        self.child.close()
    
    def children(self): return [self.child]

class ProjectExec(PhysicalNode):
    """Projects expressions to new rows."""
    def __init__(self, child: PhysicalNode, exprs: List[Expression], aliases: List[str]):
        super().__init__()
        self.child = child
        self.exprs = exprs
        self.aliases = aliases
        self.evaluator = ExpressionEvaluator()

    def open(self):
        super().open()
        self.child.open()

    def next(self) -> Optional[ExecutionRow]:
        row = self.child.next()
        if row is None:
            return None
        
        new_values = {}
        for expr, alias in zip(self.exprs, self.aliases):
            val = self.evaluator.evaluate(expr, row.values)
            new_values[alias] = val
        
        # Propagate RID
        return ExecutionRow(new_values, row.rid)

    def close(self):
        super().close()
        self.child.close()
        
    def children(self): return [self.child]

class LimitExec(PhysicalNode):
    """Limits the number of output rows."""
    def __init__(self, child: PhysicalNode, limit_expr: Expression):
        super().__init__()
        self.child = child
        self.limit_expr = limit_expr
        self.evaluator = ExpressionEvaluator()
        self._count = 0
        self._limit = 0

    def open(self):
        super().open()
        self.child.open()
        # Evaluate limit integer (constant expected mostly, strictly 1 row 0 cols dependency?)
        # Limit expression should be evaluated once?
        # Standard SQL: Evaluated once at start.
        # But against what row? 
        # Usually it's a constant or parameter, not row-dependent.
        # We pass empty row.
        val = self.evaluator.evaluate(self.limit_expr, {})
        if not isinstance(val, int):
            raise RuntimeError("LIMIT must evaluate to an integer")
        self._limit = max(0, val)
        self._count = 0

    def next(self) -> Optional[ExecutionRow]:
        if self._count >= self._limit:
            return None
        
        row = self.child.next()
        if row is None:
            return None
            
        self._count += 1
        return row

    def close(self):
        super().close()
        self.child.close()
        
    def children(self): return [self.child]

class SortExec(PhysicalNode):
    """Materializes all rows, sorts them, and yields."""
    def __init__(self, child: PhysicalNode, order_by: List[OrderItem]):
        super().__init__()
        self.child = child
        self.order_by = order_by
        self.evaluator = ExpressionEvaluator()
        self._rows: List[ExecutionRow] = []
        self._iter_rows: Optional[Iterator[ExecutionRow]] = None

    def open(self):
        super().open()
        self.child.open()
        # Materialize
        self._rows = []
        while True:
            row = self.child.next()
            if row is None: break
            self._rows.append(row)
        
        # Sort
        # Complex sort key: list of (value, asc/desc)
        # Python's sort is stable.
        # We can implement multi-key sort using tuple comparison.
        # BUT NULL handling (NULLS FIRST/LAST).
        # Python `None` comparison fails in py3.
        # Key function wrapper needed.
        
        def sort_key(row: ExecutionRow):
            key = []
            for item in self.order_by:
                val = self.evaluator.evaluate(item.expr, row.values)
                # Contract: ASC -> NULLS LAST, DESC -> NULLS FIRST
                # Map value to something camparable.
                # (is_null, value) tuples?
                # Boolean False < True.
                
                # If ASC:
                # NULLS LAST: val, IsNull=1 (so comes after 0) -> (1, None) vs (0, val)
                # But None is not comparable with val.
                # So we need (is_null, val)
                
                # If DESC:
                # NULLS FIRST: val, IsNull=1 (so comes "larger" -> first?)
                # Wait, DESC means reverse order.
                # Python sort `reverse=True` reverses the result of comparison.
                # If we use `key` function, python sorts by key ASC. then reverses if reverse=True.
                # So for DESC, we want key such that NULLs are "Large" (so they come last in ASC sort, then first in DESC reverse?)
                # Wait. `reverse=True` flips everything.
                
                # Better: Sort one by one stable sort in reverse order of items?
                # Or construct a composite key that works with single sort pass?
                # Since we have mixed ASC/DESC, single pass with `reverse` is hard.
                # We sort by primary key last? No, Python sort is stable. Sort by least significant key first.
                pass
            return tuple(key) # This is hard with mixed direction.

        # Strategy: Sort by each key in reverse order (least significant first).
        for item in reversed(self.order_by):
            ascending = item.ascending
            
            # Key func for this item
            def item_key(row):
                val = self.evaluator.evaluate(item.expr, row.values)
                # Handle NULLs
                if val is None:
                    # ASC: NULLS LAST -> treated as Max?
                    # DESC: NULLS FIRST -> treated as Max?
                    # Wait.
                    # ASC: 1, 2, NULL. (NULL > 2)
                    # DESC: NULL, 2, 1. (NULL > 2)
                    # So NULL is simply "Maximum" value in both cases if logic is consistent?
                    # Let's say NULL is Inf.
                    # Then ASC: 1, 2, Inf. Correct.
                    # DESC (reverse=True): Inf, 2, 1. Correct.
                    # So we just need a key where NULL > any value.
                    return (True, val) # True > False. So (True, None) > (False, x).
                    # Wait, (True, val) vs (True, val2)? val is None.
                    # So (True, None) vs (False, 5). True > False. Correct.
                    # But Python (True, None) > (False, 5) compares True/False first.
                    # What if val is not None? (False, 5).
                    # Compare (False, 1) and (False, 2). 1 < 2.
                else:
                    return (False, val)

            self._rows.sort(key=item_key, reverse=(not ascending))
            
        self._iter_rows = iter(self._rows)

    def next(self) -> Optional[ExecutionRow]:
        return next(self._iter_rows, None)

    def close(self):
        super().close()
        self.child.close()
        self._rows = []
        self._iter_rows = None
        
    def children(self): return [self.child]

class ValuesExec(PhysicalNode):
    """Produces constant rows."""
    def __init__(self, rows: List[List[Expression]], columns: List[str]):
        super().__init__()
        self.rows_exprs = rows
        self.columns = columns
        self.evaluator = ExpressionEvaluator()
        self._iter: Optional[Iterator[List[Expression]]] = None

    def open(self):
        super().open()
        self._iter = iter(self.rows_exprs)

    def next(self) -> Optional[ExecutionRow]:
        val_exprs = next(self._iter, None)
        if val_exprs is None:
            return None
        
        # Evaluate expressions (constants usually)
        values = {}
        for col, expr in zip(self.columns, val_exprs):
            values[col] = self.evaluator.evaluate(expr, {})
            
        return ExecutionRow(values, rid=None)

    def close(self):
        super().close()
        self._iter = None

class InsertExec(PhysicalNode):
    """Inserts rows into table with optional WAL logging."""
    def __init__(self, table: TableFile, child: PhysicalNode,
                 target_columns: Optional[List[str]] = None, ctx=None,
                 table_name: str = ""):
        super().__init__()
        self.table = table
        self.child = child
        self.target_columns = target_columns
        self._ctx = ctx
        self._table_name = table_name
        self._processed = False
        self._schema_cols = [c.name for c in table.schema.columns]

    def open(self):
        super().open()
        # Acquire EXCLUSIVE table lock before writing
        if self._ctx and self._table_name:
            from concurrency.lock_manager import LockType
            _acquire_lock(self._ctx, self._table_name, LockType.EXCLUSIVE)
        self.child.open()
        self._processed = False

    def next(self) -> Optional[ExecutionRow]:
        if self._processed:
            return None

        count = 0
        while True:
            row = self.child.next()
            if row is None:
                break

            row_tuple = []
            for col in self._schema_cols:
                row_tuple.append(row.values.get(col, None))

            # Insert row → get RID
            rid = self.table.insert_row(row_tuple)

            # WAL logging (if transactions enabled)
            if self._ctx and self._ctx.txn_manager and self._ctx.active_txn_id:
                tuple_data = serialize_row(row_tuple, self.table.schema)
                lsn = self._ctx.txn_manager.log_insert(
                    self._ctx.active_txn_id, self._table_name,
                    rid.page_id, rid.slot_id, tuple_data)
                # Update page LSN
                page = self._ctx.buffer_manager.get_page(
                    self._ctx.get_table_path(self._table_name), rid.page_id)
                if page:
                    page.page_lsn = lsn

            count += 1

        self._processed = True
        return None

    def close(self):
        super().close()
        self.child.close()

    def children(self): return [self.child]

class UpdateExec(PhysicalNode):
    """Update rows with optional WAL logging. Two-pass: Scan → Collect → Update."""
    def __init__(self, table: TableFile, child: PhysicalNode,
                 assignments: List[Assignment], ctx=None,
                 table_name: str = ""):
        super().__init__()
        self.table = table
        self.child = child
        self.assignments = assignments
        self.evaluator = ExpressionEvaluator()
        self._ctx = ctx
        self._table_name = table_name
        self._processed = False

    def open(self):
        super().open()
        # Acquire EXCLUSIVE table lock before writing
        if self._ctx and self._table_name:
            from concurrency.lock_manager import LockType
            _acquire_lock(self._ctx, self._table_name, LockType.EXCLUSIVE)
        self.child.open()
        self._processed = False

    def next(self) -> Optional[ExecutionRow]:
        if self._processed:
            return None

        candidates = []
        while True:
            row = self.child.next()
            if row is None:
                break
            if row.rid is None:
                raise RuntimeError("Cannot update row without RID")
            candidates.append(row)

        schema_cols = [c.name for c in self.table.schema.columns]

        for row in candidates:
            new_vals = row.values.copy()
            for asn in self.assignments:
                val = self.evaluator.evaluate(asn.value, row.values)
                new_vals[asn.column] = val

            # Serialize old and new tuples
            old_tuple = [row.values.get(c, None) for c in schema_cols]
            new_tuple = [new_vals.get(c, None) for c in schema_cols]

            # WAL logging before apply
            if self._ctx and self._ctx.txn_manager and self._ctx.active_txn_id:
                old_data = serialize_row(old_tuple, self.table.schema)
                new_data = serialize_row(new_tuple, self.table.schema)
                lsn = self._ctx.txn_manager.log_update(
                    self._ctx.active_txn_id, self._table_name,
                    row.rid.page_id, row.rid.slot_id, old_data, new_data)
                self.table.update_row(row.rid, new_tuple)
                page = self._ctx.buffer_manager.get_page(
                    self._ctx.get_table_path(self._table_name), row.rid.page_id)
                if page:
                    page.page_lsn = lsn
            else:
                self.table.update_row(row.rid, new_tuple)

        self._processed = True
        return None

    def close(self):
        super().close()
        self.child.close()

    def children(self): return [self.child]

class DeleteExec(PhysicalNode):
    """Delete rows with optional WAL logging. Two-pass."""
    def __init__(self, table: TableFile, child: PhysicalNode,
                 ctx=None, table_name: str = ""):
        super().__init__()
        self.table = table
        self.child = child
        self._ctx = ctx
        self._table_name = table_name
        self._processed = False

    def open(self):
        super().open()
        # Acquire EXCLUSIVE table lock before writing
        if self._ctx and self._table_name:
            from concurrency.lock_manager import LockType
            _acquire_lock(self._ctx, self._table_name, LockType.EXCLUSIVE)
        self.child.open()
        self._processed = False

    def next(self) -> Optional[ExecutionRow]:
        if self._processed:
            return None

        rows_to_delete = []
        while True:
            row = self.child.next()
            if row is None:
                break
            if row.rid is None:
                raise RuntimeError("Cannot delete row without RID")
            rows_to_delete.append(row)

        for row in rows_to_delete:
            rid = row.rid

            # WAL logging: capture tuple data before delete for undo
            if self._ctx and self._ctx.txn_manager and self._ctx.active_txn_id:
                # Get tuple bytes from page for WAL before-image
                page = self._ctx.buffer_manager.get_page(
                    self._ctx.get_table_path(self._table_name), rid.page_id)
                tuple_data = page.get_tuple(rid.slot_id) if page else b""
                if tuple_data is None:
                    tuple_data = b""

                lsn = self._ctx.txn_manager.log_delete(
                    self._ctx.active_txn_id, self._table_name,
                    rid.page_id, rid.slot_id, tuple_data)
                self.table.delete_row(rid)
                if page:
                    page.page_lsn = lsn
            else:
                self.table.delete_row(rid)

        self._processed = True
        return None

    def close(self):
        super().close()
        self.child.close()

    def children(self): return [self.child]

class DDLExec(PhysicalNode):
    """Executes DDL statements like CREATE TABLE."""
    def __init__(self, catalog: Any, table_name: str, schema: Schema, file_path: str, buffer_manager: Any):
        super().__init__()
        self.catalog = catalog
        self.table_name = table_name
        self.schema = schema
        self.file_path = file_path
        self.buffer_manager = buffer_manager
        self._executed = False

    def open(self):
        super().open()
        self._executed = False

    def next(self) -> Optional[ExecutionRow]:
        if self._executed: return None
        
        # 1. Register in Catalog
        self.catalog.create_table(self.table_name, self.schema, self.file_path)
        
        # 2. Initialize TableFile
        tf = TableFile(self.file_path, self.buffer_manager)
        tf.create(self.table_name, self.schema)
        
        self._executed = True
        return None

    def close(self):
        super().close()
