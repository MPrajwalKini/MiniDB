"""
MiniDB Logical Plan Nodes
=========================
Data-agnostic representation of operations.
Immutable (dataclasses).
"""

from dataclasses import dataclass
from typing import List, Optional, Any, Dict
from parser.ast_nodes import Expression, OrderItem, ColumnDef, Assignment

@dataclass
class LogicalNode:
    """Base class for logical plan nodes."""
    def children(self) -> List['LogicalNode']:
        return []

@dataclass
class LogicalScan(LogicalNode):
    """Scan a table."""
    table_name: str
    alias: Optional[str] = None

@dataclass
class LogicalFilter(LogicalNode):
    """Filter rows by predicate."""
    child: LogicalNode
    condition: Expression

    def children(self): return [self.child]

@dataclass
class LogicalProject(LogicalNode):
    """Project expressions to output columns."""
    child: LogicalNode
    expressions: List[Expression]
    aliases: List[str]  # Output column names

    def children(self): return [self.child]

@dataclass
class LogicalLimit(LogicalNode):
    """Limit number of output rows."""
    child: LogicalNode
    limit_expr: Expression

    def children(self): return [self.child]

@dataclass
class LogicalSort(LogicalNode):
    """Sort rows."""
    child: LogicalNode
    order_by: List[OrderItem]

    def children(self): return [self.child]

@dataclass
class LogicalValues(LogicalNode):
    """
    Produce constant rows.
    Used for `SELECT` without `FROM`, and `INSERT VALUES`.
    rows: list of expressions (expressions must be constant-evaluable usually, 
          but logical plan just holds them).
    columns: output column names.
    """
    rows: List[List[Expression]]
    columns: List[str]

@dataclass
class LogicalInsert(LogicalNode):
    """
    Insert rows from child into table.
    """
    table_name: str
    child: LogicalNode
    target_columns: Optional[List[str]] = None

    def children(self): return [self.child]

@dataclass
class LogicalUpdate(LogicalNode):
    """
    Update rows from child in table.
    """
    table_name: str
    child: LogicalNode
    assignments: List[Assignment]

    def children(self): return [self.child]

@dataclass
class LogicalDelete(LogicalNode):
    """
    Delete rows from child in table.
    """
    table_name: str
    child: LogicalNode

    def children(self): return [self.child]

@dataclass
class LogicalCreate(LogicalNode):
    """Create a table."""
    table_name: str
    columns: List[ColumnDef]
    if_not_exists: bool = False
