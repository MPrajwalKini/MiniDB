"""
MiniDB AST Nodes
================
Abstract Syntax Tree definitions for SQL statements and expressions.

Design:
- Immutable dataclasses (where possible)
- Strict separation between Statements and Expressions
- QualifiedName for table/column references (split by dot)
- GroupingExpr to preserve parentheses structure
- Source position tracking via optional Token reference
"""

from dataclasses import dataclass, field
from typing import List, Optional, Union, Any

from parser.tokenizer import Token, TokenType
from storage.types import DataType


class ASTNode:
    """Base class for all AST nodes."""
    pass


# ═══════════════════════════════════════════════════════════════════════════
# Expressions
# ═══════════════════════════════════════════════════════════════════════════

class Expression(ASTNode):
    """Base class for SQL expressions."""
    pass


@dataclass
class Literal(Expression):
    """Literal value (number, string, boolean, null)."""
    value: Any
    data_type: DataType

    def __repr__(self) -> str:
        return f"Literal({self.value!r}, {self.data_type.name})"

    def __str__(self) -> str:
        if self.value is None: return "NULL"
        if isinstance(self.value, str): return f"'{self.value}'"
        return str(self.value)


@dataclass
class QualifiedName(Expression):
    """
    Identifier with optional qualifications (e.g. table.column).
    parts=['column'] or parts=['table', 'column'] or parts=['schema', 'table', 'column']
    """
    parts: List[str]

    def __repr__(self) -> str:
        return f"QualifiedName({'.'.join(self.parts)})"

    def __str__(self) -> str:
        return ".".join(self.parts)


@dataclass
class BinaryExpr(Expression):
    """Binary operation: left op right (e.g. a + b, a = b)."""
    left: Expression
    op: TokenType
    right: Expression

    def __repr__(self) -> str:
        return f"BinaryExpr({self.left}, {self.op.name}, {self.right})"

    def __str__(self) -> str:
        # op.name is like PLUS, EQ. We need symbol.
        # Mapping? Or just use name for now?
        # Better to map.
        sym = {
            TokenType.PLUS: "+", TokenType.MINUS: "-", TokenType.STAR: "*", TokenType.SLASH: "/",
            TokenType.EQ: "=", TokenType.NEQ: "!=", TokenType.LT: "<", TokenType.GT: ">",
            TokenType.LTE: "<=", TokenType.GTE: ">=", TokenType.AND: "AND", TokenType.OR: "OR"
        }.get(self.op, self.op.name)
        return f"({self.left} {sym} {self.right})"


@dataclass
class UnaryExpr(Expression):
    """Unary operation: op operand (e.g. -a, NOT a)."""
    op: TokenType
    operand: Expression

    def __repr__(self) -> str:
        return f"UnaryExpr({self.op.name}, {self.operand})"

    def __str__(self) -> str:
        sym = {TokenType.MINUS: "-", TokenType.PLUS: "+", TokenType.NOT: "NOT"}.get(self.op, self.op.name)
        return f"{sym} {self.operand}"


@dataclass
class GroupingExpr(Expression):
    """Parenthesized expression: ( expr ). Preserves structure."""
    inner: Expression

    def __repr__(self) -> str:
        return f"GroupingExpr({self.inner})"

    def __str__(self) -> str:
        return f"({self.inner})"


@dataclass
class IsNullExpr(Expression):
    """IS NULL or IS NOT NULL check."""
    expr: Expression
    not_null: bool  # True if IS NOT NULL

    def __repr__(self) -> str:
        ops = "IS NOT NULL" if self.not_null else "IS NULL"
        return f"IsNullExpr({self.expr}, {ops})"

    def __str__(self) -> str:
        ops = "IS NOT NULL" if self.not_null else "IS NULL"
        return f"{self.expr} {ops}"


# ═══════════════════════════════════════════════════════════════════════════
# Support Structures
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class SelectItem(ASTNode):
    """Item in SELECT list: expression AS alias."""
    expr: Expression
    alias: Optional[str] = None

    def __repr__(self) -> str:
        if self.alias:
            return f"SelectItem({self.expr}, AS '{self.alias}')"
        return f"SelectItem({self.expr})"


@dataclass
class OrderItem(ASTNode):
    """Item in ORDER BY: expression ASC/DESC."""
    expr: Expression
    ascending: bool = True  # Default ASC

    def __repr__(self) -> str:
        direction = "ASC" if self.ascending else "DESC"
        return f"OrderItem({self.expr}, {direction})"


@dataclass
class ColumnDef(ASTNode):
    """Column definition in CREATE TABLE."""
    name: str
    data_type: DataType
    nullable: bool = True  # Default NULL allowed

    def __repr__(self) -> str:
        null_constr = "" if self.nullable else " NOT NULL"
        return f"ColumnDef({self.name} {self.data_type.name}{null_constr})"


@dataclass
class Assignment(ASTNode):
    """Assignment in UPDATE: column = value."""
    column: str
    value: Expression

    def __repr__(self) -> str:
        return f"Assignment({self.column} = {self.value})"


# ═══════════════════════════════════════════════════════════════════════════
# Statements
# ═══════════════════════════════════════════════════════════════════════════

class Statement(ASTNode):
    """Base class for SQL statements."""
    pass


@dataclass
class SelectStmt(Statement):
    """
    SELECT statement.
    """
    columns: List[SelectItem]
    from_table: Optional[QualifiedName] = None  # Optional for "SELECT 1"
    where: Optional[Expression] = None
    order_by: Optional[List[OrderItem]] = None
    limit: Optional[Expression] = None  # LIMIT can be an expression
    distinct: bool = False

    def __repr__(self) -> str:
        parts = [f"SELECT{' DISTINCT' if self.distinct else ''}"]
        parts.append(", ".join(map(str, self.columns)))
        if self.from_table:
            parts.append(f"FROM {self.from_table}")
        if self.where:
            parts.append(f"WHERE {self.where}")
        if self.order_by:
            parts.append(f"ORDER BY {', '.join(map(str, self.order_by))}")
        if self.limit:
            parts.append(f"LIMIT {self.limit}")
        return " ".join(parts)


@dataclass
class InsertStmt(Statement):
    """
    INSERT INTO table (col1, col2) VALUES (val1, val2).
    """
    table_name: str
    columns: Optional[List[str]]  # None means all columns implied
    values: List[Expression]

    def __repr__(self) -> str:
        cols = f"({', '.join(self.columns)})" if self.columns else ""
        vals = ", ".join(map(str, self.values))
        return f"INSERT INTO {self.table_name}{cols} VALUES ({vals})"


@dataclass
class UpdateStmt(Statement):
    """
    UPDATE table SET col=val WHERE ...
    """
    table_name: str
    assignments: List[Assignment]
    where: Optional[Expression] = None

    def __repr__(self) -> str:
        sets = ", ".join(map(str, self.assignments))
        stmt = f"UPDATE {self.table_name} SET {sets}"
        if self.where:
            stmt += f" WHERE {self.where}"
        return stmt


@dataclass
class DeleteStmt(Statement):
    """
    DELETE FROM table WHERE ...
    """
    table_name: str
    where: Optional[Expression] = None

    def __repr__(self) -> str:
        stmt = f"DELETE FROM {self.table_name}"
        if self.where:
            stmt += f" WHERE {self.where}"
        return stmt


@dataclass
class CreateTableStmt(Statement):
    """
    CREATE TABLE table (col type ...)
    """
    table_name: str
    columns: List[ColumnDef]
    if_not_exists: bool = False  # Future proofing

    def __repr__(self) -> str:
        cols = ", ".join(map(str, self.columns))
        return f"CREATE TABLE {self.table_name} ({cols})"


# ─── Transaction Control ──────────────────────────────────────────────────

@dataclass
class BeginStmt(Statement):
    """BEGIN [TRANSACTION] — start explicit transaction."""
    pass


@dataclass
class CommitStmt(Statement):
    """COMMIT — commit current transaction."""
    pass


@dataclass
class RollbackStmt(Statement):
    """ROLLBACK — abort current transaction."""
    pass


# ─── Query Analysis ───────────────────────────────────────────────────────

@dataclass
class ExplainStmt(Statement):
    """
    EXPLAIN [LOGICAL|PHYSICAL] <statement>
    Show query plan without executing.
    """
    inner: Statement
    level: str = "both"  # "logical", "physical", or "both"

    def __repr__(self) -> str:
        level_str = f" {self.level.upper()}" if self.level != "both" else ""
        return f"EXPLAIN{level_str} {self.inner}"

