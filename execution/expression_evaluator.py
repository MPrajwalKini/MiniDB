"""
MiniDB Expression Evaluator
===========================
Runtime evaluation of AST expressions against a data row.

Features:
- Three-Valued Logic (TRUE, FALSE, UNKNOWN/None)
- Arithmetic with type promotion (INT+FLOAT -> FLOAT)
- Comparisons with NULL propagation
- Runtime error handling (Division by Zero)
"""

from typing import Any, Dict, Optional
import operator

from parser.ast_nodes import (
    Expression, Literal, QualifiedName, BinaryExpr, UnaryExpr, 
    GroupingExpr, IsNullExpr, Assignment
)
from parser.tokenizer import TokenType
from storage.types import DataType

# Type alias for Row Values
RowValues = Dict[str, Any]

class ExpressionEvaluator:
    """
    Evaluates AST expressions against a row.
    """

    def evaluate(self, expr: Expression, row: RowValues) -> Any:
        """
        Evaluate an expression against a row.
        Returns: Python value (int, float, str, bool, or None for NULL/UNKNOWN)
        """
        if isinstance(expr, Literal):
            return expr.value
            
        if isinstance(expr, QualifiedName):
            # Resolve column name.
            # Row keys expected to be simple names for now, or match 
            # the fully qualified name if planner bound them.
            # For Phase 4, we assume the Planner has ensured the row contains
            # the keys we are looking for, OR we look for exact match.
            # We'll try exact match first, then suffix match if ambiguous?
            # Contracts say: "Planner must assign names".
            # So we look up exactly.
            key = str(expr)
            if key in row:
                return row[key]
            # Try last part (column name) if full name not found (simple resolution)
            col_name = expr.parts[-1]
            if col_name in row:
                return row[col_name]
            
            # If still not found, check if it's a reference to a table alias?
            # For now, strict lookup.
            raise RuntimeError(f"Column '{key}' not found in row: {list(row.keys())}")

        if isinstance(expr, GroupingExpr):
            return self.evaluate(expr.inner, row)

        if isinstance(expr, IsNullExpr):
            val = self.evaluate(expr.expr, row)
            if expr.not_null:
                return val is not None
            return val is None

        if isinstance(expr, UnaryExpr):
            return self._eval_unary(expr, row)

        if isinstance(expr, BinaryExpr):
            return self._eval_binary(expr, row)

        raise NotImplementedError(f"Expression type {type(expr)} not supported")

    def _eval_unary(self, expr: UnaryExpr, row: RowValues) -> Any:
        val = self.evaluate(expr.operand, row)
        
        if expr.op == TokenType.NOT:
            # 3VL NOT:
            # NOT TRUE -> FALSE
            # NOT FALSE -> TRUE
            # NOT UNKNOWN -> UNKNOWN
            if val is None:
                return None
            return not bool(val)
            
        if expr.op == TokenType.MINUS:
            if val is None: return None
            return -val
            
        if expr.op == TokenType.PLUS:
            if val is None: return None
            return +val
            
        raise RuntimeError(f"Unknown unary operator {expr.op}")

    def _eval_binary(self, expr: BinaryExpr, row: RowValues) -> Any:
        # Short-circuit logic for AND/OR needs care with 3VL?
        # Standard Python `and`/`or` are strictly boolean.
        # We must implement 3VL tables manually.
        
        if expr.op == TokenType.AND:
            offset = 0 # Just for visual 
            # AND Truth Table:
            # T AND T = T
            # T AND F = F
            # T AND U = U
            # F AND _ = F  <-- Short circuit possible
            # U AND T = U
            # U AND F = F
            # U AND U = U
            
            left = self.evaluate(expr.left, row)
            if left is False:
                return False  # False AND anything is False
            
            right = self.evaluate(expr.right, row)
            
            if left is True and right is True: return True
            if left is True and right is False: return False
            if left is False: return False # Covered above
            
            # If any is U (None), result is U (unless one is False, covered above)
            return None

        if expr.op == TokenType.OR:
            # OR Truth Table:
            # T OR _ = T   <-- Short circuit possible
            # F OR T = T
            # F OR F = F
            # F OR U = U
            # U OR T = T
            # U OR F = U
            # U OR U = U
            
            left = self.evaluate(expr.left, row)
            if left is True:
                return True
                
            right = self.evaluate(expr.right, row)
            
            if left is True: return True # Covered
            if right is True: return True
            
            if left is False and right is False: return False
            
            return None

        # For other ops, evaluate both sides
        left = self.evaluate(expr.left, row)
        right = self.evaluate(expr.right, row)

        # Null propagation: If either is NULL, result is NULL (usually)
        # Exception: IS DISTINCT FROM (not impl yet)
        if left is None or right is None:
            return None

        # Arithmetic
        if expr.op == TokenType.PLUS: return left + right
        if expr.op == TokenType.MINUS: return left - right
        if expr.op == TokenType.STAR: return left * right
        if expr.op == TokenType.SLASH:
            if right == 0:
                raise RuntimeError("Division by zero")
            return left / right

        # Comparisons
        if expr.op == TokenType.EQ: return left == right
        if expr.op == TokenType.NEQ: return left != right
        if expr.op == TokenType.LT: return left < right
        if expr.op == TokenType.GT: return left > right
        if expr.op == TokenType.LTE: return left <= right
        if expr.op == TokenType.GTE: return left >= right

        raise RuntimeError(f"Unknown binary operator {expr.op}")
