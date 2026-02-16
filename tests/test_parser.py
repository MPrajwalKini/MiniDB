"""
MiniDB Match-Driven Parser Tests
================================
Tests for pure SQL parser (SQL text -> AST).
Verifies correct structure, precedence, and error reporting.

Focus:
1. Valid SQL produces correct AST structure
2. Precedence rules (AND > OR, * > +)
3. Edge cases: NULL, literals, identifiers
4. Error reporting with line/column info
"""

import sys
import os
import pytest

# Ensure project root is on path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from parser import parse, ParseError
from parser.ast_nodes import (
    SelectStmt, InsertStmt, UpdateStmt, DeleteStmt, CreateTableStmt,
    BinaryExpr, UnaryExpr, Literal, QualifiedName, GroupingExpr, IsNullExpr,
    SelectItem, OrderItem, Assignment, ColumnDef
)
from parser.tokenizer import TokenType
from storage.types import DataType


class TestParser:
    
    # ─── SELECT ─────────────────────────────────────────────────────

    def test_select_basic(self):
        ast = parse("SELECT id, name FROM users")
        assert isinstance(ast, SelectStmt)
        assert len(ast.columns) == 2
        assert ast.columns[0].expr == QualifiedName(["id"])
        assert ast.columns[1].expr == QualifiedName(["name"])
        assert ast.from_table == QualifiedName(["users"])

    def test_select_star(self):
        ast = parse("SELECT * FROM users")
        assert len(ast.columns) == 1
        # Typically represented as QualifiedName(["*"]) or similar
        assert isinstance(ast.columns[0].expr, QualifiedName)
        assert ast.columns[0].expr.parts == ["*"]

    def test_select_no_from(self):
        """Test user request: SELECT without FROM (e.g. SELECT 1)"""
        ast = parse("SELECT 1")
        assert ast.from_table is None
        assert isinstance(ast.columns[0].expr, Literal)
        assert ast.columns[0].expr.value == 1

    def test_select_distinct(self):
        ast = parse("SELECT DISTINCT name FROM users")
        assert ast.distinct is True

    def test_select_limit(self):
        """Test LIMIT as expression"""
        ast = parse("SELECT * FROM t LIMIT 10")
        assert isinstance(ast.limit, Literal)
        assert ast.limit.value == 10
        
        ast2 = parse("SELECT * FROM t LIMIT 5 + 5")
        assert isinstance(ast2.limit, BinaryExpr)

    def test_select_order_by(self):
        ast = parse("SELECT * FROM t ORDER BY created_at DESC, name ASC")
        assert len(ast.order_by) == 2
        assert ast.order_by[0].ascending is False
        assert ast.order_by[1].ascending is True

    def test_select_where(self):
        ast = parse("SELECT * FROM t WHERE id = 5")
        assert isinstance(ast.where, BinaryExpr)
        assert ast.where.op == TokenType.EQ

    # ─── Operators & Precedence ─────────────────────────────────────

    def test_precedence_and_or(self):
        """AND binds tighter than OR"""
        # a OR b AND c  ->  a OR (b AND c)
        ast = parse("SELECT * FROM t WHERE a OR b AND c")
        where = ast.where
        assert isinstance(where, BinaryExpr)
        assert where.op == TokenType.OR
        assert isinstance(where.right, BinaryExpr)
        assert where.right.op == TokenType.AND

    def test_precedence_math(self):
        """* binds tighter than +"""
        # a + b * c  ->  a + (b * c)
        ast = parse("SELECT * FROM t WHERE a + b * c")
        expr = ast.where
        assert isinstance(expr, BinaryExpr)
        assert expr.op == TokenType.PLUS
        assert isinstance(expr.right, BinaryExpr)
        assert expr.right.op == TokenType.STAR

    def test_grouping_expr(self):
        """Parentheses grouping preserved"""
        # (a + b) * c
        ast = parse("SELECT * FROM t WHERE (a + b) * c")
        expr = ast.where
        assert isinstance(expr, BinaryExpr)
        assert expr.op == TokenType.STAR
        assert isinstance(expr.left, GroupingExpr)
        assert isinstance(expr.left.inner, BinaryExpr) # a + b inside

    def test_unary_precedence(self):
        """-a * b"""
        ast = parse("SELECT * FROM t WHERE -a * b")
        expr = ast.where
        assert isinstance(expr, BinaryExpr) # *
        assert isinstance(expr.left, UnaryExpr) # -a

    def test_is_null(self):
        """IS NULL and IS NOT NULL"""
        ast = parse("SELECT * FROM t WHERE a IS NULL")
        assert isinstance(ast.where, IsNullExpr)
        assert ast.where.not_null is False
        
        ast2 = parse("SELECT * FROM t WHERE a IS NOT NULL")
        assert isinstance(ast2.where, IsNullExpr)
        assert ast2.where.not_null is True

    # ─── INSERT ─────────────────────────────────────────────────────

    def test_insert_basic(self):
        ast = parse("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        assert isinstance(ast, InsertStmt)
        assert ast.table_name == "users"
        assert ast.columns == ["id", "name"]
        assert len(ast.values) == 2
        assert ast.values[0].value == 1

    def test_insert_all_columns(self):
        ast = parse("INSERT INTO users VALUES (1, 'Alice')")
        assert ast.columns is None # Implies all columns
        assert len(ast.values) == 2

    # ─── CREATE TABLE ───────────────────────────────────────────────

    def test_create_table(self):
        sql = "CREATE TABLE users (id INT NOT NULL, name VARCHAR(255), active BOOLEAN)"
        ast = parse(sql)
        assert isinstance(ast, CreateTableStmt)
        assert ast.table_name == "users"
        assert len(ast.columns) == 3
        
        c1 = ast.columns[0]
        assert c1.name == "id"
        assert c1.data_type == DataType.INT
        assert c1.nullable is False
        
        c2 = ast.columns[1]
        assert c2.name == "name"
        assert c2.data_type == DataType.STRING
        assert c2.nullable is True # Default

    # ─── UPDATE / DELETE ────────────────────────────────────────────

    def test_update(self):
        ast = parse("UPDATE users SET name = 'Bob', active = TRUE WHERE id = 1")
        assert isinstance(ast, UpdateStmt)
        assert ast.table_name == "users"
        assert len(ast.assignments) == 2
        assert ast.assignments[0].column == "name"
        assert isinstance(ast.where, BinaryExpr)

    def test_delete(self):
        ast = parse("DELETE FROM users WHERE id = 1")
        assert isinstance(ast, DeleteStmt)
        assert ast.table_name == "users"
        assert isinstance(ast.where, BinaryExpr)

    # ─── Identifiers & Literals ─────────────────────────────────────

    def test_quoted_identifier(self):
        ast = parse('SELECT "My Column" FROM "My Table"')
        assert ast.from_table.parts == ["My Table"]
        assert ast.columns[0].expr.parts == ["My Column"]

    def test_qualified_name(self):
        ast = parse("SELECT t.id, s.t.col FROM t")
        assert ast.columns[0].expr.parts == ["t", "id"]
        assert ast.columns[1].expr.parts == ["s", "t", "col"]

    def test_string_literal_escaped(self):
        ast = parse("SELECT 'O''Reilly'") 
        val = ast.columns[0].expr.value
        assert val == "O'Reilly"

    # ─── Errors ─────────────────────────────────────────────────────

    def test_error_missing_from(self):
        # SELECT * users  -> "Unexpected token after statement"
        # Since FROM is optional, "SELECT * users" is interpreted as "SELECT *" followed by "users"
        # Since parsing stops after the statement, "users" is unexpected trail.
        with pytest.raises(ParseError) as exc:
            parse("SELECT * users")
        assert "Unexpected token after statement" in str(exc.value)

    def test_error_unbalanced_paren(self):
        with pytest.raises(ParseError) as exc:
            parse("SELECT (1 + 2 FROM t") # missing )
        assert "Expected )" in str(exc.value)

    def test_error_unexpected_token(self):
        with pytest.raises(ParseError) as exc:
            parse("SELECT * FROM t WHERE") # EOF after WHERE
        # Expect expression
        assert "Unexpected end" in str(exc.value)

    def test_unexpected_token_after_statement(self):
         # Multiple statements not supported yet
        with pytest.raises(ParseError) as exc:
            parse("SELECT 1; SELECT 2")
        assert "Multiple statements" in str(exc.value)

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
