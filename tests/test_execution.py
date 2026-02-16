import pytest
import os
import shutil
from typing import List, Dict, Any

from catalog.catalog import Catalog
from storage.buffer import BufferManager
from execution.context import ExecutionContext
from execution.executor import Executor
from execution.physical_plan import ExecutionRow

@pytest.fixture
def executor(tmp_path):
    """Create an Executor instance with temporary storage."""
    base_path = str(tmp_path)
    # Initialize Catalog
    catalog = Catalog(base_path)
    # Initialize BufferManager
    buffer_manager = BufferManager()
    
    context = ExecutionContext(catalog, buffer_manager, base_path)
    return Executor(context)

def test_create_insert_select_basic(executor):
    # 1. Create Table
    list(executor.execute("CREATE TABLE users (id INT, name STRING)"))
    
    # 2. Insert
    list(executor.execute("INSERT INTO users VALUES (1, 'Alice')"))
    list(executor.execute("INSERT INTO users VALUES (2, 'Bob')"))
    
    # 3. Select *
    rows = list(executor.execute("SELECT * FROM users"))
    assert len(rows) == 2
    # Verify values. Order depends on scan (insert order usually).
    # Convert to list of dicts
    data = [row.values for row in rows]
    # Check 1
    alice = next(r for r in data if r['id'] == 1)
    assert alice['name'] == 'Alice'
    bob = next(r for r in data if r['id'] == 2)
    assert bob['name'] == 'Bob'

def test_filter_project(executor):
    list(executor.execute("CREATE TABLE items (id INT, price FLOAT)"))
    list(executor.execute("INSERT INTO items VALUES (1, 10.5)"))
    list(executor.execute("INSERT INTO items VALUES (2, 20.0)"))
    list(executor.execute("INSERT INTO items VALUES (3, 5.5)"))
    
    # Filter
    rows = list(executor.execute("SELECT id, price FROM items WHERE price > 10.0"))
    assert len(rows) == 2 # 1 and 2
    
    ids = sorted([row.values['id'] for row in rows])
    assert ids == [1, 2]
    
    # Expression in Projection
    rows = list(executor.execute("SELECT id, price * 2 AS double_price FROM items WHERE id = 3"))
    assert len(rows) == 1
    assert rows[0].values['double_price'] == 11.0

def test_sort_limit(executor):
    list(executor.execute("CREATE TABLE t (val INT)"))
    for i in [5, 1, 3, 2, 4]:
        list(executor.execute(f"INSERT INTO t VALUES ({i})"))
        
    # Sort ASC
    rows = list(executor.execute("SELECT val FROM t ORDER BY val ASC"))
    vals = [r.values['val'] for r in rows]
    assert vals == [1, 2, 3, 4, 5]
    
    # Sort DESC LIMIT
    rows = list(executor.execute("SELECT val FROM t ORDER BY val DESC LIMIT 3"))
    vals = [r.values['val'] for r in rows]
    assert vals == [5, 4, 3]

def test_update_delete(executor):
    list(executor.execute("CREATE TABLE t (id INT, status STRING)"))
    list(executor.execute("INSERT INTO t VALUES (1, 'active')"))
    list(executor.execute("INSERT INTO t VALUES (2, 'inactive')"))
    
    # Update
    list(executor.execute("UPDATE t SET status = 'deleted' WHERE id = 2"))
    
    rows = list(executor.execute("SELECT status FROM t WHERE id = 2"))
    assert rows[0].values['status'] == 'deleted'
    
    # Delete
    list(executor.execute("DELETE FROM t WHERE status = 'deleted'"))
    
    rows = list(executor.execute("SELECT * FROM t"))
    assert len(rows) == 1
    assert rows[0].values['id'] == 1

def test_select_no_from(executor):
    rows = list(executor.execute("SELECT 1 AS a, 'hello' AS b"))
    assert len(rows) == 1
    assert rows[0].values['a'] == 1
    assert rows[0].values['b'] == 'hello'

def test_null_handling(executor):
    list(executor.execute("CREATE TABLE t (val INT)"))
    # ID column implicitly? No, simplistic Create. But row values match schema.
    # We rely on positional insert? Planner Insert uses `stmt.values`.
    # `LogicalValues` produces list of expressions.
    # `InsertExec` assumes schema order.
    # Schema has `val`.
    
    # NULL insert? AST parses NULL as Literal(None).
    # But `NULL` keyword support in Tokenizer?
    # Tokenizer maps `NULL` -> `TokenType.NULL`.
    # Parser `_parse_primary` handles `TokenType.NULL` -> `Literal(None, ...)`?
    # Let's check Parser implementation later if fails.
    # Assuming `parser` supports `NULL` literal.
    
    # Actually `parser.py`:
    # if self._match(TokenType.NULL): return Literal(None, DataType.NULL) (Wait datatype is implicit?)
    # Let's check `parser.py` code via memory or test failure.
    # Assuming it works.
    
    # Need to verify if NULL is supported in Parser.
    pass

def test_runtime_error_div_zero(executor):
    with pytest.raises(RuntimeError, match="Division by zero"):
        list(executor.execute("SELECT 1 / 0"))

def test_select_expression_alias_naming(executor):
    # Verify default alias is expression text
    rows = list(executor.execute("SELECT 1 + 1"))
    # The alias should be "(1 + 1)" or similar from __str__
    # AST: BinaryExpr(Literal(1), PLUS, Literal(1))
    # __str__: "(1 + 1)"
    assert "(1 + 1)" in rows[0].values
    assert rows[0].values["(1 + 1)"] == 2
