import sys
import os

# Ensure project root is on path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from parser import parse, ParseError

def run_report():
    print("# Parser Verification Report\n")
    
    print("## Valid SQL -> AST Dumps\n")
    
    queries = [
        "SELECT 1",
        "SELECT * FROM users",
        "SELECT id, name FROM users WHERE active = TRUE",
        "INSERT INTO users VALUES (1, 'Alice')",
        "UPDATE users SET active = FALSE WHERE id = 1",
        "DELETE FROM users WHERE id = 1",
        "CREATE TABLE items (id INT NOT NULL, price FLOAT)",
        "SELECT * FROM t ORDER BY created_at DESC LIMIT 10",
        "SELECT DISTINCT name FROM users WHERE age > 18 AND (role = 'admin' OR role = 'mod')",
        "SELECT t.id, s.t.col FROM t WHERE col IS NULL"
    ]
    
    for i, sql in enumerate(queries, 1):
        print(f"### Example {i}")
        print(f"**SQL**: `{sql}`")
        try:
            ast = parse(sql)
            print(f"**AST**: `{ast}`\n")
        except ParseError as e:
            print(f"**ERROR**: {e}\n")

    print("## Parser Error Examples\n")
    
    errors = [
        "SELECT * users",          # Missing FROM (or unexpected trail)
        "SELECT FROM t",           # Missing expression
        "SELECT (1 + 2",           # Unbalanced paren
        "INSERT INTO t VALUES",    # Missing values
        "CREATE TABLE t (id)",     # Missing type
    ]
    
    for i, sql in enumerate(errors, 1):
        print(f"### Error Case {i}")
        print(f"**SQL**: `{sql}`")
        try:
            parse(sql)
            print("**Result**: Parsed successfully (Unexpected!)\n")
        except ParseError as e:
            print(f"**Error Output**: `{e}`\n")

if __name__ == "__main__":
    run_report()
