from cli.session import Session
from execution.planner import PhysicalPlanner
from planning.planner import Planner as LogicalPlanner
from catalog.resolver import Resolver
import os
import shutil
import glob
from pathlib import Path

DB_DIR = "debug_db"

if os.path.exists(DB_DIR):
    shutil.rmtree(DB_DIR)

print(f"Creating session at {DB_DIR}...")
s = Session(DB_DIR)

print("Executing CREATE TABLE t (x INT)...")
s.execute("CREATE TABLE t (x INT)")
# Removed explicit saves to test implicit persistence

# 1. Verify filesystem
print("\n--- Filesystem Check ---")
files = list(Path(DB_DIR).rglob("*.tbl"))
print(f"TBL Files: {files}")
json_files = list(Path(DB_DIR).rglob("schema.json"))
print(f"Schema Metadata Files: {json_files}")

if not files:
    print("FATAL: Table file not created!")
else:
    print(f"Table file size: {files[0].stat().st_size} bytes")

# 2. Verify Session Context
print("\n--- Session Context ---")
print(f"Current DB: {s.current_db.name} (OID: {s.current_db.oid})")
print(f"Search Path: {s.search_path}")
print(f"System Catalog Path: {s.system_catalog.data_root}")

# 3. Test New Resolver (independent)
print("\n--- Independent Resolver Test ---")
r = Resolver(s.system_catalog)
try:
    db, schema, table = r.resolve_table("t", s.current_db.oid, s.search_path)
    print(f"SUCCESS: Resolved 't' to OID {table.oid} in schema '{schema.name}'")
    
    # Check path construction match
    expected_path = schema.schema_dir / f"table_{table.oid}.tbl"
    print(f"Expected Path: {expected_path}")
    print(f"Exists? {expected_path.exists()}")
    
except Exception as e:
    print(f"FAILURE: Resolver failed: {e}")

# 4. Test Logical Planner
print("\n--- Logical Planner Test ---")
lp = LogicalPlanner(s.catalog, s.system_catalog, s.current_db, s.search_path)
schema_obj = lp._get_table_schema("t")
if schema_obj:
    print("SUCCESS: LogicalPlanner found schema.")
else:
    print("FAILURE: LogicalPlanner returned None.")

# 5. Test Physical Planner
print("\n--- Physical Planner Test ---")
pp = PhysicalPlanner(s.context)
# Manually test _resolve_table_info
schema_obj, path = pp._resolve_table_info("t")
print(f"Result: schema={schema_obj is not None}, path={path}")

if not schema_obj:
    print("FAILURE: PhysicalPlanner failed to resolve table.")
else:
    print("SUCCESS: PhysicalPlanner resolved table.")

print("\n--- Execute INSERT Test ---")
try:
    s.execute("INSERT INTO t (x) VALUES (1)")
    print("SUCCESS: INSERT executed.")
except Exception as e:
    print(f"FAILURE: INSERT failed: {e}")
    # import traceback; traceback.print_exc()

s.close()
