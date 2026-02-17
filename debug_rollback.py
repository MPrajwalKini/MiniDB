from cli.session import Session
import os
import shutil
from pathlib import Path

import sys
import storage.table
print(f"DEBUG: sys.path = {sys.path}")
print(f"DEBUG: storage.table is from {storage.table.__file__}")

DB_DIR = "debug_rollback_db"
if os.path.exists(DB_DIR):
    shutil.rmtree(DB_DIR) 

print(f"Creating session at {DB_DIR}...")
s = Session(DB_DIR)

print("Starting Transaction...")
s.execute("BEGIN")
print(f"Txn ID: {s.active_txn_id}")

print("Creating Table...")
s.execute("CREATE TABLE t (x INT)") # Schema.json updated

print("Inserting Data...")
s.execute("INSERT INTO t (x) VALUES (1)")
# Check data visible
res = list(s.execute("SELECT * FROM t"))
print(f"Rows inside txn: {len(res)}") # Should be 1

print("Rolling Back...")
s.execute("ROLLBACK")
print(f"Txn ID after rollback: {s.active_txn_id}")

# Check data gone
print("Checking data after rollback...")
res = list(s.execute("SELECT * FROM t")) # Auto-txn?
print(f"Rows after rollback: {len(res)}")

if len(res) == 0:
    print("SUCCESS: Data rolled back.")
else:
    print("FAILURE: Data persisted.")

s.close()
