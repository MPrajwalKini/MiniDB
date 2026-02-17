from cli.session import Session
import os
import shutil
import glob

if os.path.exists("test_db_repro"):
    shutil.rmtree("test_db_repro")

print("Initializing session...")
s = Session("test_db_repro")

print("Executing CREATE TABLE...")
res = s.execute("CREATE TABLE t (x INT)")
print("Result:", res)

# Verify file exists
print("Checking files...")
files = glob.glob("test_db_repro/**/*.tbl", recursive=True)
print("Files found:", files)

# Verify schema.json content
schema_files = glob.glob("test_db_repro/**/schema.json", recursive=True)
for sf in schema_files:
    print(f"Schema file: {sf}")
    with open(sf, 'r') as f:
        print(f.read())

print("Executing INSERT...")
try:
    s.execute("INSERT INTO t (x) VALUES (1)")
    print("Insert success.")
except Exception as e:
    print("Insert failed:", e)
    import traceback
    traceback.print_exc()

s.close()
