"""Smoke test for WAL LogManager."""
import os, sys, tempfile, shutil

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from transactions.wal import LogManager, WALRecordType, NULL_LSN

tmp = tempfile.mkdtemp(prefix="wal_smoke_")
try:
    # 1. Create and write records
    lm = LogManager(tmp)
    lsn1 = lm.append_begin(100)
    print(f"BEGIN  lsn={lsn1}")
    assert lsn1 == 4, f"Expected first LSN=4, got {lsn1}"

    lsn2 = lm.append_insert(100, lsn1, "users", 1, 0, b"hello world")
    print(f"INSERT lsn={lsn2}")

    lsn3 = lm.append_update(100, lsn2, "users", 1, 0, b"hello world", b"hi world")
    print(f"UPDATE lsn={lsn3}")

    lsn4 = lm.append_delete(100, lsn3, "users", 1, 0, b"hi world")
    print(f"DELETE lsn={lsn4}")

    lsn5 = lm.append_commit(100, lsn4)
    print(f"COMMIT lsn={lsn5}")

    # 2. Scan all records
    records = list(lm.scan())
    print(f"\nScanned {len(records)} records")
    for r in records:
        print(f"  LSN={r.lsn} txn={r.txn_id} prev={r.prev_lsn} type={r.record_type.name} payload={len(r.payload)}B")
    assert len(records) == 5

    # 3. Random access read
    rec = lm.read_record(lsn3)
    assert rec.record_type == WALRecordType.UPDATE
    assert rec.txn_id == 100
    tname, pid, sid, old, new = LogManager.parse_update_payload(rec.payload)
    assert tname == "users" and old == b"hello world" and new == b"hi world"
    print(f"\nRandom read at {lsn3}: UPDATE users p={pid} s={sid} old={len(old)}B new={len(new)}B")

    # 4. Parse INSERT payload
    rec2 = lm.read_record(lsn2)
    tname2, pid2, sid2, data2 = LogManager.parse_dml_payload(rec2.payload)
    assert tname2 == "users" and data2 == b"hello world"

    # 5. Durable LSN
    assert lm.durable_lsn == lm.next_lsn, "After commit, durable should equal next"

    # 6. Close and reopen — scan should still work
    lm.close()
    lm2 = LogManager(tmp)
    records2 = list(lm2.scan())
    assert len(records2) == 5
    assert records2[0].record_type == WALRecordType.BEGIN
    assert records2[-1].record_type == WALRecordType.COMMIT

    # 7. Append more after reopen
    lsn6 = lm2.append_begin(200)
    lsn7 = lm2.append_checkpoint([(200, lsn6)])
    records3 = list(lm2.scan())
    assert len(records3) == 7

    # 8. Hex dump of first record
    wal_path = os.path.join(tmp, "wal.log")
    with open(wal_path, "rb") as f:
        raw = f.read()
    print(f"\nWAL file size: {len(raw)} bytes")
    print(f"First 40 bytes (hex): {raw[:40].hex()}")

    # 9. CLR test
    lsn8 = lm2.append_clr(200, lsn7, lsn6, WALRecordType.DELETE, b"clr_data")
    rec_clr = lm2.read_record(lsn8)
    assert rec_clr.record_type == WALRecordType.CLR
    undo_next, inner_t, inner_p = LogManager.parse_clr_payload(rec_clr.payload)
    assert undo_next == lsn6
    assert inner_p == b"clr_data"

    lm2.close()
    print("\n✅ ALL SMOKE TESTS PASSED")

finally:
    shutil.rmtree(tmp, ignore_errors=True)
