"""
MiniDB Transactions Module
==========================
Phase 6: Write-Ahead Logging (WAL) with ACID guarantees.
Status: IN_PROGRESS

Components:
  - wal.py: LogManager (append-only WAL with CRC32, offset-based LSN)
  - transaction.py: TransactionManager (begin/commit/abort, CLR-based undo)
  - recovery.py: RecoveryManager (ARIES-style Analysis/Redo/Undo)
"""
