"""
MiniDB Indexing Module
======================
Disk-backed B+ Tree index for accelerating queries.

Components:
  - key_encoding: Order-preserving memcmp-sortable binary key encoding
  - btree: B+ Tree with insert, search, range scan, persistence
  - index_manager: Index lifecycle (build, open, drop)

Status: COMPLETE
"""
