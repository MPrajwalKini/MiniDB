# MiniDB Storage Format Specification

> **Format Version**: 1  
> **Last Updated**: 2026-02-16

## Overview

MiniDB uses a **page-based binary storage format** inspired by PostgreSQL's heap storage.
Each table is stored as a `.tbl` file composed of fixed-size **4KB pages**. This design
mirrors how real databases manage disk I/O — data is always read and written in page-sized
units, which aligns with OS page sizes and disk block sizes.

## Format Versioning

| Field | Value | Notes |
|-------|-------|-------|
| Current format version | `1` | Stored in byte 0-1 of the file header page |
| Magic bytes | `0x4D44` (`"MD"`) | Identifies a MiniDB file |
| Version location | File header page (page 0), offset 0 | First 4 bytes: magic (2B) + version (2B) |

### Migration Strategy

When the format version changes:
1. `verify_build.py` detects version mismatch in `.tbl` files
2. A migration script (future: `migrate_storage.py`) reads old-format pages and rewrites
3. Format version is bumped in file header after successful migration
4. Old files can be backed up before migration with `.tbl.v{N}.bak` naming

**Current version 1**: No migrations needed. This is the initial format.

## Record ID (RID) — Formal Definition

A **Record ID (RID)** uniquely identifies a single tuple within a table.

```
RID = (page_id: uint32, slot_id: uint16)
```

| Component | Type | Size | Description |
|-----------|------|------|-------------|
| `page_id` | `uint32` | 4 bytes | Zero-based index of the page within the `.tbl` file. Page 0 is the header page; data pages start at page 1. |
| `slot_id` | `uint16` | 2 bytes | Zero-based index into the page's tuple slot directory. |

### RID Properties

- **Stable within a transaction**: A RID does not change once assigned, unless the tuple is deleted and its slot is reused.
- **Not globally unique**: RIDs are scoped to a single table file.
- **Indirection via slots**: The slot directory maps `slot_id` → `(offset, length)` within the page. This allows in-page compaction without invalidating RIDs.
- **Deleted slots**: A deleted tuple's slot has `offset = 0, length = 0`. The slot may be reused by future inserts.
- **Serialized form**: When stored (e.g., in index leaf nodes), a RID is packed as 6 bytes: `page_id (4B big-endian) + slot_id (2B big-endian)`.

### Comparison with PostgreSQL

| Aspect | MiniDB RID | PostgreSQL ctid |
|--------|-----------|-----------------|
| Format | `(page_id, slot_id)` | `(block_number, offset_number)` |
| Size | 6 bytes | 6 bytes (4 + 2) |
| Scope | Per-table | Per-table |
| Stability | Stable until DELETE | Can change due to VACUUM/UPDATE (HOT) |

## Page Layout (4096 bytes)

```
┌────────────────────────────────────────────────────────────────┐
│                    PAGE HEADER (24 bytes)                      │
│  ┌──────────┬──────────┬──────────┬───────────┬────────────┐  │
│  │ format_v │ page_id  │num_slots │free_start │   flags    │  │
│  │ (2 bytes)│ (4 bytes)│ (2 bytes)│ (2 bytes) │ (2 bytes)  │  │
│  ├──────────┼──────────┼──────────┼───────────┼────────────┤  │
│  │free_end  │ checksum │  reserved                         │  │
│  │(2 bytes) │ (4 bytes)│  (6 bytes)                        │  │
│  └──────────┴──────────┴───────────────────────────────────┘  │
├────────────────────────────────────────────────────────────────┤
│                TUPLE SLOT DIRECTORY                            │
│  (grows downward from offset 24)                              │
│                                                               │
│  Each slot: 4 bytes                                           │
│  ┌──────────────┬──────────┐                                  │
│  │  offset      │  length  │                                  │
│  │  (2 bytes)   │ (2 bytes)│                                  │
│  └──────────────┴──────────┘                                  │
│                                                               │
│  slot[0], slot[1], ..., slot[num_slots - 1]                   │
├────────────────────────────────────────────────────────────────┤
│                                                               │
│              FREE SPACE                                       │
│         (between slot directory and tuples)                   │
│                                                               │
├────────────────────────────────────────────────────────────────┤
│                 TUPLE DATA                                    │
│  (grows upward from page end toward free_start)               │
│                                                               │
│  ┌──────────────────────────────────────────────────────┐     │
│  │  Tuple N (oldest, near page end)                     │     │
│  ├──────────────────────────────────────────────────────┤     │
│  │  Tuple N-1                                           │     │
│  ├──────────────────────────────────────────────────────┤     │
│  │  ...                                                 │     │
│  ├──────────────────────────────────────────────────────┤     │
│  │  Tuple 0 (newest, closest to free space)             │     │
│  └──────────────────────────────────────────────────────┘     │
└────────────────────────────────────────────────────────────────┘
```

### Page Header Fields

| Field | Offset | Size | Description |
|-------|--------|------|-------------|
| `format_version` | 0 | 2 bytes | Storage format version (currently `1`) |
| `page_id` | 2 | 4 bytes | Page number within the table file |
| `num_slots` | 6 | 2 bytes | Number of entries in slot directory |
| `free_start` | 8 | 2 bytes | Byte offset where free space begins (after slot dir) |
| `flags` | 10 | 2 bytes | Page flags (bit 0: is_leaf for index pages) |
| `free_end` | 12 | 2 bytes | Byte offset where free space ends (before tuple data) |
| `checksum` | 14 | 4 bytes | CRC32 of page contents (excluding this field) |
| `reserved` | 18 | 6 bytes | Reserved for future use (zeroed) |

### Key Design Points

| Property | Value | Notes |
|----------|-------|-------|
| Page size | 4096 bytes | Fixed. Matches typical OS page and disk block size |
| Header size | 24 bytes | Fixed. Contains metadata for the page |
| Slot size | 4 bytes | Per tuple. Offset (2B) + length (2B) |
| Max tuple size | ~4068 bytes | PAGE_SIZE - HEADER_SIZE - one slot |
| Free growth | Slot dir ↓, Tuples ↑ | Like PostgreSQL's heap pages |

### Why Page-Based?

In PostgreSQL, data is organized into 8KB pages. We use 4KB for simplicity. Benefits:
- **Buffer pool friendly**: Pages can be cached in memory
- **Disk-aligned I/O**: Reads/writes align with disk blocks
- **Slot indirection**: Tuples can be reorganized without changing external references
- **Space tracking**: Free space is managed at the page level

### Comparison with Real Databases

| Aspect | MiniDB | PostgreSQL | Snowflake |
|--------|--------|-----------|-----------|
| Page size | 4KB | 8KB | Uses micro-partitions (16MB compressed columnar files) |
| Storage model | Row-based (NSM) | Row-based (NSM) | Columnar (PAX-style) |
| Tuple identification | (page_id, slot_id) | (block_number, offset) aka ctid | Virtual row IDs |
| Free space | In-page tracking | Free Space Map (FSM) | Immutable (copy-on-write) |

## Tuple Format

Each tuple stored in a page has this binary layout:

```
┌─────────────────────────────────────────────┐
│  TUPLE HEADER (6 bytes)                     │
│  ┌─────────────┬───────────┬──────────────┐ │
│  │  tuple_len  │  null_bmp │    flags     │ │
│  │  (2 bytes)  │ (2 bytes) │   (2 bytes)  │ │
│  └─────────────┴───────────┴──────────────┘ │
├─────────────────────────────────────────────┤
│  COLUMN DATA                                │
│  Fixed-size fields in schema order           │
│  Variable-size fields: length-prefixed       │
│                                             │
│  INT:     4 bytes (signed, big-endian)      │
│  FLOAT:   8 bytes (IEEE 754 double)         │
│  BOOLEAN: 1 byte (0x00 = false, 0x01 = true)│
│  DATE:    4 bytes (days since epoch)        │
│  STRING:  2 bytes length + UTF-8 data       │
└─────────────────────────────────────────────┘
```

### Null Bitmap

The null bitmap uses 1 bit per column. If bit `i` is set, column `i` is NULL
and its data area is skipped (0 bytes written). This saves space for sparse data.

## Table File Layout (.tbl)

A `.tbl` file is a sequence of pages:

```
┌─────────┬─────────┬─────────┬─────┬─────────┐
│ Page 0  │ Page 1  │ Page 2  │ ... │ Page N  │
│ (4096B) │ (4096B) │ (4096B) │     │ (4096B) │
└─────────┴─────────┴─────────┴─────┴─────────┘
```

- **Page 0** is always the **header page** containing table metadata (schema, table name, creation time)
- **Pages 1..N** are data pages containing tuples
- New pages are allocated when existing pages have insufficient free space

## Data Type Specifications

| Type | Size | Format | Range |
|------|------|--------|-------|
| INT | 4 bytes | Big-endian signed int32 | -2,147,483,648 to 2,147,483,647 |
| FLOAT | 8 bytes | IEEE 754 double | ±1.7×10³⁰⁸ |
| STRING | Variable | 2-byte length prefix + UTF-8 | Up to 65,535 bytes |
| BOOLEAN | 1 byte | 0x00 = false, 0x01 = true | true/false |
| DATE | 4 bytes | Days since 1970-01-01 | ~5.8 million years range |

## Index File Format (.idx)

B-Tree indexes are stored in separate `.idx` files using a similar page-based layout:

```
┌────────────────┐
│ Index Header   │  Root page ID, key type, order
│ Page (4096B)   │
├────────────────┤
│ Internal Node  │  Keys + child page pointers
│ Pages          │
├────────────────┤
│ Leaf Node      │  Keys + (page_id, slot_id) tuple references
│ Pages          │
└────────────────┘
```

### B-Tree Properties

| Property | Value |
|----------|-------|
| Order (max children per node) | ~200 for INT keys |
| Minimum fill | 50% |
| Leaf linking | Doubly-linked for range scans |
| Key types supported | INT, FLOAT, STRING, DATE |

## Catalog File (catalog.dat)

Stores metadata about all tables and indexes in a JSON-serialized format
(for simplicity — real databases use system tables).

```json
{
  "tables": {
    "users": {
      "file": "users.tbl",
      "schema": [
        {"name": "id", "type": "INT", "nullable": false},
        {"name": "name", "type": "STRING", "nullable": true}
      ],
      "created_at": "2026-02-16T12:00:00Z"
    }
  },
  "indexes": {
    "idx_users_id": {
      "table": "users",
      "column": "id",
      "file": "idx_users_id.idx",
      "type": "BTREE"
    }
  }
}
```

## Write-Ahead Log (WAL) Format

The WAL file (`wal.log`) records all mutations before they're applied:

```
┌──────────────────────────────────────────┐
│ WAL Entry                                │
│ ┌──────────┬───────┬─────────┬─────────┐ │
│ │  txn_id  │  op   │  table  │  data   │ │
│ │ (4 bytes)│(1 byte)│(var len)│(var len)│ │
│ └──────────┴───────┴─────────┴─────────┘ │
├──────────────────────────────────────────┤
│ WAL Entry ...                            │
└──────────────────────────────────────────┘

Op codes: INSERT=0x01, UPDATE=0x02, DELETE=0x03,
          COMMIT=0x10, ROLLBACK=0x11
```

## Limitations (by design)

- No multi-table joins (single-table queries only)
- No subqueries or nested expressions
- No ALTER TABLE
- No VARCHAR length limits (STRING is unbounded up to 64KB)
- No foreign keys or constraints beyond NOT NULL
- Single-threaded query execution
- No query result caching
- Maximum row size ~4KB (must fit in one page)

## Extension Points

The architecture is designed for these future extensions:
- **Joins**: Add NestedLoopJoin / HashJoin operators in executor
- **Aggregations**: Add GroupBy + aggregate operators (SUM, COUNT, AVG)
- **Views**: Store parsed SQL in catalog, re-plan on access
- **Compression**: Add page-level compression (zlib/lz4)
- **Buffer pool**: Add an in-memory page cache with LRU eviction
