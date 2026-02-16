"""
MiniDB Index Manager
====================
Index lifecycle: build from table, open existing, drop.
Integrates B-Tree with Catalog for persistent index metadata.

Concurrency: single-writer assumed.
"""

import os
from typing import Optional

from catalog.catalog import Catalog
from storage.buffer import BufferManager
from storage.table import TableFile
from storage.types import DataType
from storage.page import RID
from indexing.btree import BTree


def build_index(catalog: Catalog, table_file: TableFile,
                table_name: str, column_name: str,
                index_name: str, buffer_mgr: BufferManager) -> BTree:
    """
    Build a new B-Tree index from an existing table.

    1. Registers the index in the Catalog.
    2. Scans the table for all non-NULL values in the target column.
    3. Inserts each (key, RID) pair into the B-Tree.
    4. Flushes the index to disk.

    Returns the open BTree handle.
    """
    # Validate column exists
    schema = table_file.schema
    col_idx = None
    col_type = None
    for i, col in enumerate(schema.columns):
        if col.name.lower() == column_name.lower():
            col_idx = i
            col_type = col.data_type
            break

    if col_idx is None:
        raise ValueError(
            f"Column '{column_name}' not found in table '{table_name}'. "
            f"Available: {[c.name for c in schema.columns]}"
        )

    # Register in catalog (gets file name)
    idx_file = catalog.create_index(index_name, table_name, column_name)
    idx_path = os.path.join(catalog.data_dir, idx_file)

    # Create the B-Tree
    btree = BTree.create(idx_path, table_name, column_name, col_type, buffer_mgr)

    # Scan table and insert all non-NULL keys
    count = 0
    for rid, row in table_file.scan():
        value = row[col_idx]
        if value is None:
            continue  # NULLs not indexed
        # Reject NaN floats
        if col_type == DataType.FLOAT:
            import math
            if math.isnan(value):
                continue
        btree.insert(value, rid)
        count += 1

    # Flush
    btree.close()

    # Reopen for caller
    btree = BTree.open(idx_path, buffer_mgr)
    return btree


def open_index(catalog: Catalog, index_name: str,
               buffer_mgr: BufferManager) -> BTree:
    """
    Open an existing index by name.
    Looks up the index file path from the catalog.
    """
    idx_info = catalog.get_index(index_name) if hasattr(catalog, 'get_index') else None

    if idx_info is None:
        # Fallback: search by name in indexes
        indexes = catalog.get_indexes_for_table("")  # we need to find it
        raise ValueError(f"Index '{index_name}' not found in catalog")

    idx_path = os.path.join(catalog.data_dir, idx_info["file"])
    return BTree.open(idx_path, buffer_mgr)


def open_index_by_info(catalog: Catalog, idx_info: dict,
                       buffer_mgr: BufferManager) -> BTree:
    """
    Open an existing index given its catalog entry directly.
    Avoids needing a get_index() method.
    """
    idx_path = os.path.join(catalog.data_dir, idx_info["file"])
    return BTree.open(idx_path, buffer_mgr)


def drop_index(catalog: Catalog, index_name: str) -> None:
    """
    Drop an index: remove from catalog and delete the index file.
    """
    file_name = catalog.drop_index(index_name)
    if file_name:
        idx_path = os.path.join(catalog.data_dir, file_name)
        if os.path.exists(idx_path):
            os.remove(idx_path)
