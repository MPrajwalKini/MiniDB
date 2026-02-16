from dataclasses import dataclass, field
from typing import Optional
from catalog.catalog import Catalog
from storage.buffer import BufferManager
import os

@dataclass
class ExecutionContext:
    """Run-time context for query execution."""
    catalog: Catalog
    buffer_manager: BufferManager
    base_path: str
    txn_manager: Optional[object] = field(default=None)   # TransactionManager
    log_manager: Optional[object] = field(default=None)    # LogManager
    lock_manager: Optional[object] = field(default=None)   # LockManager
    active_txn_id: Optional[int] = field(default=None)     # Current transaction

    def get_table_path(self, table_name: str) -> str:
        return os.path.join(self.base_path, f"{table_name}.tbl")

