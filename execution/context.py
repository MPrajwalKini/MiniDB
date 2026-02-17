from dataclasses import dataclass, field
from typing import Optional
from catalog.catalog import Catalog
from storage.buffer import BufferManager
import os

from catalog.system_catalog import SystemCatalog
from catalog.database import Database
from typing import List

@dataclass
class ExecutionContext:
    """Run-time context for query execution."""
    catalog: "Catalog"
    buffer_manager: BufferManager
    base_path: str
    
    # New catalog fields
    system_catalog: Optional[SystemCatalog] = field(default=None)
    current_db: Optional[Database] = field(default=None)
    search_path: List[int] = field(default_factory=list)

    txn_manager: Optional[object] = field(default=None)   # TransactionManager
    log_manager: Optional[object] = field(default=None)    # LogManager
    lock_manager: Optional[object] = field(default=None)   # LockManager
    active_txn_id: Optional[int] = field(default=None)     # Current transaction

    def get_table_path(self, table_name: str) -> str:
        return os.path.join(self.base_path, f"{table_name}.tbl")

