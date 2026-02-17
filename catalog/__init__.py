
# MiniDB Catalog Package
# ======================
# Provides the hierarchical metadata management system.

from catalog.system_catalog import SystemCatalog, DatabaseInfo
from catalog.database import Database, SchemaInfo
from catalog.schema import Schema, TableInfo, IndexInfo
from catalog.bootstrap import bootstrap_system

# Legacy support
# from catalog.legacy import Catalog as LegacyCatalog
