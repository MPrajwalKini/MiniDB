
"""
Legacy Catalog Compatibility Layer
==================================
Re-exports the Legacy Catalog class to maintain compatibility
with existing code that imports `catalog.catalog`.

DEPRECATED: Use `catalog.system_catalog.SystemCatalog` for new code.
"""

from catalog.legacy import Catalog
