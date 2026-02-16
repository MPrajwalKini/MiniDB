"""
MiniDB Schema Definition
========================
Defines table schemas: column name, type, nullable, default value.
Supports serialization of schema to/from bytes for persistence in
the table file header page.

Teaching note:
  In PostgreSQL, schemas are stored in system catalog tables (pg_attribute,
  pg_class). We store the schema directly in the table file's header page
  as a JSON blob for simplicity — this avoids bootstrapping complexity.
"""

import json
from dataclasses import dataclass, field
from typing import Any, Optional

from storage.types import DataType, type_from_string


@dataclass
class Column:
    """Definition of a single column in a table schema."""
    name: str
    data_type: DataType
    nullable: bool = True
    default: Optional[Any] = None

    def to_dict(self) -> dict:
        """Serialize column definition to a dictionary."""
        d: dict = {
            "name": self.name,
            "type": self.data_type.value,
            "nullable": self.nullable,
        }
        if self.default is not None:
            d["default"] = self.default
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "Column":
        """Deserialize a column definition from a dictionary."""
        return cls(
            name=d["name"],
            data_type=type_from_string(d["type"]),
            nullable=d.get("nullable", True),
            default=d.get("default"),
        )


@dataclass
class Schema:
    """
    Table schema — an ordered list of column definitions.
    Provides column lookup by name and index, plus serialization.
    """
    columns: list[Column] = field(default_factory=list)

    @property
    def column_count(self) -> int:
        return len(self.columns)

    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def column_index(self, name: str) -> int:
        """Get the zero-based index of a column by name. Raises KeyError if not found."""
        for i, col in enumerate(self.columns):
            if col.name.lower() == name.lower():
                return i
        raise KeyError(f"Column '{name}' not found in schema. "
                       f"Available: {self.column_names()}")

    def get_column(self, name: str) -> Column:
        """Get a column definition by name."""
        idx = self.column_index(name)
        return self.columns[idx]

    def validate_row(self, row: list[Any]) -> list[str]:
        """
        Validate a row of values against the schema.
        Returns a list of error messages (empty = valid).
        """
        errors: list[str] = []
        if len(row) != self.column_count:
            errors.append(
                f"Expected {self.column_count} values, got {len(row)}"
            )
            return errors

        for i, (col, val) in enumerate(zip(self.columns, row)):
            if val is None and not col.nullable:
                errors.append(f"Column '{col.name}' does not allow NULL")
        return errors

    # ─── Serialization ──────────────────────────────────────────────

    def to_bytes(self) -> bytes:
        """Serialize schema to bytes (JSON-encoded UTF-8)."""
        data = {"columns": [c.to_dict() for c in self.columns]}
        return json.dumps(data, separators=(",", ":")).encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> "Schema":
        """Deserialize schema from bytes."""
        parsed = json.loads(data.decode("utf-8"))
        columns = [Column.from_dict(d) for d in parsed["columns"]]
        return cls(columns=columns)

    def to_dict(self) -> dict:
        return {"columns": [c.to_dict() for c in self.columns]}

    @classmethod
    def from_dict(cls, d: dict) -> "Schema":
        columns = [Column.from_dict(cd) for cd in d["columns"]]
        return cls(columns=columns)
