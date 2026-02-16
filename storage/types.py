"""
MiniDB Data Type System
=======================
Defines supported data types: INT, FLOAT, STRING, BOOLEAN, DATE.
Each type provides serialization/deserialization, size calculation,
validation, and Python-native conversion.

Teaching note:
  Real databases (PostgreSQL) have a type system catalog (pg_type) with
  hundreds of types. We implement 5 core types. Snowflake adds VARIANT
  for semi-structured data — we don't, keeping things relational.
"""

import struct
from datetime import date, datetime
from enum import Enum
from typing import Any, Optional


class DataType(Enum):
    """Supported data types in MiniDB."""
    INT = "INT"
    FLOAT = "FLOAT"
    STRING = "STRING"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"


# ─── Size constants ─────────────────────────────────────────────────────────

FIXED_SIZES: dict[DataType, int] = {
    DataType.INT: 4,       # int32 big-endian
    DataType.FLOAT: 8,     # IEEE 754 double
    DataType.BOOLEAN: 1,   # 0x00 / 0x01
    DataType.DATE: 4,      # int32 days since epoch
}

# STRING is variable-length: 2-byte length prefix + UTF-8 data


def is_fixed_size(dtype: DataType) -> bool:
    """Return True if the data type has a fixed byte size."""
    return dtype in FIXED_SIZES


def fixed_size(dtype: DataType) -> Optional[int]:
    """Return the fixed byte size, or None for variable-length types."""
    return FIXED_SIZES.get(dtype)


# ─── Validation ─────────────────────────────────────────────────────────────

def validate(value: Any, dtype: DataType) -> bool:
    """
    Check if a Python value is compatible with the given DataType.
    Returns True if valid, False otherwise.
    """
    if value is None:
        return True  # NULL is valid for any type (nullable checked at schema level)

    if dtype == DataType.INT:
        return isinstance(value, int) and not isinstance(value, bool)
    elif dtype == DataType.FLOAT:
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    elif dtype == DataType.STRING:
        return isinstance(value, str)
    elif dtype == DataType.BOOLEAN:
        return isinstance(value, bool)
    elif dtype == DataType.DATE:
        return isinstance(value, (date, str))
    return False


def coerce(value: Any, dtype: DataType) -> Any:
    """
    Attempt to coerce a value to the target DataType.
    Used by the parser to convert string literals to typed values.
    Raises ValueError on failure.
    """
    if value is None:
        return None

    if dtype == DataType.INT:
        return int(value)
    elif dtype == DataType.FLOAT:
        return float(value)
    elif dtype == DataType.STRING:
        return str(value)
    elif dtype == DataType.BOOLEAN:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            if value.upper() in ("TRUE", "1", "YES"):
                return True
            if value.upper() in ("FALSE", "0", "NO"):
                return False
        raise ValueError(f"Cannot coerce {value!r} to BOOLEAN")
    elif dtype == DataType.DATE:
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            return datetime.strptime(value, "%Y-%m-%d").date()
        raise ValueError(f"Cannot coerce {value!r} to DATE")
    raise ValueError(f"Unknown data type: {dtype}")


# ─── Serialization ──────────────────────────────────────────────────────────

# Epoch for DATE type
_DATE_EPOCH = date(1970, 1, 1)


def serialize_value(value: Any, dtype: DataType) -> bytes:
    """
    Serialize a Python value to bytes according to its DataType.
    Raises ValueError if the value cannot be serialized.
    """
    if dtype == DataType.INT:
        return struct.pack(">i", int(value))

    elif dtype == DataType.FLOAT:
        return struct.pack(">d", float(value))

    elif dtype == DataType.BOOLEAN:
        return b"\x01" if value else b"\x00"

    elif dtype == DataType.DATE:
        if isinstance(value, str):
            value = datetime.strptime(value, "%Y-%m-%d").date()
        delta = value - _DATE_EPOCH
        return struct.pack(">i", delta.days)

    elif dtype == DataType.STRING:
        encoded = str(value).encode("utf-8")
        if len(encoded) > 65535:
            raise ValueError(f"String too long: {len(encoded)} bytes (max 65535)")
        return struct.pack(">H", len(encoded)) + encoded

    raise ValueError(f"Cannot serialize type: {dtype}")


def deserialize_value(data: bytes, offset: int, dtype: DataType) -> tuple[Any, int]:
    """
    Deserialize a value from bytes at the given offset.
    Returns (value, new_offset).
    """
    if dtype == DataType.INT:
        val = struct.unpack_from(">i", data, offset)[0]
        return val, offset + 4

    elif dtype == DataType.FLOAT:
        val = struct.unpack_from(">d", data, offset)[0]
        return val, offset + 8

    elif dtype == DataType.BOOLEAN:
        val = data[offset] != 0
        return val, offset + 1

    elif dtype == DataType.DATE:
        days = struct.unpack_from(">i", data, offset)[0]
        val = date.fromordinal(_DATE_EPOCH.toordinal() + days)
        return val, offset + 4

    elif dtype == DataType.STRING:
        length = struct.unpack_from(">H", data, offset)[0]
        offset += 2
        val = data[offset:offset + length].decode("utf-8")
        return val, offset + length

    raise ValueError(f"Cannot deserialize type: {dtype}")


def type_from_string(type_str: str) -> DataType:
    """Convert a string like 'INT' to a DataType enum member."""
    normalized = type_str.strip().upper()
    try:
        return DataType(normalized)
    except ValueError:
        raise ValueError(f"Unknown data type: {type_str!r}. "
                         f"Valid types: {[t.value for t in DataType]}")
