"""
MiniDB Tuple Serializer
=======================
Schema-aware serialization of row data to/from binary format.
Handles null bitmap, fixed-size and variable-length fields.

Tuple binary layout:
  [tuple_len: 2B] [null_bitmap: ceil(ncols/8) B] [flags: 2B] [column data...]

Teaching note:
  PostgreSQL uses a similar null bitmap approach. The bitmap saves space
  by avoiding storing data for NULL columns. Snowflake stores columnar
  data with separate null vectors per column — a fundamentally different
  approach optimized for analytics.
"""

import math
import struct
from typing import Any

from storage.schema import Schema
from storage.types import DataType, serialize_value, deserialize_value


def _null_bitmap_size(num_columns: int) -> int:
    """Number of bytes needed for the null bitmap."""
    return math.ceil(num_columns / 8)


def serialize_row(row: list[Any], schema: Schema) -> bytes:
    """
    Serialize a row of values into binary tuple format.

    Layout:
      - tuple_len  (2 bytes, uint16) — total length of the serialized tuple
      - null_bitmap (ceil(ncols/8) bytes) — 1 bit per column, bit set = NULL
      - flags      (2 bytes, uint16) — reserved for future use
      - column data — serialized values in schema order, NULLs skipped

    Returns the complete tuple bytes.
    """
    ncols = schema.column_count
    bmp_size = _null_bitmap_size(ncols)

    # Build null bitmap and column data
    null_bitmap = bytearray(bmp_size)
    col_data = bytearray()

    for i, (col, val) in enumerate(zip(schema.columns, row)):
        if val is None:
            # Set bit i in the bitmap
            byte_idx = i // 8
            bit_idx = i % 8
            null_bitmap[byte_idx] |= (1 << bit_idx)
        else:
            col_data.extend(serialize_value(val, col.data_type))

    # Flags: 0 for now (reserved)
    flags = 0

    # Header: tuple_len (2B) + null_bitmap + flags (2B)
    header_size = 2 + bmp_size + 2
    total_len = header_size + len(col_data)

    result = bytearray()
    result.extend(struct.pack(">H", total_len))
    result.extend(null_bitmap)
    result.extend(struct.pack(">H", flags))
    result.extend(col_data)

    return bytes(result)


def deserialize_row(data: bytes, schema: Schema, offset: int = 0) -> tuple[list[Any], int]:
    """
    Deserialize a row from binary tuple format at the given offset.

    Returns:
      (values_list, new_offset)
    """
    ncols = schema.column_count
    bmp_size = _null_bitmap_size(ncols)

    # Read tuple_len
    tuple_len = struct.unpack_from(">H", data, offset)[0]
    start_offset = offset
    offset += 2

    # Read null bitmap
    null_bitmap = data[offset:offset + bmp_size]
    offset += bmp_size

    # Read flags (ignored for now)
    _flags = struct.unpack_from(">H", data, offset)[0]
    offset += 2

    # Read column values
    values: list[Any] = []
    for i, col in enumerate(schema.columns):
        byte_idx = i // 8
        bit_idx = i % 8
        is_null = (null_bitmap[byte_idx] >> bit_idx) & 1

        if is_null:
            values.append(None)
        else:
            val, offset = deserialize_value(data, offset, col.data_type)
            values.append(val)

    return values, start_offset + tuple_len


def serialized_row_size(row: list[Any], schema: Schema) -> int:
    """Calculate the serialized size of a row without actually serializing."""
    ncols = schema.column_count
    bmp_size = _null_bitmap_size(ncols)
    header_size = 2 + bmp_size + 2  # tuple_len + bitmap + flags

    data_size = 0
    for col, val in zip(schema.columns, row):
        if val is not None:
            data_size += len(serialize_value(val, col.data_type))

    return header_size + data_size
