"""
MiniDB Key Encoding
===================
Order-preserving binary encoding for B-Tree index keys.
All encoded keys can be compared via memcmp (byte-by-byte)
and the result matches the SQL ordering of the original values.

Encoding rules:
  INT     → XOR sign bit + big-endian int32 (4 bytes)
  FLOAT   → IEEE 754 sortable transform (8 bytes)
            NaN is NOT indexable (treated like NULL — rejected).
            +0 and -0 normalize to the same encoding.
  STRING  → UTF-8 bytes + 0x00 terminator.
            Embedded 0x00 in strings escaped as 0x00 0x01.
            Terminator is 0x00 0x00.
  BOOLEAN → 0x00 (False) / 0x01 (True) (1 byte)
  DATE    → Same as INT (days since epoch, 4 bytes)

Concurrency: single-writer assumed.
"""

import math
import struct
from datetime import date, datetime
from typing import Any, Tuple

from storage.types import DataType


# ─── Epoch for DATE ─────────────────────────────────────────────────────────
_DATE_EPOCH = date(1970, 1, 1)


# ─── Encode ─────────────────────────────────────────────────────────────────

def encode_key(value: Any, dtype: DataType) -> bytes:
    """
    Encode a Python value to an order-preserving binary key.

    Raises ValueError if value is None (NULLs not indexed)
    or if value is NaN (not indexable).
    """
    if value is None:
        raise ValueError("NULL values cannot be indexed")

    if dtype == DataType.INT:
        return _encode_int(int(value))

    elif dtype == DataType.FLOAT:
        fval = float(value)
        if math.isnan(fval):
            raise ValueError("NaN values cannot be indexed")
        # Normalize -0.0 to +0.0
        if fval == 0.0:
            fval = 0.0
        return _encode_float(fval)

    elif dtype == DataType.STRING:
        return _encode_string(str(value))

    elif dtype == DataType.BOOLEAN:
        return b"\x01" if value else b"\x00"

    elif dtype == DataType.DATE:
        if isinstance(value, str):
            value = datetime.strptime(value, "%Y-%m-%d").date()
        days = (value - _DATE_EPOCH).days
        return _encode_int(days)

    raise ValueError(f"Unsupported key type: {dtype}")


def _encode_int(val: int) -> bytes:
    """
    INT encoding: XOR the sign bit of a big-endian int32.
    This maps: MIN_INT → 0x00000000, 0 → 0x80000000, MAX_INT → 0xFFFFFFFF.
    Binary order == numeric order.
    """
    raw = struct.pack(">i", val)
    # XOR the sign bit (first byte, bit 7)
    return bytes([raw[0] ^ 0x80, raw[1], raw[2], raw[3]])


def _encode_float(val: float) -> bytes:
    """
    FLOAT encoding: IEEE 754 sortable transform.
    1. Pack as big-endian double (8 bytes).
    2. If positive (sign bit 0): flip sign bit → positives sort after negatives.
    3. If negative (sign bit 1): flip ALL bits → negatives sort correctly
       (more negative = smaller binary value).
    """
    raw = bytearray(struct.pack(">d", val))
    if raw[0] & 0x80:
        # Negative: flip all bits
        for i in range(8):
            raw[i] ^= 0xFF
    else:
        # Positive (or +0): flip sign bit only
        raw[0] ^= 0x80
    return bytes(raw)


def _encode_string(val: str) -> bytes:
    """
    STRING encoding: UTF-8 bytes with null-byte escaping + terminator.
    - Every 0x00 byte in the UTF-8 data is escaped as 0x00 0x01.
    - The string is terminated with 0x00 0x00.
    This preserves lexicographic order under memcmp.
    """
    utf8 = val.encode("utf-8")
    result = bytearray()
    for b in utf8:
        if b == 0x00:
            result.append(0x00)
            result.append(0x01)
        else:
            result.append(b)
    # Terminator
    result.append(0x00)
    result.append(0x00)
    return bytes(result)


# ─── Decode ─────────────────────────────────────────────────────────────────

def decode_key(data: bytes, offset: int, dtype: DataType) -> Tuple[Any, int]:
    """
    Decode a key from binary at the given offset.
    Returns (value, new_offset).
    """
    if dtype == DataType.INT:
        val, new_off = _decode_int(data, offset)
        return val, new_off

    elif dtype == DataType.FLOAT:
        val, new_off = _decode_float(data, offset)
        return val, new_off

    elif dtype == DataType.STRING:
        val, new_off = _decode_string(data, offset)
        return val, new_off

    elif dtype == DataType.BOOLEAN:
        return data[offset] != 0, offset + 1

    elif dtype == DataType.DATE:
        days, new_off = _decode_int(data, offset)
        dt = date.fromordinal(_DATE_EPOCH.toordinal() + days)
        return dt, new_off

    raise ValueError(f"Unsupported key type: {dtype}")


def _decode_int(data: bytes, offset: int) -> Tuple[int, int]:
    """Reverse the XOR sign-bit transform and unpack int32."""
    b = bytearray(data[offset:offset + 4])
    b[0] ^= 0x80
    val = struct.unpack(">i", bytes(b))[0]
    return val, offset + 4


def _decode_float(data: bytes, offset: int) -> Tuple[float, int]:
    """Reverse the IEEE sortable transform and unpack double."""
    raw = bytearray(data[offset:offset + 8])
    if raw[0] & 0x80:
        # Was positive: flip sign bit back
        raw[0] ^= 0x80
    else:
        # Was negative: flip all bits back
        for i in range(8):
            raw[i] ^= 0xFF
    val = struct.unpack(">d", bytes(raw))[0]
    return val, offset + 8


def _decode_string(data: bytes, offset: int) -> Tuple[str, int]:
    """
    Decode a null-terminated escaped string.
    0x00 0x01 → literal 0x00
    0x00 0x00 → end of string
    """
    result = bytearray()
    i = offset
    while i < len(data):
        b = data[i]
        if b == 0x00:
            # Look at next byte
            next_b = data[i + 1]
            if next_b == 0x00:
                # Terminator
                i += 2
                break
            elif next_b == 0x01:
                # Escaped null byte
                result.append(0x00)
                i += 2
            else:
                raise ValueError(f"Invalid escape sequence 0x00 0x{next_b:02X} at offset {i}")
        else:
            result.append(b)
            i += 1
    return result.decode("utf-8"), i


# ─── Key Size ───────────────────────────────────────────────────────────────

def encoded_key_size(value: Any, dtype: DataType) -> int:
    """Return the byte size of the encoded key."""
    return len(encode_key(value, dtype))


def fixed_key_size(dtype: DataType) -> int:
    """
    Return fixed key size for fixed-size types.
    Returns -1 for variable-size types (STRING).
    """
    sizes = {
        DataType.INT: 4,
        DataType.FLOAT: 8,
        DataType.BOOLEAN: 1,
        DataType.DATE: 4,
    }
    return sizes.get(dtype, -1)
