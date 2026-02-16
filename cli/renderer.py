"""
MiniDB Result Renderer
======================
Formats query results as aligned ASCII tables.

Features:
  - Streaming: prints rows as they arrive (no full materialization)
  - Auto-column-width with configurable max
  - NULL displayed distinctly
  - Row count + elapsed time footer
  - DML/DDL message rendering
  - Modes: table, vertical, raw
  - Configurable: headers, timer, display limit
"""

import sys
import time
from typing import Any, Dict, Iterator, List, Optional, TextIO


class Renderer:
    """
    Streaming result renderer with configurable display modes.
    """

    def __init__(self, output: TextIO = None):
        self.output = output or sys.stdout
        self.mode: str = "table"        # table, vertical, raw
        self.show_headers: bool = True
        self.show_timer: bool = True
        self.display_limit: Optional[int] = None  # None = no limit
        self.max_col_width: int = 50

    # ─── Public API ─────────────────────────────────────────────────

    def render_rows(self, rows: Iterator, column_names: Optional[List[str]] = None) -> int:
        """
        Render query results. Streams rows from iterator.
        Returns number of rows rendered.

        Strategy for table mode:
          - Buffer first N rows to determine column widths
          - Then stream remaining rows using those widths
          - This balances alignment quality with streaming
        """
        start = time.perf_counter()
        count = 0

        if self.mode == "raw":
            count = self._render_raw(rows, column_names)
        elif self.mode == "vertical":
            count = self._render_vertical(rows, column_names)
        else:
            count = self._render_table(rows, column_names)

        elapsed = time.perf_counter() - start

        if self.show_timer:
            self._print(f"\n{count} row(s) returned ({elapsed:.3f}s)")
        else:
            self._print(f"\n{count} row(s) returned")

        return count

    def render_message(self, message: str):
        """Render a non-query result message (DML, DDL, transaction control)."""
        if message:
            self._print(message)

    def render_error(self, error: Exception):
        """Render an error with classification prefix."""
        error_type = type(error).__name__
        # Classify known error types
        prefix = self._classify_error(error_type)
        self._print(f"{prefix}: {error}")

    # ─── Table Mode (streaming with width sampling) ─────────────────

    def _render_table(self, rows: Iterator, column_names: Optional[List[str]]) -> int:
        """
        Render rows in aligned table format.
        Buffers first batch to determine column widths, then streams.
        """
        # Buffer first batch (up to 100 rows) for width calculation
        buffer = []
        sample_size = 100
        headers = column_names or []
        first_row = True

        for row in rows:
            if self.display_limit is not None and len(buffer) >= self.display_limit:
                # We've hit the display limit during buffering
                break
            vals = self._extract_values(row)
            if first_row and not headers:
                headers = list(vals.keys())
                first_row = False
            buffer.append(vals)
            if len(buffer) >= sample_size:
                break

        if not buffer and not headers:
            return 0

        if first_row and not headers and buffer:
            headers = list(buffer[0].keys())

        # Calculate column widths from buffer
        widths = self._calculate_widths(headers, buffer)

        # Print header
        if self.show_headers:
            self._print_table_separator(widths, headers)
            self._print_table_row(widths, headers, {h: h for h in headers})
            self._print_table_separator(widths, headers)

        # Print buffered rows
        count = 0
        for vals in buffer:
            self._print_table_row(widths, headers, vals)
            count += 1

        # Stream remaining rows
        for row in rows:
            if self.display_limit is not None and count >= self.display_limit:
                self._print(f"... (display limit {self.display_limit} reached)")
                break
            vals = self._extract_values(row)
            self._print_table_row(widths, headers, vals)
            count += 1

        # Footer separator
        if self.show_headers and count > 0:
            self._print_table_separator(widths, headers)

        return count

    def _calculate_widths(self, headers: List[str], rows: List[Dict]) -> Dict[str, int]:
        """Calculate column widths from headers and sample rows."""
        widths = {}
        for h in headers:
            widths[h] = min(len(h), self.max_col_width)

        for row in rows:
            for h in headers:
                val = self._format_value(row.get(h))
                widths[h] = max(widths[h], min(len(val), self.max_col_width))

        return widths

    def _print_table_separator(self, widths: Dict[str, int], headers: List[str]):
        """Print +----+------+ separator line."""
        parts = ["+"]
        for h in headers:
            parts.append("-" * (widths[h] + 2) + "+")
        self._print("".join(parts))

    def _print_table_row(self, widths: Dict[str, int], headers: List[str], vals: Dict):
        """Print | col1 | col2 | row."""
        parts = ["|"]
        for h in headers:
            val_str = self._format_value(vals.get(h))
            if len(val_str) > self.max_col_width:
                val_str = val_str[:self.max_col_width - 3] + "..."
            w = widths[h]
            # Right-align numbers, left-align strings
            raw_val = vals.get(h)
            if isinstance(raw_val, (int, float)) and raw_val is not None:
                parts.append(f" {val_str:>{w}} |")
            else:
                parts.append(f" {val_str:<{w}} |")
        self._print("".join(parts))

    # ─── Vertical Mode ──────────────────────────────────────────────

    def _render_vertical(self, rows: Iterator, column_names: Optional[List[str]]) -> int:
        """Render each row as key: value pairs."""
        count = 0
        headers = column_names or []
        first_row = True

        for row in rows:
            if self.display_limit is not None and count >= self.display_limit:
                self._print(f"... (display limit {self.display_limit} reached)")
                break
            vals = self._extract_values(row)
            if first_row and not headers:
                headers = list(vals.keys())
                first_row = False

            count += 1
            self._print(f"*** Row {count} ***")
            max_key_len = max(len(h) for h in headers) if headers else 0
            for h in headers:
                val_str = self._format_value(vals.get(h))
                self._print(f"  {h:>{max_key_len}}: {val_str}")

        return count

    # ─── Raw Mode ───────────────────────────────────────────────────

    def _render_raw(self, rows: Iterator, column_names: Optional[List[str]]) -> int:
        """Render values separated by pipes, no formatting."""
        count = 0
        headers = column_names or []
        first_row = True

        for row in rows:
            if self.display_limit is not None and count >= self.display_limit:
                break
            vals = self._extract_values(row)
            if first_row and not headers:
                headers = list(vals.keys())
                if self.show_headers:
                    self._print("|".join(headers))
                first_row = False
            parts = [self._format_value(vals.get(h)) for h in headers]
            self._print("|".join(parts))
            count += 1

        return count

    # ─── Helpers ────────────────────────────────────────────────────

    def _extract_values(self, row) -> Dict[str, Any]:
        """Extract values dict from an ExecutionRow or dict."""
        if hasattr(row, 'values'):
            return row.values
        if isinstance(row, dict):
            return row
        return {"?": str(row)}

    def _format_value(self, value) -> str:
        """Format a single value for display."""
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, float):
            # Avoid unnecessary decimal places
            if value == int(value):
                return str(int(value))
            return f"{value:.6g}"
        return str(value)

    def _classify_error(self, error_type: str) -> str:
        """Map error class name to user-friendly prefix."""
        mapping = {
            "ParseError": "SyntaxError",
            "SyntaxError": "SyntaxError",
            "SessionError": "TransactionError",
            "RuntimeError": "ExecutionError",
            "ValueError": "ExecutionError",
            "KeyError": "ExecutionError",
            "TypeError": "ExecutionError",
            "KeyboardInterrupt": "Interrupted",
        }
        return mapping.get(error_type, f"Error[{error_type}]")

    def _print(self, text: str):
        """Print a line to the output stream."""
        print(text, file=self.output)
