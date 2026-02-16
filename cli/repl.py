"""
MiniDB Interactive REPL
=======================
Interactive command-line shell with minidb> prompt.

Features:
  - Multi-line SQL with ; terminator
  - Meta-commands (dot-prefixed, no ; needed)
  - Ctrl+C: cancel current input OR interrupt running query
  - Ctrl+D/EOF: safe exit with active txn rollback
  - Persistent readline history (~/.minidb_history)
  - Error classification and display
"""

import os
import sys
import time
import threading
from typing import Optional

from cli.session import Session, SessionError
from cli.renderer import Renderer


# ─── History ────────────────────────────────────────────────────────
HISTORY_FILE = os.path.expanduser("~/.minidb_history")
HISTORY_MAX = 1000

try:
    import readline
    _HAS_READLINE = True
except ImportError:
    try:
        import pyreadline3 as readline
        _HAS_READLINE = True
    except ImportError:
        _HAS_READLINE = False


def _load_history():
    if _HAS_READLINE and os.path.exists(HISTORY_FILE):
        try:
            readline.read_history_file(HISTORY_FILE)
        except Exception:
            pass


def _save_history():
    if _HAS_READLINE:
        try:
            readline.set_history_length(HISTORY_MAX)
            readline.write_history_file(HISTORY_FILE)
        except Exception:
            pass


# ─── REPL ───────────────────────────────────────────────────────────

class REPL:
    """
    Interactive MiniDB shell.

    Usage:
        repl = REPL("path/to/db")
        repl.run()
    """

    PROMPT = "minidb> "
    CONTINUATION = "   ...> "

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.session: Optional[Session] = None
        self.renderer = Renderer()
        self._running = False

    def run(self):
        """Main REPL loop."""
        # Initialize session
        try:
            self.session = Session(self.db_path)
        except Exception as e:
            print(f"Error: failed to open database at '{self.db_path}': {e}", file=sys.stderr)
            return

        _load_history()
        self._running = True

        print(f"MiniDB v0.8.0")
        print(f"Database: {self.db_path}")
        recovery = self.session._recovery_stats
        if recovery.get("redo_count", 0) > 0 or recovery.get("undo_count", 0) > 0:
            print(f"Recovery: {recovery.get('redo_count', 0)} redo, "
                  f"{recovery.get('undo_count', 0)} undo")
        print(f'Type ".help" for usage hints.')
        print()

        sql_buffer = ""

        try:
            while self._running:
                # Determine prompt
                prompt = self.CONTINUATION if sql_buffer else self.PROMPT

                # Show txn indicator
                if self.session.active_txn_id is not None:
                    prompt = f"minidb[txn:{self.session.active_txn_id}]> "
                    if sql_buffer:
                        prompt = self.CONTINUATION

                try:
                    line = input(prompt)
                except KeyboardInterrupt:
                    # Ctrl+C: cancel current input
                    print()
                    sql_buffer = ""
                    continue
                except EOFError:
                    # Ctrl+D: exit
                    print()
                    break

                # Empty line
                stripped = line.strip()
                if not stripped:
                    if not sql_buffer:
                        continue
                    sql_buffer += "\n"
                    continue

                # Meta-commands (dot-prefixed, no ; needed)
                if not sql_buffer and stripped.startswith("."):
                    self._handle_meta_command(stripped)
                    continue

                # Accumulate SQL
                sql_buffer += " " + line if sql_buffer else line

                # Check for multi-statement input (split on ;)
                while ";" in sql_buffer:
                    idx = self._find_semicolon_outside_quotes(sql_buffer)
                    if idx == -1:
                        break
                    statement = sql_buffer[:idx].strip()
                    sql_buffer = sql_buffer[idx + 1:].strip()

                    if statement:
                        self._execute_statement(statement)

        finally:
            _save_history()
            self._shutdown()

    # ─── Statement Execution ────────────────────────────────────────

    def _execute_statement(self, sql: str):
        """Execute a single SQL statement with error handling."""
        try:
            rows, message, col_names = self.session.execute(sql)

            if rows is not None:
                # SELECT — stream results
                try:
                    count = self.renderer.render_rows(rows, col_names)
                except KeyboardInterrupt:
                    # Interrupt running query
                    self.session.cancel()
                    print("\nQuery interrupted.")
                    if self.session.autocommit and self.session.active_txn_id is not None:
                        self.session._rollback_implicit()
                        print("Implicit transaction rolled back.")
            elif message:
                self.renderer.render_message(message)

        except KeyboardInterrupt:
            print("\nQuery interrupted.")
            if self.session.autocommit and self.session.active_txn_id is not None:
                self.session._rollback_implicit()
                print("Implicit transaction rolled back.")
        except Exception as e:
            self.renderer.render_error(e)

    # ─── Meta-Commands ──────────────────────────────────────────────

    def _handle_meta_command(self, line: str):
        """Handle dot-prefixed meta-commands."""
        parts = line.split(None, 1)
        cmd = parts[0].lower()
        arg = parts[1].strip() if len(parts) > 1 else ""

        if cmd in (".quit", ".exit", ".q"):
            self._running = False
        elif cmd == ".help":
            self._cmd_help()
        elif cmd == ".tables":
            self._cmd_tables()
        elif cmd == ".schema":
            self._cmd_schema(arg)
        elif cmd == ".indexes":
            self._cmd_indexes(arg)
        elif cmd == ".mode":
            self._cmd_mode(arg)
        elif cmd == ".timer":
            self._cmd_timer(arg)
        elif cmd == ".headers":
            self._cmd_headers(arg)
        elif cmd == ".limit":
            self._cmd_limit(arg)
        elif cmd == ".stats":
            self._cmd_stats()
        else:
            print(f"Unknown command: {cmd}. Type .help for available commands.")

    def _cmd_help(self):
        print("""MiniDB Commands:
  .help                Show this help
  .tables              List all tables
  .schema [TABLE]      Show table schema (and indexes)
  .indexes [TABLE]     List indexes
  .mode table|vertical|raw  Set output mode (default: table)
  .timer on|off        Toggle query timing display
  .headers on|off      Toggle column headers
  .limit N|off         Set display row limit
  .stats               Show session statistics
  .quit                Exit (aliases: .exit, .q)

SQL Transaction Commands:
  BEGIN                Start explicit transaction
  COMMIT               Commit transaction
  ROLLBACK             Rollback transaction
  EXPLAIN [LOGICAL|PHYSICAL] <SQL>  Show query plan

Tips:
  - Statements end with ;
  - Multi-line input supported (continue until ;)
  - Ctrl+C cancels current input or running query
  - Ctrl+D exits the shell""")

    def _cmd_tables(self):
        tables = self.session.catalog.list_tables()
        if not tables:
            print("No tables.")
        else:
            for t in tables:
                print(f"  {t}")

    def _cmd_schema(self, table_name: str):
        if not table_name:
            # Show all tables with schemas
            tables = self.session.catalog.list_tables()
            if not tables:
                print("No tables.")
                return
            for t in tables:
                self._print_table_schema(t)
                print()
        else:
            schema = self.session.catalog.get_table_schema(table_name)
            if schema is None:
                print(f"Table '{table_name}' not found.")
            else:
                self._print_table_schema(table_name)

    def _print_table_schema(self, table_name: str):
        schema = self.session.catalog.get_table_schema(table_name)
        if schema is None:
            return
        print(f"Table: {table_name}")
        for col in schema.columns:
            null_str = "" if col.nullable else " NOT NULL"
            print(f"  {col.name:<20} {col.data_type.name:<10}{null_str}")

        # Show indexes
        indexes = self.session.catalog.get_indexes_for_table(table_name)
        if indexes:
            print(f"  Indexes:")
            for idx in indexes:
                print(f"    {idx.get('name', '?')} on ({', '.join(idx.get('columns', []))})")

    def _cmd_indexes(self, table_name: str = ""):
        if table_name:
            indexes = self.session.catalog.get_indexes_for_table(table_name)
        else:
            indexes = []
            for t in self.session.catalog.list_tables():
                for idx in self.session.catalog.get_indexes_for_table(t):
                    idx['_table'] = t
                    indexes.append(idx)

        if not indexes:
            print("No indexes.")
        else:
            for idx in indexes:
                tbl = idx.get('_table', idx.get('table', '?'))
                name = idx.get('name', '?')
                cols = ', '.join(idx.get('columns', []))
                print(f"  {name} on {tbl}({cols})")

    def _cmd_mode(self, arg: str):
        valid = ("table", "vertical", "raw")
        if arg.lower() in valid:
            self.renderer.mode = arg.lower()
            print(f"Output mode: {arg.lower()}")
        else:
            print(f"Usage: .mode {{{' | '.join(valid)}}}")
            print(f"Current: {self.renderer.mode}")

    def _cmd_timer(self, arg: str):
        if arg.lower() in ("on", "1", "true"):
            self.renderer.show_timer = True
            print("Timer ON")
        elif arg.lower() in ("off", "0", "false"):
            self.renderer.show_timer = False
            print("Timer OFF")
        else:
            print(f"Timer is {'ON' if self.renderer.show_timer else 'OFF'}")

    def _cmd_headers(self, arg: str):
        if arg.lower() in ("on", "1", "true"):
            self.renderer.show_headers = True
            print("Headers ON")
        elif arg.lower() in ("off", "0", "false"):
            self.renderer.show_headers = False
            print("Headers OFF")
        else:
            print(f"Headers are {'ON' if self.renderer.show_headers else 'OFF'}")

    def _cmd_limit(self, arg: str):
        if arg.lower() in ("off", "none", "0"):
            self.renderer.display_limit = None
            print("Display limit OFF")
        elif arg.isdigit() and int(arg) > 0:
            self.renderer.display_limit = int(arg)
            print(f"Display limit: {arg} rows")
        else:
            current = self.renderer.display_limit or "OFF"
            print(f"Usage: .limit N | .limit off")
            print(f"Current: {current}")

    def _cmd_stats(self):
        s = self.session.stats
        print("Session Statistics:")
        print(f"  Statements executed:    {s['statements_executed']}")
        print(f"  Transactions committed: {s['transactions_committed']}")
        print(f"  Transactions aborted:   {s['transactions_aborted']}")
        if self.session.active_txn_id:
            print(f"  Active transaction:     {self.session.active_txn_id}")
        print(f"  Autocommit:             {'ON' if self.session.autocommit else 'OFF'}")

    # ─── Helpers ────────────────────────────────────────────────────

    def _find_semicolon_outside_quotes(self, sql: str) -> int:
        """Find the first ; that isn't inside single quotes."""
        in_quote = False
        for i, ch in enumerate(sql):
            if ch == "'" and not in_quote:
                in_quote = True
            elif ch == "'" and in_quote:
                # Check for escaped quote ''
                if i + 1 < len(sql) and sql[i + 1] == "'":
                    continue
                in_quote = False
            elif ch == ";" and not in_quote:
                return i
        return -1

    def _shutdown(self):
        """Clean shutdown: close session, warn about active txn."""
        if self.session is not None:
            warning = self.session.close()
            if warning:
                print(warning, file=sys.stderr)
            print("Goodbye.")
