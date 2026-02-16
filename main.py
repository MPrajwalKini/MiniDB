"""
MiniDB — Lightweight Relational Database Engine
================================================
Entry point for the database system.

Usage:
    python main.py [options] [database_path]

Options:
    --help              Show help
    --execute SQL       Execute single SQL statement and exit
    --file PATH         Execute SQL script file and exit

Default:
    Interactive REPL mode with database at ./minidb_data
"""

import sys
import os


def print_help():
    print("""
MiniDB — Lightweight Relational Database Engine

Usage:
    python main.py [database_path]               Interactive REPL
    python main.py --execute "SQL" [db_path]      Execute single statement
    python main.py --file script.sql [db_path]    Execute SQL script

Options:
    --help          Show this help
    --execute SQL   Execute SQL and exit
    --file PATH     Execute SQL script and exit
    database_path   Path to database directory (default: ./minidb_data)

Meta-Commands (REPL only):
    .help           Command reference
    .tables         List tables
    .schema [T]     Show schema
    .indexes [T]    List indexes
    .mode M         Set output mode (table/vertical/raw)
    .timer on|off   Toggle timing
    .stats          Session statistics
    .quit           Exit
""")


def execute_single(db_path: str, sql: str):
    """Execute a single SQL statement and exit."""
    from cli.session import Session
    from cli.renderer import Renderer

    renderer = Renderer()

    with Session(db_path) as session:
        try:
            rows, message, col_names = session.execute(sql)
            if rows is not None:
                renderer.render_rows(rows, col_names)
            elif message:
                renderer.render_message(message)
        except Exception as e:
            renderer.render_error(e)
            sys.exit(1)


def execute_script(db_path: str, script_path: str):
    """
    Execute a SQL script file and exit.

    Semantics: each statement autocommit unless explicit BEGIN.
    Meta-commands (.tables etc.) are supported in scripts.
    Errors stop execution.
    """
    from cli.session import Session
    from cli.renderer import Renderer

    if not os.path.isfile(script_path):
        print(f"Error: script file not found: {script_path}", file=sys.stderr)
        sys.exit(1)

    with open(script_path, "r", encoding="utf-8") as f:
        content = f.read()

    renderer = Renderer()
    renderer.show_timer = False  # Cleaner script output

    with Session(db_path) as session:
        # Split on ; but respect quotes
        statements = _split_statements(content)

        for stmt_text in statements:
            stmt_text = stmt_text.strip()
            if not stmt_text:
                continue
            # Skip comment-only lines
            if stmt_text.startswith("--"):
                continue

            # Meta-commands in scripts
            if stmt_text.startswith("."):
                print(f"-- meta-command not supported in script mode: {stmt_text}",
                      file=sys.stderr)
                continue

            try:
                rows, message, col_names = session.execute(stmt_text)
                if rows is not None:
                    renderer.render_rows(rows, col_names)
                elif message:
                    renderer.render_message(message)
            except Exception as e:
                renderer.render_error(e)
                print(f"Error in statement: {stmt_text[:80]}...", file=sys.stderr)
                sys.exit(1)


def _split_statements(content: str):
    """Split SQL content on ; outside of single quotes."""
    statements = []
    current = []
    in_quote = False

    for ch in content:
        if ch == "'" and not in_quote:
            in_quote = True
            current.append(ch)
        elif ch == "'" and in_quote:
            in_quote = False
            current.append(ch)
        elif ch == ";" and not in_quote:
            statements.append("".join(current))
            current = []
        else:
            current.append(ch)

    # Last segment (no trailing ;)
    remaining = "".join(current).strip()
    if remaining:
        statements.append(remaining)

    return statements


def main() -> None:
    """Parse CLI arguments and dispatch."""
    args = sys.argv[1:]

    if "--help" in args or "-h" in args:
        print_help()
        return

    db_path = None
    execute_sql = None
    script_file = None

    i = 0
    while i < len(args):
        if args[i] == "--execute" and i + 1 < len(args):
            execute_sql = args[i + 1]
            i += 2
        elif args[i] == "--file" and i + 1 < len(args):
            script_file = args[i + 1]
            i += 2
        elif args[i].startswith("-"):
            print(f"Unknown option: {args[i]}", file=sys.stderr)
            print_help()
            sys.exit(1)
        else:
            db_path = args[i]
            i += 1

    if db_path is None:
        db_path = os.path.join(os.getcwd(), "minidb_data")

    if execute_sql:
        execute_single(db_path, execute_sql)
    elif script_file:
        execute_script(db_path, script_file)
    else:
        # Interactive REPL
        from cli.repl import REPL
        repl = REPL(db_path)
        repl.run()


if __name__ == "__main__":
    main()
