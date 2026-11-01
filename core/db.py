"""SQLite connections and schema init."""
import sqlite3
import threading

from core.paths import DB_PATH

thread_local = threading.local()


def get_thread_connection() -> sqlite3.Connection:
    """Get or create a SQLite connection for the current thread."""
    if not hasattr(thread_local, "connection"):
        thread_local.connection = sqlite3.connect(DB_PATH)
    return thread_local.connection


def connect(readonly: bool = False) -> sqlite3.Connection:
    """Open a short-lived connection. Caller must close it."""
    if readonly:
        uri = f"file:{DB_PATH}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
    else:
        conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 5000")
    if not readonly:
        conn.execute("PRAGMA journal_mode = WAL")
    return conn


def init_db() -> None:
    """Initialize / migrate the database schema (run once from main thread)."""
    from migrations.runner import run_migrations

    conn = sqlite3.connect(DB_PATH)
    try:
        run_migrations(conn)
    finally:
        conn.close()
