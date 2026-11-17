"""SQLite connections and schema init."""
import sqlite3
import threading
import time

from core.paths import DB_PATH

thread_local = threading.local()
# Ingest commits from several workers plus the job-log writer. One in-process
# writer at a time, so a slow fsync cannot expire the others' busy timeout.
_write_lock = threading.Lock()
BUSY_TIMEOUT_MS = 30000
BUSY_TIMEOUT_S = BUSY_TIMEOUT_MS / 1000
LOCK_RETRIES = 5


def _apply_pragmas(conn: sqlite3.Connection, *, readonly: bool) -> None:
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    if readonly:
        return
    conn.execute("PRAGMA journal_mode = WAL")
    # FULL fsync on a busy disk held the write lock long enough for the other
    # ingest workers to give up. WAL + NORMAL still flushes on checkpoint.
    conn.execute("PRAGMA synchronous = NORMAL")


def get_thread_connection() -> sqlite3.Connection:
    """Get or create a SQLite connection for the current thread."""
    if not hasattr(thread_local, "connection"):
        conn = sqlite3.connect(DB_PATH, timeout=BUSY_TIMEOUT_S)
        _apply_pragmas(conn, readonly=False)
        thread_local.connection = conn
    return thread_local.connection


def connect(readonly: bool = False) -> sqlite3.Connection:
    """Open a short-lived connection. Caller must close it."""
    if readonly:
        uri = f"file:{DB_PATH}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=BUSY_TIMEOUT_S)
    else:
        conn = sqlite3.connect(DB_PATH, timeout=BUSY_TIMEOUT_S)
    conn.row_factory = sqlite3.Row
    _apply_pragmas(conn, readonly=readonly)
    return conn


def _rollback(conn: sqlite3.Connection) -> None:
    try:
        conn.rollback()
    except sqlite3.Error:
        pass


def commit_exclusive(conn: sqlite3.Connection, fn):
    """Run fn() as the only in-process write, then commit.

    A locked write used to leave the worker connection inside a transaction.
    The next file on that thread then failed immediately with
    "database is locked". Roll back and retry on that error.
    """
    last = None
    for attempt in range(LOCK_RETRIES):
        with _write_lock:
            try:
                if conn.in_transaction:
                    conn.rollback()
                result = fn()
                conn.commit()
                return result
            except sqlite3.OperationalError as exc:
                _rollback(conn)
                last = exc
                if "locked" not in str(exc).lower() or attempt == LOCK_RETRIES - 1:
                    raise
            except Exception:
                _rollback(conn)
                raise
        time.sleep(0.05 * (attempt + 1))
    raise last


def init_db() -> None:
    """Initialize / migrate the database schema (run once from main thread)."""
    from migrations.runner import run_migrations

    conn = sqlite3.connect(DB_PATH, timeout=BUSY_TIMEOUT_S)
    try:
        _apply_pragmas(conn, readonly=False)
        run_migrations(conn)
    finally:
        conn.close()
