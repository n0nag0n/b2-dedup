"""Migration 006 — best-effort capture date for the library / timeline."""
import sqlite3


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def up(conn: sqlite3.Connection):
    cols = _columns(conn, "files")
    if "best_taken_at" not in cols:
        conn.execute("ALTER TABLE files ADD COLUMN best_taken_at TEXT")
    if "taken_at_source" not in cols:
        conn.execute("ALTER TABLE files ADD COLUMN taken_at_source TEXT")

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_files_best_taken_at ON files(best_taken_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_files_library "
        "ON files(file_type, searchable, is_original, best_taken_at)"
    )
