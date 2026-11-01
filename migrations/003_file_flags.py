"""Migration 003 — junk / searchable flags on files."""
import sqlite3

from migrations.file_flags import apply_flag_updates


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def up(conn: sqlite3.Connection):
    cols = _columns(conn, "files")
    if "is_junk" not in cols:
        conn.execute("ALTER TABLE files ADD COLUMN is_junk INTEGER NOT NULL DEFAULT 0")
    if "searchable" not in cols:
        conn.execute("ALTER TABLE files ADD COLUMN searchable INTEGER NOT NULL DEFAULT 1")

    apply_flag_updates(conn)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_files_searchable ON files(searchable)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_files_hash ON files(hash)")
