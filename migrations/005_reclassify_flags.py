"""Migration 005 — reclassify junk flags after the LIKE '_' wildcard bug."""
import sqlite3

from migrations.file_flags import apply_flag_updates


def up(conn: sqlite3.Connection):
    conn.execute("UPDATE files SET is_junk = 0, searchable = 1")
    apply_flag_updates(conn)
