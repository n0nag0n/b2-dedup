"""Migration 008 — drop indexes that duplicate the surviving indexes.

Verified with PRAGMA index_xinfo and EXPLAIN QUERY PLAN against a copy of the
live catalog: every query that used these keeps an equivalent plan on the
survivor.

- idx_files_drive_path (drive_name, file_path) is a byte-for-byte duplicate of
  the UNIQUE(drive_name, file_path) autoindex.
- idx_files_original (hash, is_original) is identical to
  idx_files_hash_original.
- idx_files_hash (hash) is the leftmost prefix of idx_files_hash_original;
  every hash lookup in the app is an equality lookup.

idx_files_file_type is also a leftmost prefix of idx_files_library, but it is
kept on purpose: it is 16 MB vs 59 MB and get_file_types() does a DISTINCT
scan of it on every Search/Library page (~3.5x faster when cold).

DROP INDEX only frees pages for reuse; run VACUUM if you want the file to
shrink on disk.
"""
import sqlite3


def _index_columns(conn: sqlite3.Connection, name: str) -> tuple[str, ...]:
    rows = conn.execute(
        "SELECT name FROM pragma_index_xinfo(?) WHERE key = 1 ORDER BY seqno",
        (name,),
    ).fetchall()
    return tuple(r[0] for r in rows)


def _has_index(conn: sqlite3.Connection, name: str, columns: tuple[str, ...]) -> bool:
    return _index_columns(conn, name) == columns


def _has_unique_index(conn: sqlite3.Connection, columns: tuple[str, ...]) -> bool:
    rows = conn.execute(
        'SELECT name FROM pragma_index_list(?) WHERE "unique" = 1',
        ("files",),
    ).fetchall()
    return any(_has_index(conn, r[0], columns) for r in rows)


def up(conn: sqlite3.Connection):
    # Keep the old index only if the UNIQUE constraint is somehow absent
    # (a pre-versioning catalog that never got the constraint).
    if _has_unique_index(conn, ("drive_name", "file_path")):
        conn.execute("DROP INDEX IF EXISTS idx_files_drive_path")

    if _has_index(conn, "idx_files_hash_original", ("hash", "is_original")):
        conn.execute("DROP INDEX IF EXISTS idx_files_original")
        conn.execute("DROP INDEX IF EXISTS idx_files_hash")
