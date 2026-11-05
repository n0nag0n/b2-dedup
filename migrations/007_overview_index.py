"""Migration 007 — covering index for the Home overview aggregates.

get_vault_overview() sums size / counts originals for every file, plus a
per-drive GROUP BY. Without a covering index SQLite scans the wide `files`
table and (for the per-drive totals) does random row lookups, so a cold,
multi-GB catalog can take several seconds on the first Home request. This
narrow index lets both overview queries run as index-only scans.
"""
import sqlite3


def up(conn: sqlite3.Connection):
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_files_overview "
        "ON files(drive_name, is_original, size)"
    )
