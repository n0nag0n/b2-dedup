"""Migration 004 — background jobs and the per-session restore set."""
import sqlite3


def up(conn: sqlite3.Connection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            payload_json TEXT,
            progress_done INTEGER NOT NULL DEFAULT 0,
            progress_total INTEGER NOT NULL DEFAULT 0,
            log_text TEXT,
            error TEXT,
            created_at TEXT,
            started_at TEXT,
            finished_at TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status, created_at)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS restore_set (
            session_key TEXT NOT NULL,
            file_id INTEGER NOT NULL,
            added_at TEXT,
            PRIMARY KEY (session_key, file_id)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_restore_set_file ON restore_set(file_id)")
