"""Migration runner — applies pending migrations in order against the live DB.

Usage (called from b2_dedup.init_db):
    from migrations.runner import run_migrations
    run_migrations(conn)

Each migration module (001_xxx.py, 002_xxx.py, …) must expose:
    def up(conn: sqlite3.Connection) -> None

The runner tracks applied migrations in a `schema_migrations` table so each
migration runs exactly once, even on an existing database that pre-dates
versioning (it stamps migration 001 as already applied if the `files` table
already exists, so the CREATE IF NOT EXISTS calls are harmless but the stamp
prevents re-running).
"""
import importlib
import importlib.util
import sqlite3
import sys
from pathlib import Path

MIGRATIONS_DIR = Path(__file__).parent


def _bootstrap_migrations_table(conn: sqlite3.Connection):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version     TEXT PRIMARY KEY,
            applied_at  TEXT NOT NULL DEFAULT (datetime('now'))
        )
    ''')
    conn.commit()


def _applied_versions(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT version FROM schema_migrations").fetchall()
    return {r[0] for r in rows}


def _mark_applied(conn: sqlite3.Connection, version: str):
    conn.execute(
        "INSERT OR IGNORE INTO schema_migrations (version) VALUES (?)", (version,)
    )
    conn.commit()


def _load_migration(path: Path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def run_migrations(conn: sqlite3.Connection):
    """Apply all pending migrations in numeric order."""
    _bootstrap_migrations_table(conn)
    applied = _applied_versions(conn)

    migration_files = sorted(MIGRATIONS_DIR.glob("[0-9][0-9][0-9]_*.py"))
    for path in migration_files:
        version = path.stem  # e.g. "001_initial"
        if version in applied:
            continue
        print(f"  Applying migration: {version}")
        mod = _load_migration(path)
        mod.up(conn)
        conn.commit()
        _mark_applied(conn, version)
        print(f"  ✓ {version} applied.")
