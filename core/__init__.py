"""StorageInsights core: catalog, B2, ingest, restore.

Public names used by the CLI, the web app, and the legacy Streamlit UI
are re-exported here so callers can `from core import B2Manager, DB_PATH`.
"""
from core.b2 import B2Manager, sanitize_b2_path
from core.db import get_thread_connection, init_db
from core.hash import sha256_file
from core.paths import (
    CACHE_PATH,
    CHUNK_SIZE,
    DATA_DIR,
    DB_PATH,
    DB_REMOTE_PATH,
    DEFAULT_MAX_WORKERS,
    FILE_COUNT_CACHE_DAYS,
    POINTER_EXTENSION,
)
from core.pointers import create_pointer_content

# Legacy alias used by gui/config.py
_DATA_DIR = DATA_DIR

__all__ = [
    "B2Manager",
    "CACHE_PATH",
    "CHUNK_SIZE",
    "DATA_DIR",
    "DB_PATH",
    "DB_REMOTE_PATH",
    "DEFAULT_MAX_WORKERS",
    "FILE_COUNT_CACHE_DAYS",
    "POINTER_EXTENSION",
    "_DATA_DIR",
    "create_pointer_content",
    "get_thread_connection",
    "init_db",
    "sanitize_b2_path",
    "sha256_file",
]
