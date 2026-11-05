"""Filesystem locations and shared constants."""
import os
from pathlib import Path

from core.env import load_project_env

load_project_env()

DATA_DIR = Path(os.environ.get("B2_DEDUP_DATA_DIR", Path(__file__).resolve().parent.parent / "data"))
DATA_DIR.mkdir(exist_ok=True)

DB_PATH = DATA_DIR / "b2_dedup.db"
THUMBS_DIR = DATA_DIR / "thumbs"
EXPORTS_DIR = DATA_DIR / "exports"
CACHE_PATH = DATA_DIR / ".b2_dedup_cache.json"
GUI_CONFIG_PATH = DATA_DIR / "b2_gui_config.json"

DEFAULT_MAX_WORKERS = 10
CHUNK_SIZE = 4 * 1024 * 1024  # 4MB
FILE_COUNT_CACHE_DAYS = 7
POINTER_EXTENSION = ".b2ptr"
DB_REMOTE_PATH = "__b2_dedup_metadata__/b2_dedup.db"
DB_REMOTE_PREV_PATH = "__b2_dedup_metadata__/b2_dedup.prev.db"
