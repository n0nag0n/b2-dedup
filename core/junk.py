"""Ingest-skip vs search-hide rules for noise files.

Two policies (see .notes/overhaul-plan.md):
- Ingest skip: never hash/upload (node_modules, venv, Mac resource forks, …).
- Search hide: keep in the backup (including .git/.hg) but hide from default search.

Existing catalog rows are classified the same way so Home/Search can hide
AppleDouble and package-manager trees already ingested.
"""
from __future__ import annotations

from pathlib import Path

# Directory names that should never be uploaded (any path segment).
INGEST_SKIP_DIR_NAMES = frozenset({
    "node_modules",
    "venv",
    ".venv",
    "vendor",
    "__pycache__",
    ".Spotlight-V100",
    ".Trashes",
    ".fseventsd",
    "__MACOSX",
})

# Exact file basenames that should never be uploaded.
INGEST_SKIP_FILE_NAMES = frozenset({
    ".DS_Store",
    "Thumbs.db",
    "desktop.ini",
})

# Version-control trees stay in the backup so a restore is complete.
# They are hidden from default search only.
SEARCH_HIDE_DIR_NAMES = frozenset({
    ".git",
    ".hg",
})


def _path_parts(rel_path: str | Path) -> list[str]:
    return Path(str(rel_path).replace("\\", "/")).parts


def _basename(rel_path: str | Path) -> str:
    name = Path(str(rel_path).replace("\\", "/")).name
    return name


def is_appledouble(rel_path: str | Path) -> bool:
    """True for AppleDouble resource forks (`._file` next to `file`)."""
    return _basename(rel_path).startswith("._")


def is_ingest_skip(rel_path: str | Path) -> bool:
    """True if this relative path should not be hashed or uploaded."""
    parts = _path_parts(rel_path)
    if not parts:
        return False
    if any(part in INGEST_SKIP_DIR_NAMES for part in parts[:-1]):
        return True
    name = parts[-1]
    if name in INGEST_SKIP_FILE_NAMES:
        return True
    if name.startswith("._"):
        return True
    if name.startswith(".Spotlight-"):
        return True
    return False


def is_search_hidden(rel_path: str | Path) -> bool:
    """True if default search should omit this path.

    Includes ingest-skip noise plus version-control trees that we still restore.
    """
    if is_ingest_skip(rel_path):
        return True
    parts = _path_parts(rel_path)
    return any(part in SEARCH_HIDE_DIR_NAMES for part in parts)


def classify_path(rel_path: str | Path) -> dict:
    """Return flags used by catalog queries and ingest."""
    skip = is_ingest_skip(rel_path)
    hide = is_search_hidden(rel_path)
    return {
        "is_junk": skip,
        "searchable": not hide,
        "ingest_skip": skip,
    }
