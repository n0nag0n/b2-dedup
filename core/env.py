"""Load a project-root .env without overriding real environment variables."""
from __future__ import annotations

import os
from pathlib import Path

_LOADED = False


def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def load_project_env(path: Path | None = None) -> None:
    """Read KEY=VALUE lines into os.environ if the key is not already set."""
    global _LOADED
    if path is None and _LOADED:
        return
    env_path = path or (project_root() / ".env")
    if path is None:
        _LOADED = True
    if not env_path.is_file():
        return
    try:
        text = env_path.read_text(encoding="utf-8")
    except OSError:
        return
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        os.environ[key] = value
