"""Persistent app config (bucket, restore destination, last verify)."""
import json

from core.paths import DEFAULT_MAX_WORKERS, GUI_CONFIG_PATH


def load_config() -> dict:
    if GUI_CONFIG_PATH.exists():
        try:
            with open(GUI_CONFIG_PATH, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_config(config: dict) -> None:
    try:
        with open(GUI_CONFIG_PATH, "w") as f:
            json.dump(config, f)
    except Exception:
        pass


def update_config(**kwargs) -> dict:
    cfg = load_config()
    cfg.update(kwargs)
    save_config(cfg)
    return cfg


def get_bucket_name() -> str:
    return (load_config().get("bucket_name") or "").strip()


def get_restore_dest() -> str:
    return (load_config().get("restore_dest") or "").strip()


def set_restore_dest(path: str) -> None:
    update_config(restore_dest=path)


def set_bucket_name(name: str) -> None:
    update_config(bucket_name=name.strip())


def get_workers() -> int:
    raw = load_config().get("workers")
    try:
        return max(1, min(int(raw), 64))
    except (TypeError, ValueError):
        return DEFAULT_MAX_WORKERS


def set_workers(n: int) -> None:
    update_config(workers=max(1, min(int(n), 64)))


# --- Browser download limits ------------------------------------------------
# Defaults let a home vault hand over a few GB without touching Settings,
# but stop an accidental whole-drive export.

DEFAULT_DOWNLOAD_MAX_FILES = 1000
DEFAULT_DOWNLOAD_MAX_BYTES = 5 * 1024 ** 3
DEFAULT_DOWNLOAD_STREAM_THRESHOLD = 500 * 1024 ** 2
DEFAULT_DOWNLOAD_EXPIRY_HOURS = 24

_DOWNLOAD_LIMIT_KEYS = {
    "max_files": ("download_max_files", DEFAULT_DOWNLOAD_MAX_FILES, 1, 100000),
    "max_bytes": ("download_max_bytes", DEFAULT_DOWNLOAD_MAX_BYTES, 1, 1024 ** 5),
    "stream_threshold": (
        "download_stream_threshold",
        DEFAULT_DOWNLOAD_STREAM_THRESHOLD,
        0,
        1024 ** 5,
    ),
    "expiry_hours": ("download_expiry_hours", DEFAULT_DOWNLOAD_EXPIRY_HOURS, 1, 24 * 30),
}


def _get_download_int(kind: str) -> int:
    key, default, low, high = _DOWNLOAD_LIMIT_KEYS[kind]
    raw = load_config().get(key)
    try:
        return max(low, min(int(raw), high))
    except (TypeError, ValueError):
        return default


def _set_download_int(kind: str, value) -> int:
    key, default, low, high = _DOWNLOAD_LIMIT_KEYS[kind]
    try:
        clean = max(low, min(int(value), high))
    except (TypeError, ValueError):
        clean = default
    update_config(**{key: clean})
    return clean


def get_download_max_files() -> int:
    return _get_download_int("max_files")


def set_download_max_files(n) -> int:
    return _set_download_int("max_files", n)


def get_download_max_bytes() -> int:
    return _get_download_int("max_bytes")


def set_download_max_bytes(n) -> int:
    return _set_download_int("max_bytes", n)


def get_download_stream_threshold() -> int:
    return _get_download_int("stream_threshold")


def set_download_stream_threshold(n) -> int:
    return _set_download_int("stream_threshold", n)


def get_download_expiry_hours() -> int:
    return _get_download_int("expiry_hours")


def set_download_expiry_hours(n) -> int:
    return _set_download_int("expiry_hours", n)


def get_last_verify() -> dict:
    value = load_config().get("last_verify")
    return value if isinstance(value, dict) else {}


def set_last_verify(result: dict) -> None:
    update_config(last_verify=result)


def get_ingest_extra_roots() -> list[str]:
    value = load_config().get("ingest_extra_roots")
    if not isinstance(value, list):
        return []
    roots = []
    seen = set()
    for item in value:
        text = str(item).strip()
        if not text or text in seen:
            continue
        roots.append(text)
        seen.add(text)
    return roots


def set_ingest_extra_roots(paths: list[str]) -> None:
    cleaned = []
    seen = set()
    for item in paths:
        text = str(item).strip()
        if not text or text in seen:
            continue
        cleaned.append(text)
        seen.add(text)
    update_config(ingest_extra_roots=cleaned)


def add_ingest_extra_root(path: str) -> list[str]:
    roots = get_ingest_extra_roots()
    text = path.strip()
    if text and text not in roots:
        roots.append(text)
        set_ingest_extra_roots(roots)
    return roots


def remove_ingest_extra_root(path: str) -> list[str]:
    text = path.strip()
    roots = [item for item in get_ingest_extra_roots() if item != text]
    set_ingest_extra_roots(roots)
    return roots
