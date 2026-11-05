"""Persistent app config (bucket, restore destination, last verify)."""
import json
from pathlib import Path

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


# --- Scratch drives (browser uploads) ---------------------------------------
# Several named drives can receive browser drops. The selected name is the
# one the Scratch page uses until the user picks another. Staging files live
# in scratch_staging_dir when set, otherwise data/scratch.

DEFAULT_SCRATCH_DRIVE_NAME = "Scratch"
DEFAULT_SCRATCH_RETENTION_DAYS = 7
_STAGING_HISTORY_LIMIT = 8


def normalize_drive_name(name: str) -> str | None:
    """A catalog drive name: no slashes, no control characters."""
    text = (name or "").strip()
    if not text or len(text) > 80:
        return None
    if "/" in text or "\\" in text:
        return None
    if any(ord(ch) < 32 for ch in text):
        return None
    return text


def get_scratch_drives() -> list[str]:
    """Names the Scratch page can ingest into, oldest addition first.

    A config written before this list existed has only ``scratch_drive_name``.
    That single name stays the whole list so a renamed drive is not joined
    by a new default called Scratch.
    """
    cfg = load_config()
    raw = cfg.get("scratch_drives")
    names: list[str] = []
    seen: set[str] = set()
    if isinstance(raw, list):
        for item in raw:
            name = normalize_drive_name(str(item))
            if not name or name.lower() in seen:
                continue
            names.append(name)
            seen.add(name.lower())
    legacy = normalize_drive_name(str(cfg.get("scratch_drive_name") or ""))
    if names:
        if legacy and legacy.lower() not in seen:
            names.insert(0, legacy)
        return names
    if legacy:
        return [legacy]
    return [DEFAULT_SCRATCH_DRIVE_NAME]


def get_scratch_drive_name() -> str:
    """The scratch drive selected for the next upload."""
    names = get_scratch_drives()
    selected = normalize_drive_name(str(load_config().get("scratch_drive_name") or ""))
    if selected:
        for name in names:
            if name.lower() == selected.lower():
                return name
    return names[0]


def set_scratch_drive_name(name: str) -> str:
    """Add ``name`` to the scratch drives if needed, and select it.

    An invalid name leaves the current selection unchanged.
    """
    text = normalize_drive_name(name)
    if not text:
        return get_scratch_drive_name()
    names = get_scratch_drives()
    canonical = text
    found = False
    for existing in names:
        if existing.lower() == text.lower():
            canonical = existing
            found = True
            break
    if not found:
        names.append(text)
    update_config(scratch_drives=names, scratch_drive_name=canonical)
    return canonical


def remove_scratch_drive(name: str) -> list[str]:
    """Drop a name from the list. The last remaining drive is kept."""
    text = normalize_drive_name(name)
    names = get_scratch_drives()
    if not text or len(names) <= 1:
        return names
    kept = [item for item in names if item.lower() != text.lower()]
    if len(kept) == len(names):
        return names
    if not kept:
        kept = [DEFAULT_SCRATCH_DRIVE_NAME]
    selected = get_scratch_drive_name()
    if selected.lower() == text.lower() or selected not in kept:
        selected = kept[0]
    update_config(scratch_drives=kept, scratch_drive_name=selected)
    return kept


def get_scratch_staging_dir() -> str:
    """Absolute staging folder from config, or ``""`` to use the default."""
    return (load_config().get("scratch_staging_dir") or "").strip()


def get_scratch_staging_history() -> list[str]:
    """Earlier staging folders, so batches left behind can still be finished."""
    raw = load_config().get("scratch_staging_history")
    if not isinstance(raw, list):
        return []
    paths: list[str] = []
    seen: set[str] = set()
    for item in raw:
        text = str(item).strip()
        if not text or text in seen:
            continue
        if not Path(text).expanduser().is_absolute():
            continue
        paths.append(text)
        seen.add(text)
    return paths


def set_scratch_staging_dir(path: str) -> str:
    """Remember ``path`` as the folder new uploads are staged in."""
    text = (path or "").strip()
    previous = get_scratch_staging_dir()
    history = get_scratch_staging_history()
    if previous and previous != text and previous not in history:
        history.append(previous)
    history = [item for item in history if item != text][-_STAGING_HISTORY_LIMIT:]
    update_config(scratch_staging_dir=text, scratch_staging_history=history)
    return text


def get_scratch_retention_days() -> int:
    raw = load_config().get("scratch_retention_days")
    try:
        return max(1, min(int(raw), 365))
    except (TypeError, ValueError):
        return DEFAULT_SCRATCH_RETENTION_DAYS


def set_scratch_retention_days(days) -> int:
    try:
        clean = max(1, min(int(days), 365))
    except (TypeError, ValueError):
        clean = DEFAULT_SCRATCH_RETENTION_DAYS
    update_config(scratch_retention_days=clean)
    return clean


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
