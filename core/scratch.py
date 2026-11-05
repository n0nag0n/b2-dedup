"""Browser-upload staging for the Scratch drive.

Dropped files land in ``data/scratch/<batch>/`` and are then handed to the
normal ingest job. That means hashing, junk rules, dedup pointers, and catalog
rows behave exactly as they do for a plugged-in disk.

A successful ingest removes its staged batch. A batch with per-file errors is
kept so it can be retried, and old batches expire after the configured number
of days.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import BinaryIO

from core.catalog import format_size
from core.junk import is_ingest_skip
from core.paths import SCRATCH_DIR

MIN_FREE_BYTES = 1024 ** 3  # keep at least 1 GiB free on the data disk
COPY_CHUNK = 1024 * 1024
INCOMING_DIR_NAME = ".incoming"
BATCH_RE = re.compile(r"^\d{8}-\d{6}-[0-9a-f]{8}$")
MAX_REL_PATH = 1024


class ScratchError(ValueError):
    """An upload path or batch that cannot be accepted."""


class ScratchDiskFull(ScratchError):
    """Not enough free space to stage another upload."""


def new_batch_id() -> str:
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"{stamp}-{uuid.uuid4().hex[:8]}"


def batch_dir(batch_id: str, *, must_exist: bool = False) -> Path | None:
    """Return the folder for a batch id, or None when the id is not ours."""
    text = (batch_id or "").strip()
    if not BATCH_RE.fullmatch(text):
        return None
    path = SCRATCH_DIR / text
    if must_exist and not path.is_dir():
        return None
    return path


def sanitize_rel_path(raw: str | Path) -> str:
    """Normalize a browser-supplied relative path, rejecting escapes.

    Accepts ``photos/2024/a.jpg`` and Windows-style separators. Rejects
    absolute paths, ``..`` segments, NUL bytes, and overly long paths.
    """
    text = str(raw or "").replace("\\", "/").strip()
    if not text:
        raise ScratchError("A file path is required.")
    if text.startswith("/") or text.startswith("~"):
        raise ScratchError("Absolute paths are not allowed.")
    # Some Windows browsers include a drive prefix.
    if re.match(r"^[A-Za-z]:", text):
        text = text[2:]
        if text.startswith("/"):
            text = text[1:]
    parts: list[str] = []
    for part in text.split("/"):
        part = part.strip()
        if part in ("", "."):
            continue
        if part == "..":
            raise ScratchError("Parent-folder paths are not allowed.")
        if "\x00" in part:
            raise ScratchError("That path contains an invalid character.")
        if len(part) > 255:
            raise ScratchError("That file name is too long.")
        parts.append(part)
    if not parts:
        raise ScratchError("A file path is required.")
    clean = "/".join(parts)
    if len(clean) > MAX_REL_PATH:
        raise ScratchError("That path is too long.")
    return clean


def join_prefix(prefix: str, rel_path: str) -> str:
    """Combine an optional Scratch subfolder with the uploaded relative path."""
    prefix = (prefix or "").strip().strip("/")
    rel_path = (rel_path or "").strip()
    if not prefix:
        return sanitize_rel_path(rel_path)
    return sanitize_rel_path(f"{prefix}/{rel_path}")


def _incoming_dir() -> Path:
    path = SCRATCH_DIR / INCOMING_DIR_NAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def save_upload(
    stream: BinaryIO,
    batch_id: str,
    rel_path: str | Path,
    last_modified_ms: str | int | None = None,
) -> dict:
    """Copy one uploaded file into its batch. Returns a small result dict.

    Junk paths are reported as skipped and never written. Data goes to a
    temporary file outside the batch first, then is renamed into place so a
    half-finished upload can never be ingested.
    """
    rel = sanitize_rel_path(rel_path)
    if is_ingest_skip(rel):
        return {"path": rel, "bytes": 0, "skipped": True}

    batch = batch_dir(batch_id)
    if batch is None:
        raise ScratchError("That upload batch is not valid. Reload the page and try again.")

    target = batch.joinpath(*PurePosixPath(rel).parts)
    batch.mkdir(parents=True, exist_ok=True)
    target.parent.mkdir(parents=True, exist_ok=True)

    free = shutil.disk_usage(batch).free
    if free <= MIN_FREE_BYTES:
        raise ScratchDiskFull(
            "The data disk is nearly full. Free some space, then try the upload again."
        )
    budget = free - MIN_FREE_BYTES

    tmp = _incoming_dir() / f"{uuid.uuid4().hex}.part"
    written = 0
    try:
        with open(tmp, "wb") as handle:
            while True:
                chunk = stream.read(COPY_CHUNK)
                if not chunk:
                    break
                written += len(chunk)
                if written > budget:
                    raise ScratchDiskFull(
                        "That upload would leave less than 1 GiB free on the data disk."
                    )
                handle.write(chunk)
        os.replace(tmp, target)
    except Exception:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise

    _apply_client_mtime(target, last_modified_ms)
    return {"path": rel, "bytes": written, "skipped": False}


def _apply_client_mtime(target: Path, last_modified_ms) -> None:
    """Keep the browser's file date so incremental ingest and dates work."""
    try:
        mtime = float(last_modified_ms) / 1000.0
    except (TypeError, ValueError):
        return
    now = time.time()
    if not (0 < mtime <= now + 86400):
        return
    try:
        os.utime(target, (now, mtime))
    except OSError:
        pass


def _batch_files(batch: Path) -> tuple[int, int]:
    files = 0
    total = 0
    for root, _dirs, names in os.walk(batch):
        for name in names:
            path = Path(root) / name
            try:
                if not path.is_file() or path.is_symlink():
                    continue
                total += path.stat().st_size
            except OSError:
                continue
            files += 1
    return files, total


def batch_stats(batch_id: str) -> dict:
    batch = batch_dir(batch_id, must_exist=True)
    if batch is None:
        return {
            "id": (batch_id or "").strip(),
            "path": "",
            "files": 0,
            "bytes": 0,
            "bytes_fmt": format_size(0),
            "when": "",
        }
    files, total = _batch_files(batch)
    try:
        mtime = batch.stat().st_mtime
        when = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M")
    except OSError:
        when = ""
    return {
        "id": batch.name,
        "path": str(batch),
        "files": files,
        "bytes": total,
        "bytes_fmt": format_size(total),
        "when": when,
    }


def _active_scratch_sources() -> set[str]:
    """Staged folders that a pending/running ingest job is reading right now."""
    try:
        from core.db import connect

        conn = connect(readonly=True)
        try:
            rows = conn.execute(
                "SELECT payload_json FROM jobs "
                "WHERE type = 'ingest' AND status IN ('pending', 'running')"
            ).fetchall()
        finally:
            conn.close()
    except Exception:
        return set()
    sources: set[str] = set()
    for row in rows:
        try:
            payload = json.loads(row["payload_json"] or "{}")
        except (TypeError, json.JSONDecodeError):
            continue
        source = payload.get("source")
        if not source:
            continue
        try:
            sources.add(str(Path(source).resolve()))
        except Exception:
            continue
    return sources


def batch_is_active(batch_id: str) -> bool:
    batch = batch_dir(batch_id, must_exist=True)
    if batch is None:
        return False
    try:
        return str(batch.resolve()) in _active_scratch_sources()
    except Exception:
        return False


def list_batches() -> list[dict]:
    if not SCRATCH_DIR.is_dir():
        return []
    active = _active_scratch_sources()
    batches = []
    try:
        entries = list(SCRATCH_DIR.iterdir())
    except OSError:
        return []
    for entry in entries:
        if not entry.is_dir() or not BATCH_RE.fullmatch(entry.name):
            continue
        try:
            mtime = entry.stat().st_mtime
        except OSError:
            continue
        stats = batch_stats(entry.name)
        stats["mtime"] = mtime
        try:
            stats["active"] = str(entry.resolve()) in active
        except Exception:
            stats["active"] = False
        batches.append(stats)
    batches.sort(key=lambda item: item["mtime"], reverse=True)
    return batches


def scratch_usage() -> dict:
    batches = list_batches()
    files = sum(item["files"] for item in batches)
    total = sum(item["bytes"] for item in batches)
    try:
        free = shutil.disk_usage(SCRATCH_DIR if SCRATCH_DIR.is_dir() else SCRATCH_DIR.parent).free
    except OSError:
        free = 0
    return {
        "path": str(SCRATCH_DIR),
        "batches": len(batches),
        "files": files,
        "bytes": total,
        "bytes_fmt": format_size(total),
        "free": free,
        "free_fmt": format_size(free) if free else "",
    }


def cleanup_batch(batch_id: str, *, allow_active: bool = False) -> bool:
    batch = batch_dir(batch_id, must_exist=True)
    if batch is None:
        return False
    if not allow_active:
        try:
            if str(batch.resolve()) in _active_scratch_sources():
                return False
        except Exception:
            return False
    try:
        shutil.rmtree(batch)
    except OSError:
        return False
    return not batch.exists()


def clear_batches() -> int:
    """Remove every staged batch. Returns how many were removed."""
    removed = 0
    for batch in list_batches():
        if cleanup_batch(batch["id"]):
            removed += 1
    return removed


def is_scratch_path(path: str | Path) -> bool:
    """True only for paths strictly inside the staging root."""
    try:
        resolved = Path(path).resolve()
        root = SCRATCH_DIR.resolve()
        resolved.relative_to(root)
    except (OSError, RuntimeError, ValueError):
        return False
    return resolved != root


def cleanup_after_ingest(source: str | Path) -> bool:
    """Delete a staged batch after a successful ingest. Never touches disks."""
    source = Path(source)
    if not is_scratch_path(source):
        return False
    try:
        shutil.rmtree(source)
    except OSError:
        return False
    return not source.exists()


def cleanup_stale_batches(days: int | None = None) -> int:
    """Expire old staged batches and abandoned partial uploads."""
    if days is None:
        from core.config import get_scratch_retention_days

        days = get_scratch_retention_days()
    removed = 0
    if not SCRATCH_DIR.is_dir():
        return removed
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    try:
        entries = list(SCRATCH_DIR.iterdir())
    except OSError:
        return removed
    for entry in entries:
        if not entry.is_dir() or not BATCH_RE.fullmatch(entry.name):
            continue
        try:
            mtime = datetime.fromtimestamp(entry.stat().st_mtime, tz=timezone.utc)
        except OSError:
            continue
        if mtime < cutoff:
            shutil.rmtree(entry, ignore_errors=True)
            removed += 1

    # Abandoned .part files never reached a batch; they are safe to remove
    # after a day even if a batch is still inside its retention window.
    incoming = SCRATCH_DIR / INCOMING_DIR_NAME
    if incoming.is_dir():
        partial_cutoff = time.time() - 86400
        for part in incoming.glob("*.part"):
            try:
                if part.stat().st_mtime < partial_cutoff:
                    part.unlink(missing_ok=True)
            except OSError:
                continue
    return removed
