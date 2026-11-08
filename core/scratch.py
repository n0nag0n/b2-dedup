"""Browser-upload staging for scratch drives.

Dropped files land in the configured staging folder (``data/scratch/<batch>/``
unless a different folder was chosen) and are then handed to the normal ingest
job. Hashing, junk rules, dedup pointers, and catalog rows behave exactly as
they do for a plugged-in disk.

A successful ingest removes its staged batch. A batch with per-file errors is
kept so it can be retried, and old batches expire after the configured number
of days. Batches left in a previous staging folder stay visible so they can
still be ingested or deleted.
"""
from __future__ import annotations

import fcntl
import hashlib
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

MIN_FREE_BYTES = 1024 ** 3  # keep at least 1 GiB free on the staging disk
COPY_CHUNK = 1024 * 1024
# The reverse proxy in front of StorageInsights is expected to allow 256 MB
# per request (nginx: `client_max_body_size 256m`). Pieces stay under that so
# the multipart wrapper still fits. Larger files are assembled from pieces.
MAX_CHUNK_BYTES = 248 * 1024 * 1024
INCOMING_DIR_NAME = ".incoming"
BATCH_RE = re.compile(r"^\d{8}-\d{6}-[0-9a-f]{8}$")
MAX_REL_PATH = 1024
BROWSE_LIMIT = 300


class ScratchError(ValueError):
    """An upload path or batch that cannot be accepted."""


class ScratchDiskFull(ScratchError):
    """Not enough free space to stage another upload.

    ``fatal`` means the disk itself is too full to keep going. A single file
    that does not fit is not fatal: the rest of the folder still can.
    """

    def __init__(self, message: str, *, fatal: bool = True):
        super().__init__(message)
        self.fatal = fatal


class ScratchConflict(ScratchError):
    """A piece arrived at a byte offset the partial file is not waiting for."""

    def __init__(self, received: int):
        super().__init__(
            "That piece does not continue the file. "
            "Send the next piece from the byte the server already has."
        )
        self.received = received


def new_batch_id() -> str:
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"{stamp}-{uuid.uuid4().hex[:8]}"


def default_vault_prefix() -> str:
    """Folder inside the scratch drive for the next upload.

    The built-in ``data/scratch`` directory stays at the top of the drive.
    A folder the user picked contributes its own name, so staging in
    ``.../asus-backup`` is stored as ``Scratch/asus-backup/…``.
    """
    root = staging_root()
    try:
        if root.resolve() == Path(SCRATCH_DIR).resolve():
            return ""
    except OSError:
        return ""
    name = root.name.strip()
    if not name or name in {".", ".."}:
        return ""
    try:
        return sanitize_rel_path(name)
    except ScratchError:
        return ""


def staging_root() -> Path:
    """Folder that receives new uploads.

    An empty config uses ``SCRATCH_DIR`` (``data/scratch``). Tests patch that
    constant; a saved absolute path overrides it.
    """
    from core.config import get_scratch_staging_dir

    raw = get_scratch_staging_dir()
    if raw:
        path = Path(raw).expanduser()
        if path.is_absolute():
            try:
                return path.resolve()
            except OSError:
                return path
    return SCRATCH_DIR


def scratch_roots() -> list[Path]:
    """Staging folders whose batches this app is allowed to list or delete.

    Includes the active folder, the default ``data/scratch``, and folders the
    user used before changing the setting. Cleanup never walks outside these.
    """
    from core.config import get_scratch_staging_history

    candidates = [staging_root(), SCRATCH_DIR]
    for item in get_scratch_staging_history():
        candidates.append(Path(item).expanduser())
    roots: list[Path] = []
    seen: set[str] = set()
    for path in candidates:
        try:
            if not path.is_absolute():
                continue
            resolved = path.resolve()
        except OSError:
            continue
        key = str(resolved)
        if key in seen:
            continue
        seen.add(key)
        roots.append(resolved)
    return roots


def batch_dir(batch_id: str, *, must_exist: bool = False) -> Path | None:
    """Return the folder for a batch id, or None when the id is not ours.

    New batches are created in the active staging folder. Existing batches are
    found in any folder Scratch has used, so a settings change does not hide
    an upload that still needs to be ingested.
    """
    text = (batch_id or "").strip()
    if not BATCH_RE.fullmatch(text):
        return None
    if must_exist:
        for root in scratch_roots():
            path = root / text
            if path.is_dir():
                return path
        return None
    return staging_root() / text


def resolve_browse_dir(raw: str) -> Path:
    """Resolve a folder the staging browser is allowed to list."""
    text = (raw or "").strip()
    if not text:
        raise ScratchError("Choose a folder.")
    path = Path(text).expanduser()
    try:
        resolved = path.resolve()
    except OSError as exc:
        raise ScratchError("Could not open that folder.") from exc
    if not resolved.is_dir():
        raise ScratchError("That path is not a folder on this computer.")
    return resolved


def validate_staging_dir(raw: str) -> Path:
    """Accept a host folder as the place new Scratch uploads are stored."""
    resolved = resolve_browse_dir(raw)
    from core.volumes import is_forbidden_root

    if is_forbidden_root(resolved):
        raise ScratchError("Choose a folder inside a disk, not a system directory.")
    if not os.access(resolved, os.W_OK | os.X_OK):
        raise ScratchError("That folder is not writable by StorageInsights.")
    return resolved


def list_child_dirs(path: Path) -> tuple[list[dict], bool]:
    """Subfolders of ``path`` for the staging browser. Skips symlinks."""
    try:
        children = list(path.iterdir())
    except OSError as exc:
        raise ScratchError("That folder could not be listed.") from exc
    children.sort(key=lambda item: item.name.lower())
    found: list[dict] = []
    truncated = False
    try:
        current = path.resolve()
    except OSError:
        current = path
    for child in children:
        if len(found) >= BROWSE_LIMIT:
            truncated = True
            break
        try:
            if child.is_symlink():
                target = child.resolve()
                if not target.is_dir() or target == current:
                    continue
                found.append({"name": child.name or str(child), "path": str(target)})
                continue
            if not child.is_dir():
                continue
            found.append({"name": child.name or str(child), "path": str(child.resolve())})
        except OSError:
            continue
    return found, truncated


def folder_crumbs(path: Path) -> list[dict]:
    parts = path.parts
    if not parts:
        return []
    crumbs = []
    acc = Path(parts[0])
    crumbs.append({"label": parts[0], "path": str(acc)})
    for part in parts[1:]:
        acc = acc / part
        crumbs.append({"label": part, "path": str(acc)})
    return crumbs


def parent_dir(path: Path) -> str:
    parent = path.parent
    if parent == path:
        return ""
    return str(parent)


def create_child_dir(parent: Path, name: str) -> Path:
    """Create one folder inside ``parent`` for the staging browser."""
    from core.volumes import is_forbidden_root

    text = (name or "").strip()
    if (
        not text
        or text in {".", ".."}
        or "/" in text
        or "\\" in text
        or "\x00" in text
    ):
        raise ScratchError("Use a folder name without slashes.")
    if len(text) > 255:
        raise ScratchError("That folder name is too long.")
    parent = resolve_browse_dir(str(parent))
    if is_forbidden_root(parent):
        raise ScratchError("Create the folder inside a disk, not in a system directory.")
    if not os.access(parent, os.W_OK | os.X_OK):
        raise ScratchError("That folder is not writable.")
    dest = parent / text
    try:
        dest.relative_to(parent)
    except ValueError as exc:
        raise ScratchError("That folder name is not allowed.") from exc
    try:
        dest.mkdir(parents=False, exist_ok=True)
    except OSError as exc:
        raise ScratchError(f"Could not create that folder: {exc}") from exc
    if not dest.is_dir():
        raise ScratchError("Could not create that folder.")
    return dest.resolve()


def staging_is_ready(path: Path | None = None) -> bool:
    """True when new uploads can be created in the active staging folder."""
    root = path or staging_root()
    try:
        if root.is_dir():
            return os.access(root, os.W_OK | os.X_OK)
        parent = root.parent
        return parent.is_dir() and os.access(parent, os.W_OK | os.X_OK)
    except OSError:
        return False


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
    path = staging_root() / INCOMING_DIR_NAME
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
            "The staging disk is nearly full. Free some space, then try the upload again."
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
                        "That upload would leave less than 1 GiB free on the staging disk."
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


def _parse_byte_count(value, label: str) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise ScratchError(f"That upload {label} is not valid.") from None
    if number < 0:
        raise ScratchError(f"That upload {label} is not valid.")
    return number


def _free_bytes(probe: Path) -> int:
    current = probe
    if not current.exists():
        current = current.parent
    return shutil.disk_usage(current).free


def _partial_path(batch_id: str, rel: str) -> Path:
    digest = hashlib.sha256(rel.encode("utf-8")).hexdigest()
    return _incoming_dir() / batch_id / f"{digest}.part"


def discard_partials(batch_id: str) -> None:
    """Remove unfinished pieces for one batch. Finished files are left alone."""
    text = (batch_id or "").strip()
    if not BATCH_RE.fullmatch(text):
        return
    for root in scratch_roots():
        incoming = root / INCOMING_DIR_NAME / text
        if incoming.is_dir():
            shutil.rmtree(incoming, ignore_errors=True)


def write_chunk(
    stream: BinaryIO,
    batch_id: str,
    rel_path: str | Path,
    offset,
    total,
    last_modified_ms: str | int | None = None,
) -> dict:
    """Append one piece of a large upload, then publish the file when complete.

    Pieces live under ``.incoming`` until the last byte arrives, so a half
    finished video is never ingested. The piece must continue exactly where
    the previous piece stopped.
    """
    rel = sanitize_rel_path(rel_path)
    if is_ingest_skip(rel):
        return {
            "path": rel,
            "bytes": 0,
            "received": 0,
            "skipped": True,
            "complete": True,
        }

    batch = batch_dir(batch_id)
    if batch is None:
        raise ScratchError("That upload batch is not valid. Reload the page and try again.")

    offset = _parse_byte_count(offset, "offset")
    total = _parse_byte_count(total, "size")
    if offset > total:
        raise ScratchError("That upload piece does not line up with the file.")

    payload = stream.read(MAX_CHUNK_BYTES + 1)
    if not payload:
        payload = b""
    if len(payload) > MAX_CHUNK_BYTES:
        raise ScratchError("That piece is too large. Send a smaller piece and try again.")
    if offset + len(payload) > total:
        raise ScratchError("That piece runs past the end of the file.")
    if not payload and offset != total:
        raise ScratchError("That piece was empty.")

    free = _free_bytes(staging_root())
    if free <= MIN_FREE_BYTES:
        raise ScratchDiskFull(
            "The staging disk is nearly full. Free some space, then try the upload again."
        )
    budget = free - MIN_FREE_BYTES
    if offset == 0 and total > budget:
        raise ScratchDiskFull(
            f"This file is {format_size(total)} and only {format_size(budget)} "
            "can be used on the staging disk, so it was left out.",
            fatal=False,
        )
    if len(payload) > budget:
        raise ScratchDiskFull(
            "That upload would leave less than 1 GiB free on the staging disk."
        )

    part = _partial_path(batch_id, rel)
    part.parent.mkdir(parents=True, exist_ok=True)
    part.touch(exist_ok=True)
    with open(part, "r+b") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        if offset == 0:
            handle.seek(0)
            handle.truncate(0)
        else:
            handle.seek(0, os.SEEK_END)
            current = handle.tell()
            if current != offset:
                raise ScratchConflict(current)
        handle.seek(offset)
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
        received = offset + len(payload)
        if received != total:
            return {
                "path": rel,
                "bytes": received,
                "received": received,
                "skipped": False,
                "complete": False,
            }
        target = batch.joinpath(*PurePosixPath(rel).parts)
        batch.mkdir(parents=True, exist_ok=True)
        target.parent.mkdir(parents=True, exist_ok=True)
        os.replace(part, target)

    _apply_client_mtime(target, last_modified_ms)
    return {
        "path": rel,
        "bytes": received,
        "received": received,
        "skipped": False,
        "complete": True,
    }


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
    active = _active_scratch_sources()
    batches = []
    seen: set[str] = set()
    for root in scratch_roots():
        if not root.is_dir():
            continue
        try:
            entries = list(root.iterdir())
        except OSError:
            continue
        for entry in entries:
            if not entry.is_dir() or not BATCH_RE.fullmatch(entry.name):
                continue
            try:
                resolved = entry.resolve()
                mtime = entry.stat().st_mtime
            except OSError:
                continue
            key = str(resolved)
            if key in seen:
                continue
            seen.add(key)
            files, total = _batch_files(entry)
            batches.append({
                "id": entry.name,
                "path": str(resolved),
                "files": files,
                "bytes": total,
                "bytes_fmt": format_size(total),
                "when": datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M"),
                "mtime": mtime,
                "active": key in active,
            })
    batches.sort(key=lambda item: item["mtime"], reverse=True)
    return batches


def scratch_usage() -> dict:
    batches = list_batches()
    files = sum(item["files"] for item in batches)
    total = sum(item["bytes"] for item in batches)
    root = staging_root()
    try:
        probe = root if root.is_dir() else root.parent
        free = shutil.disk_usage(probe).free
    except OSError:
        free = 0
    return {
        "path": str(root),
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
    if batch.exists():
        return False
    discard_partials(batch.name)
    return True


def clear_batches() -> int:
    """Remove every staged batch. Returns how many were removed."""
    removed = 0
    for batch in list_batches():
        if cleanup_batch(batch["id"]):
            removed += 1
    return removed


def is_scratch_path(path: str | Path) -> bool:
    """True only for paths strictly inside a staging folder Scratch has used."""
    try:
        resolved = Path(path).resolve()
    except (OSError, RuntimeError):
        return False
    for root in scratch_roots():
        try:
            resolved.relative_to(root)
        except ValueError:
            continue
        if resolved != root:
            return True
    return False


def cleanup_after_ingest(source: str | Path) -> bool:
    """Delete a staged batch after a successful ingest. Never touches disks."""
    source = Path(source)
    if not is_scratch_path(source):
        return False
    batch_id = source.name
    try:
        shutil.rmtree(source)
    except OSError:
        return False
    if source.exists():
        return False
    discard_partials(batch_id)
    return True


def cleanup_stale_batches(days: int | None = None) -> int:
    """Expire old staged batches and abandoned partial uploads."""
    if days is None:
        from core.config import get_scratch_retention_days

        days = get_scratch_retention_days()
    removed = 0
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    partial_cutoff = time.time() - 86400
    for root in scratch_roots():
        if not root.is_dir():
            continue
        try:
            entries = list(root.iterdir())
        except OSError:
            continue
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

        # Abandoned pieces never reached a batch; they are safe to remove
        # after a day even if a batch is still inside its retention window.
        incoming = root / INCOMING_DIR_NAME
        if not incoming.is_dir():
            continue
        for part in incoming.rglob("*"):
            if not part.is_file() or part.suffix not in {".part", ".lock"}:
                continue
            try:
                if part.stat().st_mtime < partial_cutoff:
                    part.unlink(missing_ok=True)
            except OSError:
                continue
        for directory in sorted(incoming.rglob("*"), reverse=True):
            if not directory.is_dir():
                continue
            try:
                directory.rmdir()
            except OSError:
                continue
    return removed
