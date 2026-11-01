"""Catalog snapshot and B2 backup / restore of the SQLite database."""
from __future__ import annotations

import os
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from core.b2 import B2Manager
from core.catalog import format_size
from core.config import get_bucket_name, load_config, update_config
from core.paths import DATA_DIR, DB_PATH, DB_REMOTE_PATH, DB_REMOTE_PREV_PATH

SNAPSHOT_NAME = "b2_dedup.snapshot.db"
INCOMING_NAME = "b2_dedup.incoming.db"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fmt_ts(ts: float | int | None, *, ms: bool = False) -> str:
    if ts is None:
        return ""
    try:
        seconds = float(ts) / 1000.0 if ms else float(ts)
        return datetime.fromtimestamp(seconds).strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError, OSError):
        return ""


def local_db_stat() -> dict:
    if not DB_PATH.exists():
        return {"exists": False, "size": 0, "mtime": 0.0, "size_fmt": "0 B", "mtime_fmt": ""}
    st = DB_PATH.stat()
    return {
        "exists": True,
        "size": int(st.st_size),
        "mtime": float(st.st_mtime),
        "size_fmt": format_size(st.st_size),
        "mtime_fmt": _fmt_ts(st.st_mtime),
    }


def backup_status() -> dict:
    """Local-only view of catalog backup health (no B2 call)."""
    cfg = load_config()
    local = local_db_stat()
    backed_mtime = cfg.get("db_backup_local_mtime")
    backed_size = cfg.get("db_backup_local_size")
    remote_size = cfg.get("db_backup_remote_size")
    remote_ts_ms = cfg.get("db_backup_remote_ts_ms")
    checked_at = cfg.get("db_backup_checked_at") or ""

    if not get_bucket_name():
        state = "no_bucket"
        summary = "Set a B2 bucket before backing up the catalog."
    elif backed_mtime is None:
        state = "never"
        summary = "The catalog has not been copied to B2 from this app yet."
    elif not local["exists"]:
        state = "missing"
        summary = "The local catalog file is missing."
    elif local["mtime"] > float(backed_mtime) + 1 or local["size"] != int(backed_size or 0):
        state = "stale"
        summary = "Local catalog has changed since the last backup."
    else:
        state = "ok"
        summary = "Local catalog matches the last backup snapshot."

    return {
        "state": state,
        "summary": summary,
        "local": local,
        "backed_mtime": backed_mtime,
        "backed_size": int(backed_size or 0) if backed_size is not None else None,
        "backed_size_fmt": format_size(backed_size) if backed_size is not None else "",
        "backed_mtime_fmt": _fmt_ts(backed_mtime),
        "remote_size": remote_size,
        "remote_size_fmt": format_size(remote_size) if remote_size else "",
        "remote_ts_ms": remote_ts_ms,
        "remote_ts_fmt": _fmt_ts(remote_ts_ms, ms=True),
        "checked_at": checked_at,
        "checked_at_fmt": (checked_at or "")[:19].replace("T", " "),
        "remote_path": DB_REMOTE_PATH,
        "bucket": get_bucket_name(),
    }


def record_remote_info(info: dict | None) -> None:
    if info:
        update_config(
            db_backup_remote_size=int(info.get("size") or 0),
            db_backup_remote_ts_ms=int(info.get("upload_timestamp_ms") or 0),
            db_backup_checked_at=_now(),
        )
    else:
        update_config(
            db_backup_remote_size=None,
            db_backup_remote_ts_ms=None,
            db_backup_checked_at=_now(),
        )


def refresh_remote_status(bucket: str | None = None, b2=None) -> dict:
    bucket = (bucket or get_bucket_name() or "").strip()
    if not bucket:
        raise ValueError("No B2 bucket configured.")
    if b2 is None:
        b2 = B2Manager(bucket)
    info = b2.get_file_info(DB_REMOTE_PATH)
    record_remote_info(info)
    return backup_status()


def snapshot_catalog(dest: Path | None = None) -> Path:
    """Consistent SQLite snapshot via the backup API (does not upload the live WAL)."""
    dest = dest or (DATA_DIR / SNAPSHOT_NAME)
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        dest.unlink()
    src = sqlite3.connect(str(DB_PATH))
    try:
        src.execute("PRAGMA wal_checkpoint(PASSIVE)")
        dst = sqlite3.connect(str(dest))
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()
    return dest


def _remember_local_snapshot(stat_mtime: float, stat_size: int, remote_info: dict | None = None) -> None:
    payload = {
        "db_backup_local_mtime": stat_mtime,
        "db_backup_local_size": stat_size,
        "db_backup_checked_at": _now(),
    }
    if remote_info:
        payload["db_backup_remote_size"] = int(remote_info.get("size") or 0)
        payload["db_backup_remote_ts_ms"] = int(remote_info.get("upload_timestamp_ms") or 0)
    update_config(**payload)


def sync_catalog_to_b2(bucket: str | None = None, *, b2=None, progress=None) -> dict:
    """Snapshot the live catalog and upload it to the metadata prefix."""
    bucket = (bucket or get_bucket_name() or "").strip()
    if not bucket:
        raise ValueError("No B2 bucket configured.")
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Local catalog not found: {DB_PATH}")
    if b2 is None:
        b2 = B2Manager(bucket)

    if progress:
        progress(0, 0, "Snapshotting the catalog…")
    local = local_db_stat()
    snapshot = snapshot_catalog()
    snap_size = snapshot.stat().st_size
    if progress:
        progress(0, snap_size, f"Snapshot is {format_size(snap_size)}. Copying previous B2 backup aside…")

    try:
        b2.copy_file(DB_REMOTE_PATH, DB_REMOTE_PREV_PATH)
    except Exception:
        pass

    listener = _job_byte_listener(progress, snap_size) if progress else None
    if progress:
        progress(0, snap_size, f"Uploading catalog to {DB_REMOTE_PATH}…")
    b2.upload_file(snapshot, DB_REMOTE_PATH, progress_listener=listener)

    info = None
    try:
        info = b2.get_file_info(DB_REMOTE_PATH)
    except Exception:
        pass
    _remember_local_snapshot(local["mtime"], local["size"], info)
    try:
        snapshot.unlink()
    except OSError:
        pass
    if progress:
        progress(snap_size, snap_size, f"Catalog backed up ({format_size(snap_size)}).")
    return backup_status()


def sync_catalog_from_b2(bucket: str | None = None, *, b2=None, progress=None) -> dict:
    """Download the B2 catalog over the local file, keeping a .prev copy."""
    from core.db import init_db

    bucket = (bucket or get_bucket_name() or "").strip()
    if not bucket:
        raise ValueError("No B2 bucket configured.")
    if b2 is None:
        b2 = B2Manager(bucket)

    incoming = DATA_DIR / INCOMING_NAME
    if incoming.exists():
        incoming.unlink()

    if progress:
        progress(0, 0, f"Downloading {DB_REMOTE_PATH}…")
    b2.download_file_to_path(DB_REMOTE_PATH, incoming)
    incoming_size = incoming.stat().st_size if incoming.exists() else 0
    if progress:
        progress(1, 2, f"Downloaded {format_size(incoming_size)}. Replacing the local catalog…")

    live = sqlite3.connect(str(DB_PATH))
    try:
        live.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except Exception:
        pass
    finally:
        live.close()

    prev = Path(str(DB_PATH) + ".prev")
    if DB_PATH.exists():
        shutil.copy2(DB_PATH, prev)

    os.replace(incoming, DB_PATH)
    for suffix in ("-wal", "-shm"):
        extra = Path(str(DB_PATH) + suffix)
        if extra.exists():
            try:
                extra.unlink()
            except OSError:
                pass

    init_db()
    local = local_db_stat()
    info = None
    try:
        info = b2.get_file_info(DB_REMOTE_PATH)
    except Exception:
        pass
    _remember_local_snapshot(local["mtime"], local["size"], info)
    if progress:
        progress(2, 2, f"Local catalog replaced ({local['size_fmt']}). Previous copy at {prev.name}.")
    return backup_status()


def _job_byte_listener(progress, total: int):
    """B2 upload listener that reports megabytes through the job log."""
    from b2sdk.v2 import AbstractProgressListener

    class JobByteListener(AbstractProgressListener):
        def __init__(self):
            self.total = total
            self._last = 0

        def set_total_bytes(self, total_bytes):
            self.total = int(total_bytes or self.total)

        def bytes_completed(self, byte_count):
            byte_count = int(byte_count or 0)
            if byte_count - self._last < 8 * 1024 * 1024 and byte_count < self.total:
                return
            self._last = byte_count
            if progress:
                progress(byte_count, self.total, None)

        def close(self):
            if progress:
                progress(self.total, self.total, None)

    return JobByteListener()


def run_backup_job(job: dict) -> None:
    from core.jobs import finish_job, update_progress

    job_id = job["id"]
    bucket = (job["payload"].get("bucket") or get_bucket_name() or "").strip()
    if not bucket:
        finish_job(job_id, "error", "No B2 bucket configured.")
        return

    def progress(done, total, line=None):
        update_progress(job_id, done, total, line)

    try:
        status = sync_catalog_to_b2(bucket, progress=progress)
    except Exception as e:
        finish_job(job_id, "error", str(e))
        return
    update_progress(job_id, 1, 1, f"Backup state: {status['state']}. {status['summary']}")
    finish_job(job_id, "done")


def run_restore_catalog_job(job: dict) -> None:
    from core.jobs import finish_job, update_progress

    job_id = job["id"]
    bucket = (job["payload"].get("bucket") or get_bucket_name() or "").strip()
    if not bucket:
        finish_job(job_id, "error", "No B2 bucket configured.")
        return

    def progress(done, total, line=None):
        update_progress(job_id, done, total, line)

    try:
        status = sync_catalog_from_b2(bucket, progress=progress)
    except Exception as e:
        finish_job(job_id, "error", str(e))
        return
    update_progress(job_id, 1, 1, f"Restore state: {status['state']}. {status['summary']}")
    finish_job(job_id, "done")


def run_delete_drive_job(job: dict) -> None:
    from core.b2 import sanitize_b2_path
    from core.catalog import delete_drive
    from core.jobs import finish_job, update_progress

    job_id = job["id"]
    payload = job["payload"]
    drive_name = (payload.get("drive_name") or "").strip()
    delete_from_b2 = bool(payload.get("delete_from_b2"))
    dry_run = bool(payload.get("dry_run"))
    bucket = (payload.get("bucket") or get_bucket_name() or "").strip()

    if not drive_name:
        finish_job(job_id, "error", "Drive name is required.")
        return

    if delete_from_b2:
        if not bucket:
            finish_job(job_id, "error", "No B2 bucket configured.")
            return
        try:
            b2 = B2Manager(bucket)
        except Exception as e:
            finish_job(job_id, "error", f"Could not connect to B2: {e}")
            return
        prefix = sanitize_b2_path(drive_name) + "/"
        update_progress(job_id, 0, 0, f"{'Dry run: listing' if dry_run else 'Deleting'} B2 objects under {prefix}")

        def cb(current, total):
            update_progress(job_id, current, total)

        try:
            names = b2.delete_directory(prefix, progress_callback=None if dry_run else cb, dry_run=dry_run)
        except Exception as e:
            finish_job(job_id, "error", f"B2 delete failed: {e}")
            return
        update_progress(
            job_id, len(names), len(names) or 1,
            f"{'Would delete' if dry_run else 'Deleted'} {len(names):,} B2 object(s).",
        )
        for name in names[:30]:
            update_progress(job_id, len(names), len(names) or 1, f"  {name}")
        if len(names) > 30:
            update_progress(job_id, len(names), len(names) or 1, f"  … and {len(names) - 30:,} more")
        if dry_run:
            update_progress(job_id, len(names), len(names) or 1, "Local catalog was not changed.")
            finish_job(job_id, "done")
            return

    if dry_run:
        finish_job(job_id, "done")
        return

    update_progress(job_id, 0, 1, f"Removing catalog rows for {drive_name}…")
    deleted = delete_drive(drive_name)
    update_progress(job_id, 1, 1, f"Removed {deleted:,} catalog row(s) for {drive_name}.")
    finish_job(job_id, "done")
