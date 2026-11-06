"""Settings: bucket, restore dest, catalog backup, verify, manage drives."""
from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from core.backup import backup_status, refresh_remote_status
from core.catalog import get_drive_names
from core.library import dates_status
from core.config import (
    add_ingest_extra_root,
    get_bucket_name,
    get_download_expiry_hours,
    get_download_max_bytes,
    get_download_max_files,
    get_download_stream_threshold,
    get_ingest_extra_roots,
    get_restore_dest,
    get_scratch_drive_name,
    get_scratch_drives,
    get_scratch_retention_days,
    get_workers,
    normalize_drive_name,
    remove_ingest_extra_root,
    remove_scratch_drive,
    set_bucket_name,
    set_download_expiry_hours,
    set_download_max_bytes,
    set_download_max_files,
    set_download_stream_threshold,
    set_restore_dest,
    set_scratch_drive_name,
    set_scratch_retention_days,
    set_workers,
)
from core.volumes import is_forbidden_root, scan_places
from core.health import get_vault_health
from core.jobs import enqueue
from core.junk import INGEST_SKIP_DIR_NAMES, INGEST_SKIP_FILE_NAMES, SEARCH_HIDE_DIR_NAMES
from core.paths import DB_REMOTE_PATH
from core.scratch import clear_batches, scratch_usage, staging_root
from core.thumbs import thumbs_status
from core.verify import DEFAULT_SAMPLE_SIZE
from web.auth import password_required
from web.templating import page_vars, set_notice, templates

router = APIRouter()

SYNC_FROM_PHRASE = "overwrite"


def _library_view() -> dict:
    dates = dates_status()
    return {
        "images_fmt": f"{dates['images']:,}",
        "dated_fmt": f"{dates['dated']:,}",
        "folder_fmt": f"{dates['folder']:,}",
        "mtime_fmt": f"{dates['mtime']:,}",
        "exif_fmt": f"{dates['exif']:,}",
        **dates,
    }


def _downloads_view() -> dict:
    max_bytes = get_download_max_bytes()
    threshold = get_download_stream_threshold()
    return {
        "max_files": get_download_max_files(),
        "max_gb": round(max_bytes / 1024 ** 3, 2),
        "stream_threshold_mb": round(threshold / 1024 ** 2),
        "expiry_hours": get_download_expiry_hours(),
    }


def _settings_page(request: Request, **extra):
    health = get_vault_health()
    return templates.TemplateResponse(
        "settings.html",
        page_vars(
            request,
            health=health,
            library=_library_view(),
            thumbs=thumbs_status(),
            backup=backup_status(),
            drives=get_drive_names(),
            bucket=get_bucket_name(),
            restore_dest=get_restore_dest(),
            workers=get_workers(),
            downloads=_downloads_view(),
            sample_size=DEFAULT_SAMPLE_SIZE,
            junk_dirs=sorted(INGEST_SKIP_DIR_NAMES),
            junk_files=sorted(INGEST_SKIP_FILE_NAMES),
            hide_dirs=sorted(SEARCH_HIDE_DIR_NAMES),
            password_set=password_required(),
            remote_path=DB_REMOTE_PATH,
            sync_from_phrase=SYNC_FROM_PHRASE,
            ingest_scan=scan_places(),
            ingest_extra_roots=get_ingest_extra_roots(),
            scratch=scratch_usage(),
            scratch_drive=get_scratch_drive_name(),
            scratch_drives=get_scratch_drives(),
            scratch_staging=str(staging_root()),
            scratch_retention=get_scratch_retention_days(),
            **extra,
        ),
    )


@router.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request):
    return _settings_page(request)


@router.post("/settings/save")
def settings_save(
    request: Request,
    bucket_name: str = Form(""),
    restore_dest: str = Form(""),
    workers: int = Form(10),
):
    bucket_name = bucket_name.strip()
    restore_dest = restore_dest.strip()
    try:
        workers = max(1, min(int(workers), 64))
    except (TypeError, ValueError):
        workers = get_workers()
    set_bucket_name(bucket_name)
    set_restore_dest(restore_dest)
    set_workers(workers)
    set_notice(request, "Connection saved.")
    return RedirectResponse("/settings#connection", status_code=303)


@router.post("/settings/downloads")
def settings_downloads(
    request: Request,
    max_files: str = Form("1000"),
    max_gb: str = Form("5"),
    stream_threshold_mb: str = Form("500"),
    expiry_hours: str = Form("24"),
):
    try:
        files = int(max_files)
    except (TypeError, ValueError):
        files = get_download_max_files()
    try:
        gb = float(max_gb)
    except (TypeError, ValueError):
        gb = get_download_max_bytes() / 1024 ** 3
    try:
        threshold = float(stream_threshold_mb)
    except (TypeError, ValueError):
        threshold = get_download_stream_threshold() / 1024 ** 2
    try:
        hours = int(expiry_hours)
    except (TypeError, ValueError):
        hours = get_download_expiry_hours()

    set_download_max_files(files)
    set_download_max_bytes(int(gb * 1024 ** 3))
    set_download_stream_threshold(int(threshold * 1024 ** 2))
    set_download_expiry_hours(hours)
    set_notice(request, "Download limits saved.")
    return RedirectResponse("/settings#downloads", status_code=303)


@router.post("/settings/ingest-root/add")
def settings_add_ingest_root(request: Request, folder: str = Form("")):
    raw = folder.strip()
    if not raw:
        set_notice(request, "Enter a folder to watch for ingest.")
        return RedirectResponse("/settings#ingest", status_code=303)
    path = Path(raw).expanduser()
    try:
        if not path.is_dir():
            set_notice(request, "That path is not a folder on this computer.")
            return RedirectResponse("/settings#ingest", status_code=303)
        resolved = path.resolve()
    except OSError:
        set_notice(request, "Could not open that folder.")
        return RedirectResponse("/settings#ingest", status_code=303)
    if is_forbidden_root(resolved):
        set_notice(request, "Choose the disk or folder you want to back up, not a system directory.")
        return RedirectResponse("/settings#ingest", status_code=303)
    add_ingest_extra_root(str(resolved))
    set_notice(request, f"Ingest will also look in {resolved}.")
    return RedirectResponse("/settings#ingest", status_code=303)


@router.post("/settings/ingest-root/remove")
def settings_remove_ingest_root(request: Request, folder: str = Form("")):
    remove_ingest_extra_root(folder.strip())
    set_notice(request, "Removed that extra folder.")
    return RedirectResponse("/settings#ingest", status_code=303)


@router.post("/settings/scratch")
def settings_scratch(
    request: Request,
    drive_name: str = Form(""),
    retention_days: str = Form("7"),
):
    days = set_scratch_retention_days(retention_days)
    # drive_name is optional so older clients can still add a drive with this form.
    if drive_name.strip():
        if normalize_drive_name(drive_name) is None:
            set_notice(request, "Drive name is required and cannot contain slashes.")
            return RedirectResponse("/settings#scratch", status_code=303)
        name = set_scratch_drive_name(drive_name)
        set_notice(request, f"Scratch drive {name} added. Staged uploads are kept for {days} days.")
    else:
        set_notice(request, f"Scratch retention saved: {days} days.")
    return RedirectResponse("/settings#scratch", status_code=303)


@router.post("/settings/scratch/add")
def settings_scratch_add(request: Request, drive_name: str = Form("")):
    if normalize_drive_name(drive_name) is None:
        set_notice(request, "Drive name is required and cannot contain slashes.")
        return RedirectResponse("/settings#scratch", status_code=303)
    name = set_scratch_drive_name(drive_name)
    set_notice(request, f"{name} is a scratch drive now.")
    return RedirectResponse("/settings#scratch", status_code=303)


@router.post("/settings/scratch/remove")
def settings_scratch_remove(request: Request, drive_name: str = Form("")):
    names = get_scratch_drives()
    target = None
    for name in names:
        if name.lower() == drive_name.strip().lower():
            target = name
            break
    if target is None:
        set_notice(request, "That scratch drive is not in the list.")
    elif len(names) == 1:
        set_notice(request, "Keep at least one scratch drive.")
    else:
        remove_scratch_drive(target)
        set_notice(request, f"Removed {target} from the scratch drives. Files already in the vault stay.")
    return RedirectResponse("/settings#scratch", status_code=303)


@router.post("/settings/scratch/clear")
def settings_scratch_clear(request: Request):
    removed = clear_batches()
    if removed:
        set_notice(request, f"Removed {removed} staged upload{'s' if removed != 1 else ''}.")
    else:
        set_notice(request, "There are no staged uploads to remove.")
    return RedirectResponse("/settings#scratch", status_code=303)


@router.post("/settings/verify")
def settings_verify(
    request: Request,
    mode: str = Form("sample"),
    sample_size: int = Form(DEFAULT_SAMPLE_SIZE),
):
    bucket = get_bucket_name()
    if not bucket:
        set_notice(request, "Set a B2 bucket first.")
        return RedirectResponse("/settings#connection", status_code=303)
    mode = "full" if mode == "full" else "sample"
    try:
        sample_size = max(1, min(int(sample_size), 10000))
    except (TypeError, ValueError):
        sample_size = DEFAULT_SAMPLE_SIZE
    job_id = enqueue(
        "verify",
        {
            "mode": mode,
            "sample_size": sample_size,
            "bucket": bucket,
            "workers": get_workers(),
        },
    )
    set_notice(request, f"Verify job #{job_id} queued ({mode}).")
    return RedirectResponse(f"/jobs/{job_id}", status_code=303)


@router.post("/settings/dates")
def settings_dates(request: Request):
    job_id = enqueue("dates", {})
    set_notice(request, f"Date job #{job_id} queued.")
    return RedirectResponse(f"/jobs/{job_id}", status_code=303)


@router.post("/settings/thumbs")
def settings_thumbs(request: Request, limit: str = Form("500")):
    bucket = get_bucket_name()
    if not bucket:
        set_notice(request, "Set a B2 bucket first.")
        return RedirectResponse("/settings#connection", status_code=303)
    try:
        n = int(limit)
    except (TypeError, ValueError):
        n = 500
    payload = {"bucket": bucket, "limit": n}
    job_id = enqueue("thumbs", payload)
    set_notice(request, f"Thumbnail job #{job_id} queued.")
    return RedirectResponse(f"/jobs/{job_id}", status_code=303)


@router.post("/settings/backup/refresh")
def settings_backup_refresh(request: Request):
    if not get_bucket_name():
        set_notice(request, "Set a B2 bucket first.")
        return RedirectResponse("/settings#connection", status_code=303)
    try:
        refresh_remote_status()
        set_notice(request, "Checked B2 for the catalog backup.")
    except Exception as e:
        set_notice(request, f"Could not check B2: {e}")
    return RedirectResponse("/settings#backup", status_code=303)


@router.post("/settings/backup/sync-to")
def settings_backup_sync_to(request: Request, confirm: str = Form("")):
    if not get_bucket_name():
        set_notice(request, "Set a B2 bucket first.")
        return RedirectResponse("/settings#connection", status_code=303)
    if confirm != "1":
        set_notice(request, "Confirm that you want to replace the B2 catalog backup.")
        return RedirectResponse("/settings#backup", status_code=303)
    job_id = enqueue("db_backup", {"bucket": get_bucket_name()})
    set_notice(request, f"Catalog backup job #{job_id} queued.")
    return RedirectResponse(f"/jobs/{job_id}", status_code=303)


@router.post("/settings/backup/sync-from")
def settings_backup_sync_from(request: Request, confirm: str = Form("")):
    if not get_bucket_name():
        set_notice(request, "Set a B2 bucket first.")
        return RedirectResponse("/settings#connection", status_code=303)
    if (confirm or "").strip().lower() != SYNC_FROM_PHRASE:
        set_notice(request, f'Type "{SYNC_FROM_PHRASE}" to replace the local catalog from B2.')
        return RedirectResponse("/settings#backup", status_code=303)
    job_id = enqueue("db_restore", {"bucket": get_bucket_name()})
    set_notice(request, f"Catalog restore job #{job_id} queued.")
    return RedirectResponse(f"/jobs/{job_id}", status_code=303)


@router.post("/settings/delete-drive")
def settings_delete_drive(
    request: Request,
    drive_name: str = Form(""),
    confirm_name: str = Form(""),
    delete_from_b2: str = Form(""),
    dry_run: str = Form(""),
):
    drive_name = drive_name.strip()
    confirm_name = confirm_name.strip()
    if not drive_name:
        set_notice(request, "Choose a drive to remove.")
        return RedirectResponse("/settings#danger", status_code=303)
    if confirm_name != drive_name:
        set_notice(request, "Drive name did not match. Nothing was deleted.")
        return RedirectResponse("/settings#danger", status_code=303)
    from_b2 = bool(delete_from_b2)
    is_dry = bool(dry_run)
    if from_b2 and not get_bucket_name():
        set_notice(request, "Set a B2 bucket before deleting objects there.")
        return RedirectResponse("/settings#connection", status_code=303)
    job_id = enqueue(
        "delete_drive",
        {
            "drive_name": drive_name,
            "delete_from_b2": from_b2,
            "dry_run": is_dry,
            "bucket": get_bucket_name(),
        },
    )
    set_notice(request, f"Delete-drive job #{job_id} queued for {drive_name}.")
    return RedirectResponse(f"/jobs/{job_id}", status_code=303)
