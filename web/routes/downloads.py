"""Browser downloads: one file, a streamed ZIP, or a prepared ZIP export."""
from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse, RedirectResponse, StreamingResponse

from core import restore_set
from core.catalog import get_file, get_files_by_ids, get_selection_size
from core.config import get_bucket_name, get_download_expiry_hours
from core.download import (
    content_disposition,
    estimated_export_need,
    export_name,
    export_path,
    free_disk_bytes,
    open_remote,
    resolve_source,
    selection_error,
    shared_manager,
    stream_downloaded_file,
    stream_zip,
)
from core.jobs import enqueue, get_job
from web.templating import page_vars, session_key, set_notice, templates

router = APIRouter()


def _message(request: Request, message: str, status_code: int = 400):
    return templates.TemplateResponse(
        "error.html",
        page_vars(request, message=message),
        status_code=status_code,
    )


@router.get("/download/{file_id}")
def download_file(request: Request, file_id: int):
    """Stream one catalog row straight to the browser as an attachment."""
    rec = get_file(file_id)
    if not rec:
        return _message(request, "That file is not in the catalog.", status_code=404)

    bucket = get_bucket_name()
    if not bucket:
        set_notice(request, "Set a B2 bucket in Settings before downloading.")
        return RedirectResponse("/settings", status_code=303)

    try:
        b2 = shared_manager(bucket)
    except Exception as exc:  # noqa: BLE001 - credentials / network
        return _message(request, f"Could not connect to B2: {exc}", status_code=502)

    try:
        source = resolve_source(b2, rec)
        downloaded = open_remote(b2, source)
    except Exception as exc:  # noqa: BLE001 - missing object, bad pointer, no creds
        return _message(
            request,
            f"B2 does not have the bytes for {rec['drive_name']}/{rec['file_path']}: {exc}",
            status_code=404,
        )

    try:
        length = int(downloaded.download_version.content_length or rec.get("size") or 0)
    except Exception:  # noqa: BLE001 - fall back to the catalog size
        length = int(rec.get("size") or 0)

    headers = {
        "Content-Disposition": content_disposition(rec.get("name") or rec["file_path"]),
        "Content-Length": str(length),
        "Cache-Control": "private, no-store",
        "X-Content-Type-Options": "nosniff",
    }

    return StreamingResponse(
        stream_downloaded_file(downloaded),
        media_type=rec.get("mime_type") or "application/octet-stream",
        headers=headers,
    )


def _selection(request: Request) -> tuple[list[int], int]:
    ids = restore_set.list_ids(session_key(request))
    return ids, get_selection_size(ids)


@router.get("/restore-set/download.zip")
def download_zip_stream(request: Request):
    """Stream the whole restore set as a ZIP. Nothing is staged on disk."""
    ids, total_bytes = _selection(request)
    if not ids:
        set_notice(request, "Restore set is empty.")
        return RedirectResponse("/restore-set", status_code=303)

    problem = selection_error(len(ids), total_bytes)
    if problem:
        return _message(request, problem, status_code=413)

    bucket = get_bucket_name()
    if not bucket:
        set_notice(request, "Set a B2 bucket in Settings before downloading.")
        return RedirectResponse("/settings", status_code=303)

    try:
        b2 = shared_manager(bucket)
    except Exception as exc:  # noqa: BLE001
        return _message(request, f"Could not connect to B2: {exc}", status_code=502)

    records = get_files_by_ids(ids)
    if not records:
        set_notice(request, "Those files are no longer in the catalog.")
        return RedirectResponse("/restore-set", status_code=303)

    stamp = datetime.now().strftime("%Y%m%d-%H%M")
    headers = {
        "Content-Disposition": content_disposition(f"storageinsights-{stamp}.zip"),
        "Cache-Control": "private, no-store",
        "X-Content-Type-Options": "nosniff",
    }
    return StreamingResponse(
        stream_zip(records, b2),
        media_type="application/zip",
        headers=headers,
    )


@router.post("/restore-set/export")
def prepare_zip(request: Request):
    """Queue a background job that builds a resumable ZIP on the server."""
    ids, total_bytes = _selection(request)
    if not ids:
        set_notice(request, "Restore set is empty.")
        return RedirectResponse("/restore-set", status_code=303)

    problem = selection_error(len(ids), total_bytes)
    if problem:
        set_notice(request, problem)
        return RedirectResponse("/restore-set", status_code=303)

    bucket = get_bucket_name()
    if not bucket:
        set_notice(request, "Set a B2 bucket in Settings before downloading.")
        return RedirectResponse("/settings", status_code=303)

    free = free_disk_bytes()
    need = estimated_export_need(total_bytes)
    if free and free < need:
        set_notice(
            request,
            "Not enough free disk space to prepare that archive "
            f"({free / 1024 ** 3:.1f} GB free, about {need / 1024 ** 3:.1f} GB needed). "
            "Stream it instead, or remove some files.",
        )
        return RedirectResponse("/restore-set", status_code=303)

    job_id = enqueue(
        "zip_export",
        {"ids": ids, "bucket": bucket, "bytes": total_bytes},
    )
    set_notice(request, f"Preparing ZIP job #{job_id} queued.")
    return RedirectResponse(f"/jobs/{job_id}", status_code=303)


@router.get("/exports/{job_id}.zip")
def download_export(request: Request, job_id: int):
    """Serve a ZIP prepared by a finished job. Supports Range, so it resumes."""
    job = get_job(job_id)
    if not job or job.get("type") != "zip_export":
        return _message(request, "That download was not found.", status_code=404)
    if job.get("status") != "done":
        set_notice(request, "That ZIP is still being prepared.")
        return RedirectResponse(f"/jobs/{job_id}", status_code=303)

    path = export_path(job_id)
    if not path.is_file():
        set_notice(
            request,
            "That prepared ZIP has expired. Build it again from the restore set.",
        )
        return RedirectResponse("/restore-set", status_code=303)

    expiry = get_download_expiry_hours()
    return FileResponse(
        path,
        media_type="application/zip",
        filename=export_name(job_id, job.get("created_at")),
        headers={
            "Cache-Control": f"private, max-age={expiry * 3600}",
            "X-Content-Type-Options": "nosniff",
        },
    )
