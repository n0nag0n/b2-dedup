"""Manual browser uploads into the Scratch drive.

The page accepts files or whole folders, stages them under ``data/scratch``,
then queues the normal ingest job. Everything after the upload is the same
code path a plugged-in disk uses.
"""
from __future__ import annotations

from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from core.config import (
    get_bucket_name,
    get_scratch_drive_name,
    get_scratch_retention_days,
    get_workers,
    set_bucket_name,
)
from core.jobs import enqueue
from core.junk import INGEST_SKIP_DIR_NAMES, INGEST_SKIP_FILE_NAMES
from core.scratch import (
    ScratchDiskFull,
    ScratchError,
    batch_dir,
    batch_is_active,
    batch_stats,
    cleanup_batch,
    join_prefix,
    list_batches,
    save_upload,
    scratch_usage,
)
from web.templating import page_vars, set_notice, templates

router = APIRouter()


def _wants_json(request: Request) -> bool:
    return "application/json" in (request.headers.get("accept") or "")


def _fail(request: Request, message: str, status_code: int = 400):
    if _wants_json(request):
        return JSONResponse({"ok": False, "error": message}, status_code=status_code)
    set_notice(request, message)
    return RedirectResponse("/scratch", status_code=303)


def _drive_name(raw: str) -> str | None:
    name = (raw or "").strip() or get_scratch_drive_name()
    if not name or "/" in name or "\\" in name:
        return None
    return name


def _page(request: Request, **extra):
    return templates.TemplateResponse(
        "scratch.html",
        page_vars(
            request,
            scratch_drive=get_scratch_drive_name(),
            retention_days=get_scratch_retention_days(),
            batches=list_batches(),
            usage=scratch_usage(),
            bucket=get_bucket_name(),
            workers=get_workers(),
            junk_dirs=sorted(INGEST_SKIP_DIR_NAMES),
            junk_files=sorted(INGEST_SKIP_FILE_NAMES),
            **extra,
        ),
    )


@router.get("/scratch", response_class=HTMLResponse)
def scratch_page(request: Request):
    return _page(request)


@router.post("/scratch/upload")
def scratch_upload(
    file: UploadFile = File(...),
    batch: str = Form(""),
    path: str = Form(""),
    prefix: str = Form(""),
    modified: str = Form(""),
):
    if batch_dir(batch) is None:
        return JSONResponse(
            {"ok": False, "error": "That upload batch is not valid. Reload the page and try again."},
            status_code=400,
        )

    rel = (path or file.filename or "").strip()
    try:
        rel = join_prefix(prefix, rel)
    except ScratchError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)

    try:
        result = save_upload(file.file, batch, rel, modified)
    except ScratchDiskFull as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=507)
    except ScratchError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    except OSError as exc:
        return JSONResponse(
            {"ok": False, "error": f"Could not save that file: {exc}"},
            status_code=500,
        )

    result["ok"] = True
    return JSONResponse(result)


@router.post("/scratch/finish")
def scratch_finish(
    request: Request,
    batch: str = Form(""),
    drive_name: str = Form(""),
    bucket: str = Form(""),
):
    stats = batch_stats(batch)
    if stats["files"] == 0:
        return _fail(request, "There are no uploaded files to ingest yet.")
    if batch_is_active(batch):
        return _fail(request, "That upload is already being ingested.")

    drive = _drive_name(drive_name)
    if not drive:
        return _fail(request, "Drive name is required and cannot contain slashes.")

    bucket = (bucket or get_bucket_name()).strip()
    if not bucket:
        return _fail(
            request,
            "Set a B2 bucket first. Uploads stay staged so you can ingest them later.",
        )
    if bucket != get_bucket_name():
        set_bucket_name(bucket)

    job_id = enqueue(
        "ingest",
        {
            "source": stats["path"],
            "drive_name": drive,
            "drive_root": stats["path"],
            "bucket": bucket,
            "scan_only": False,
            "dry_run": False,
            "workers": get_workers(),
            "cleanup_source": True,
        },
    )
    message = (
        f"Scratch ingest job #{job_id} queued "
        f"({stats['files']:,} files, {stats['bytes_fmt']})."
    )
    if _wants_json(request):
        return JSONResponse(
            {"ok": True, "job_id": job_id, "redirect": f"/jobs/{job_id}", "message": message}
        )
    set_notice(request, message)
    return RedirectResponse(f"/jobs/{job_id}", status_code=303)


@router.post("/scratch/cleanup")
def scratch_cleanup(request: Request, batch: str = Form("")):
    if batch_is_active(batch):
        set_notice(request, "That upload is being ingested right now. Try again when the job finishes.")
    elif cleanup_batch(batch):
        set_notice(request, "Removed the staged upload.")
    else:
        set_notice(request, "That staged upload could not be removed.")
    return RedirectResponse("/scratch", status_code=303)
