"""Restore set + start restore job."""
from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from core import restore_set
from core.config import get_bucket_name, get_restore_dest, set_restore_dest
from core.jobs import enqueue
from web.templating import page_vars, session_key, set_notice, templates

router = APIRouter()


def _next_url(request: Request, nxt: str) -> str:
    nxt = (nxt or "").strip()
    if nxt.startswith("/") and not nxt.startswith("//"):
        return nxt
    return request.headers.get("referer") or "/restore-set"


@router.get("/restore-set", response_class=HTMLResponse)
def restore_set_page(request: Request):
    key = session_key(request)
    return templates.TemplateResponse(
        "restore_set.html",
        page_vars(
            request,
            bag=restore_set.summary(key),
            restore_dest=get_restore_dest(),
            bucket=get_bucket_name(),
        ),
    )


@router.post("/restore-set/add")
def restore_add(
    request: Request,
    file_id: int | None = Form(None),
    drive: str = Form(""),
    folder_path: str = Form(""),
    next: str = Form(""),
):
    key = session_key(request)
    if file_id:
        n = restore_set.add_ids(key, [file_id])
        set_notice(request, "Added to restore set." if n else "Already in the restore set.")
    elif drive:
        n = restore_set.add_folder(key, drive, folder_path)
        label = folder_path or drive
        set_notice(request, f"Added {n:,} file(s) from {label}.")
    return RedirectResponse(_next_url(request, next), status_code=303)


@router.post("/restore-set/remove")
def restore_remove(
    request: Request,
    file_id: int = Form(...),
    next: str = Form(""),
):
    restore_set.remove_id(session_key(request), file_id)
    set_notice(request, "Removed from restore set.")
    return RedirectResponse(_next_url(request, next), status_code=303)


@router.post("/restore-set/clear")
def restore_clear(request: Request):
    restore_set.clear(session_key(request))
    set_notice(request, "Restore set cleared.")
    return RedirectResponse("/restore-set", status_code=303)


@router.post("/restore-set/start")
def restore_start(
    request: Request,
    dest: str = Form(""),
):
    key = session_key(request)
    dest = dest.strip()
    if dest:
        set_restore_dest(dest)
    else:
        dest = get_restore_dest()
    ids = restore_set.list_ids(key)
    if not ids:
        set_notice(request, "Restore set is empty.")
        return RedirectResponse("/restore-set", status_code=303)
    if not dest:
        set_notice(request, "Set a restore destination first.")
        return RedirectResponse("/restore-set", status_code=303)
    if not get_bucket_name():
        set_notice(request, "Set a B2 bucket in Settings first.")
        return RedirectResponse("/restore-set", status_code=303)

    job_id = enqueue(
        "restore",
        {
            "ids": ids,
            "dest": dest,
            "bucket": get_bucket_name(),
        },
    )
    set_notice(request, f"Restore job #{job_id} queued.")
    return RedirectResponse(f"/jobs/{job_id}", status_code=303)
