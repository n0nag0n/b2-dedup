"""Manual browser uploads into a scratch drive.

The page accepts files or whole folders, stages them in the folder chosen on
this computer, then queues the normal ingest job. Everything after the upload
is the same code path a plugged-in disk uses. The B2 bucket comes from
Settings and is not edited here.
"""
from __future__ import annotations

from pathlib import Path
from urllib.parse import urlencode

from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from core.config import (
    get_bucket_name,
    get_scratch_drive_name,
    get_scratch_drives,
    get_scratch_retention_days,
    get_workers,
    normalize_drive_name,
    remove_scratch_drive,
    set_scratch_drive_name,
    set_scratch_staging_dir,
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
    create_child_dir,
    folder_crumbs,
    join_prefix,
    list_batches,
    list_child_dirs,
    parent_dir,
    resolve_browse_dir,
    save_upload,
    scratch_usage,
    staging_is_ready,
    staging_root,
    validate_staging_dir,
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


def _known_drive(raw: str) -> str | None:
    """Return the stored spelling of a scratch drive, or None."""
    text = (raw or "").strip() or get_scratch_drive_name()
    text = normalize_drive_name(text) or ""
    if not text:
        return None
    for name in get_scratch_drives():
        if name.lower() == text.lower():
            return name
    return None


def _browse_start() -> Path:
    root = staging_root()
    if root.is_dir():
        return root
    if root.parent.is_dir():
        return root.parent
    home = Path.home()
    if home.is_dir():
        return home
    return Path("/")


def _browse_url(path: str = "") -> str:
    query = {"browse": "1"}
    if path:
        query["path"] = path
    return "/scratch?" + urlencode(query)


def _listing(current: Path) -> dict:
    dirs, truncated = list_child_dirs(current)
    staging = staging_root()
    try:
        is_staging = current.resolve() == staging.resolve()
    except OSError:
        is_staging = False
    return {
        "path": str(current),
        "parent": parent_dir(current),
        "crumbs": folder_crumbs(current),
        "dirs": dirs,
        "truncated": truncated,
        "is_staging": is_staging,
    }


def _page(request: Request, **extra):
    root = staging_root()
    extra.setdefault("browsing", False)
    extra.setdefault("listing", None)
    return templates.TemplateResponse(
        "scratch.html",
        page_vars(
            request,
            scratch_drive=get_scratch_drive_name(),
            scratch_drives=get_scratch_drives(),
            retention_days=get_scratch_retention_days(),
            batches=list_batches(),
            usage=scratch_usage(),
            bucket=get_bucket_name(),
            workers=get_workers(),
            junk_dirs=sorted(INGEST_SKIP_DIR_NAMES),
            junk_files=sorted(INGEST_SKIP_FILE_NAMES),
            staging_path=str(root),
            staging_ok=staging_is_ready(root),
            home_path=str(Path.home()),
            **extra,
        ),
    )


@router.get("/scratch", response_class=HTMLResponse)
def scratch_page(request: Request, browse: str = "", path: str = ""):
    listing = None
    if browse == "1":
        requested = path.strip()
        target = requested or str(_browse_start())
        try:
            listing = _listing(resolve_browse_dir(target))
        except ScratchError as exc:
            set_notice(request, str(exc))
            if requested:
                return RedirectResponse(_browse_url(), status_code=303)
    return _page(request, browsing=listing is not None, listing=listing)


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


@router.post("/scratch/drives")
def scratch_add_drive(request: Request, drive_name: str = Form("")):
    if normalize_drive_name(drive_name) is None:
        set_notice(request, "Drive name is required and cannot contain slashes.")
        return RedirectResponse("/scratch", status_code=303)
    name = set_scratch_drive_name(drive_name)
    set_notice(request, f"{name} is a scratch drive now. It is selected for the next upload.")
    return RedirectResponse("/scratch", status_code=303)


@router.post("/scratch/drives/select")
def scratch_select_drive(request: Request, drive_name: str = Form("")):
    drive = _known_drive(drive_name)
    if not drive:
        return _fail(request, "That scratch drive is not in the list. Add it first.")
    set_scratch_drive_name(drive)
    if _wants_json(request):
        return JSONResponse({"ok": True, "drive_name": drive})
    return RedirectResponse("/scratch", status_code=303)


@router.post("/scratch/drives/remove")
def scratch_remove_drive(request: Request, drive_name: str = Form("")):
    names = get_scratch_drives()
    target = None
    for name in names:
        if name.lower() == (drive_name or "").strip().lower():
            target = name
            break
    if target is None:
        set_notice(request, "That scratch drive is not in the list.")
    elif len(names) == 1:
        set_notice(request, "Keep at least one scratch drive.")
    else:
        remove_scratch_drive(target)
        set_notice(
            request,
            f"Removed {target} from the scratch drives. Files already in the vault stay.",
        )
    return RedirectResponse("/scratch", status_code=303)


@router.post("/scratch/staging")
def scratch_set_staging(request: Request, path: str = Form("")):
    try:
        chosen = validate_staging_dir(path)
    except ScratchError as exc:
        set_notice(request, str(exc))
        return RedirectResponse(_browse_url(path.strip()), status_code=303)
    set_scratch_staging_dir(str(chosen))
    set_notice(
        request,
        f"Scratch uploads will be stored in {chosen}. "
        "Batches already staged in another folder stay on the list below until you ingest or delete them.",
    )
    return RedirectResponse("/scratch", status_code=303)


@router.post("/scratch/staging/mkdir")
def scratch_mkdir(request: Request, parent: str = Form(""), name: str = Form("")):
    try:
        created = create_child_dir(Path(parent), name)
    except ScratchError as exc:
        set_notice(request, str(exc))
        back = parent.strip()
        return RedirectResponse(_browse_url(back) if back else _browse_url(), status_code=303)
    return RedirectResponse(_browse_url(str(created)), status_code=303)


@router.post("/scratch/finish")
def scratch_finish(
    request: Request,
    batch: str = Form(""),
    drive_name: str = Form(""),
):
    stats = batch_stats(batch)
    if stats["files"] == 0:
        return _fail(request, "There are no uploaded files to ingest yet.")
    if batch_is_active(batch):
        return _fail(request, "That upload is already being ingested.")

    drive = _known_drive(drive_name)
    if not drive:
        return _fail(request, "That scratch drive is not in the list. Add it first.")
    set_scratch_drive_name(drive)

    bucket = get_bucket_name()
    if not bucket:
        return _fail(
            request,
            "Set a B2 bucket in Settings first. Uploads stay staged so you can ingest them later.",
        )

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
