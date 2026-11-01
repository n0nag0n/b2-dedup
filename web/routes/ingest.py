"""Web ingest wizard: pick a plugged-in disk or added folder, preview, start a job."""
from __future__ import annotations

from pathlib import Path
from urllib.parse import urlencode

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from core.catalog import get_drive_names
from core.config import (
    add_ingest_extra_root,
    get_bucket_name,
    get_ingest_extra_roots,
    get_workers,
    set_bucket_name,
)
from core.ingest import preview_ingest
from core.jobs import enqueue
from core.junk import INGEST_SKIP_DIR_NAMES, INGEST_SKIP_FILE_NAMES
from core.paths import DEFAULT_MAX_WORKERS
from core.volumes import (
    allowed_roots,
    containing_root,
    extra_root_paths,
    discover_volume_paths,
    is_forbidden_root,
    list_ingest_targets,
    resolve_allowed_path,
    suggest_drive,
)
from web.templating import page_vars, set_notice, templates

router = APIRouter()


def _roots() -> list[Path]:
    return allowed_roots(
        extra=extra_root_paths(get_ingest_extra_roots()),
        volumes=discover_volume_paths(),
    )


def _volume_and_extra() -> tuple[list[Path], list[Path]]:
    return discover_volume_paths(), extra_root_paths(get_ingest_extra_roots())


def _browse_url(path: str = "") -> str:
    if not path:
        return "/ingest"
    return "/ingest?" + urlencode({"path": path})


def _crumbs(current: Path, root: Path) -> list[dict]:
    crumbs = [{"label": "Disks", "path": ""}]
    try:
        rel_parts = list(current.resolve().relative_to(root).parts)
    except ValueError:
        crumbs.append({"label": current.name or str(current), "path": str(current)})
        return crumbs
    acc = root
    crumbs.append({"label": root.name or str(root), "path": str(root)})
    for part in rel_parts:
        acc = acc / part
        crumbs.append({"label": part, "path": str(acc)})
    return crumbs


def _parent_path(current: Path, root: Path) -> str:
    if current.resolve() == root.resolve():
        return ""
    return str(current.parent)


def _list_dirs(current: Path, roots: list[Path]) -> list[dict]:
    entries = []
    try:
        children = sorted(current.iterdir(), key=lambda p: p.name.lower())
    except OSError:
        return entries
    for child in children:
        if child.is_symlink() or not child.is_dir():
            continue
        try:
            resolved = child.resolve()
        except OSError:
            continue
        if containing_root(resolved, roots) is None:
            continue
        entries.append({"name": child.name, "path": str(resolved)})
    return entries


def _listing(current: Path, roots: list[Path], volumes: list[Path], extras: list[Path]) -> dict:
    name, drive_root = suggest_drive(current, volumes, extras)
    root = containing_root(current, roots) or drive_root
    return {
        "path": str(current),
        "abs_path": str(current),
        "parent": _parent_path(current, root),
        "crumbs": _crumbs(current, root),
        "dirs": _list_dirs(current, roots),
        "suggested_drive": name,
        "suggested_root": str(drive_root),
    }


def _validate_drive_name(drive_name: str) -> str | None:
    drive_name = drive_name.strip()
    if not drive_name or "/" in drive_name or "\\" in drive_name:
        return None
    return drive_name


def _common_page(request: Request, **extra):
    catalog = get_drive_names()
    extra.setdefault("bucket", get_bucket_name())
    extra.setdefault("workers", get_workers())
    extra.setdefault("targets", list_ingest_targets(catalog_names=catalog))
    extra.setdefault("catalog_drives", catalog)
    extra.setdefault("junk_dirs", sorted(INGEST_SKIP_DIR_NAMES))
    extra.setdefault("junk_files", sorted(INGEST_SKIP_FILE_NAMES))
    extra.setdefault("existing_drive", False)
    return page_vars(request, **extra)


@router.get("/ingest", response_class=HTMLResponse)
def ingest_browse(request: Request, path: str = ""):
    roots = _roots()
    volumes, extras = _volume_and_extra()
    listing = None
    if path:
        current = resolve_allowed_path(path, roots)
        if current is None or not current.is_dir():
            set_notice(request, "That folder is not one of the disks or folders we can ingest.")
            return RedirectResponse("/ingest", status_code=303)
        listing = _listing(current, roots, volumes, extras)
    return templates.TemplateResponse(
        "ingest.html",
        _common_page(
            request,
            listing=listing,
            preview=None,
            drive_name=(listing or {}).get("suggested_drive", ""),
            drive_root=(listing or {}).get("suggested_root", ""),
            existing_drive=False,
        ),
    )


@router.post("/ingest/add-folder")
def ingest_add_folder(request: Request, folder: str = Form("")):
    raw = folder.strip()
    if not raw:
        set_notice(request, "Enter the folder for the disk you want to ingest.")
        return RedirectResponse("/ingest", status_code=303)
    path = Path(raw).expanduser()
    try:
        if not path.is_dir():
            set_notice(request, "That path is not a folder on this computer.")
            return RedirectResponse("/ingest", status_code=303)
        resolved = path.resolve()
    except OSError:
        set_notice(request, "Could not open that folder.")
        return RedirectResponse("/ingest", status_code=303)
    if is_forbidden_root(resolved):
        set_notice(request, "Choose the disk or folder you want to back up, not a system directory.")
        return RedirectResponse("/ingest", status_code=303)
    add_ingest_extra_root(str(resolved))
    set_notice(request, f"Added {resolved.name}. You can ingest it below.")
    return RedirectResponse(_browse_url(str(resolved)), status_code=303)


@router.get("/ingest/preview", response_class=HTMLResponse)
def ingest_preview(
    request: Request,
    path: str = "",
    drive_name: str = "",
    drive_root: str = "",
    mode: str = "upload",
    workers: int = DEFAULT_MAX_WORKERS,
    bucket: str = "",
):
    roots = _roots()
    volumes, extras = _volume_and_extra()
    source = resolve_allowed_path(path, roots)
    if source is None or not source.is_dir():
        set_notice(request, "Choose a disk or folder to ingest.")
        return RedirectResponse("/ingest", status_code=303)

    drive_name = _validate_drive_name(drive_name)
    if not drive_name:
        set_notice(request, "Drive name is required and cannot contain slashes.")
        return RedirectResponse(_browse_url(str(source)), status_code=303)

    suggested_name, suggested_root = suggest_drive(source, volumes, extras)
    root_path = suggested_root
    if drive_root.strip():
        candidate = resolve_allowed_path(drive_root, roots)
        if candidate is None or not candidate.is_dir():
            set_notice(request, "Drive root must be a folder we can see.")
            return RedirectResponse(_browse_url(str(source)), status_code=303)
        try:
            source.relative_to(candidate)
            root_path = candidate
        except ValueError:
            set_notice(request, "The selected folder is not inside the drive root.")
            return RedirectResponse(_browse_url(str(source)), status_code=303)

    mode = mode if mode in ("upload", "scan-only", "dry-run") else "upload"
    try:
        workers = max(1, min(int(workers), 64))
    except (TypeError, ValueError):
        workers = DEFAULT_MAX_WORKERS

    preview = preview_ingest(source, drive_name, root_path)
    listing = _listing(source, roots, volumes, extras)
    catalog = get_drive_names()
    return templates.TemplateResponse(
        "ingest.html",
        _common_page(
            request,
            listing=listing,
            preview=preview,
            drive_name=drive_name,
            drive_root=str(root_path),
            mode=mode,
            workers=workers,
            bucket=(bucket or get_bucket_name()).strip(),
            existing_drive=drive_name in catalog,
            suggested_drive=suggested_name,
        ),
    )


@router.post("/ingest/start")
def ingest_start(
    request: Request,
    path: str = Form(""),
    drive_name: str = Form(""),
    drive_root: str = Form(""),
    mode: str = Form("upload"),
    workers: int = Form(DEFAULT_MAX_WORKERS),
    bucket: str = Form(""),
):
    roots = _roots()
    source = resolve_allowed_path(path, roots)
    if source is None or not source.is_dir():
        set_notice(request, "Choose a disk or folder to ingest.")
        return RedirectResponse("/ingest", status_code=303)

    drive_name = _validate_drive_name(drive_name)
    if not drive_name:
        set_notice(request, "Drive name is required and cannot contain slashes.")
        return RedirectResponse(_browse_url(str(source)), status_code=303)

    volumes, extras = _volume_and_extra()
    _suggested_name, suggested_root = suggest_drive(source, volumes, extras)
    root_path = suggested_root
    if drive_root.strip():
        candidate = resolve_allowed_path(drive_root, roots)
        if candidate is None or not candidate.is_dir():
            set_notice(request, "Drive root must be a folder we can see.")
            return RedirectResponse(_browse_url(str(source)), status_code=303)
        try:
            source.relative_to(candidate)
            root_path = candidate
        except ValueError:
            set_notice(request, "The selected folder is not inside the drive root.")
            return RedirectResponse(_browse_url(str(source)), status_code=303)

    mode = mode if mode in ("upload", "scan-only", "dry-run") else "upload"
    scan_only = mode == "scan-only"
    dry_run = mode == "dry-run"
    try:
        workers = max(1, min(int(workers), 64))
    except (TypeError, ValueError):
        workers = DEFAULT_MAX_WORKERS

    bucket = (bucket or get_bucket_name() or "").strip()
    if bucket and bucket != get_bucket_name():
        set_bucket_name(bucket)
    if not scan_only and not bucket:
        set_notice(request, "Set a B2 bucket (or use scan-only, which does not upload).")
        return RedirectResponse(_browse_url(str(source)), status_code=303)

    job_id = enqueue(
        "ingest",
        {
            "source": str(source),
            "drive_name": drive_name,
            "drive_root": str(root_path),
            "bucket": bucket,
            "scan_only": scan_only,
            "dry_run": dry_run,
            "workers": workers,
        },
    )
    set_notice(request, f"Ingest job #{job_id} queued.")
    return RedirectResponse(f"/jobs/{job_id}", status_code=303)
