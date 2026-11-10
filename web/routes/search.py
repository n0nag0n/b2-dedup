"""GET /search — name search, one drive's folders, and moments."""
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse

from core import restore_set
from core.bulk import (
    MAX_CHECKED_FILES,
    MAX_CHECKED_FOLDERS,
    collect_records,
    download_mode,
    parse_file_ids,
    parse_folders,
    save_pick,
)
from core.catalog import (
    get_drive_names,
    get_file_types,
    list_folder_files,
    list_subdirs,
    search_files,
)
from core.config import get_bucket_name
from core.download import content_disposition, shared_manager, stream_zip
from core.jobs import enqueue
from core.library import (
    MONTH_NAMES,
    list_moment_photos,
    list_moments,
    list_months,
    list_years,
)
from web.templating import page_vars, session_key, set_notice, templates

router = APIRouter()

MONTHS = [
    {"value": f"{index:02d}", "name": name}
    for index, name in enumerate(MONTH_NAMES, start=1)
]


def _crumbs(drive: str, path: str) -> list[dict]:
    crumbs = [{"label": drive, "path": ""}]
    parts = [part for part in path.replace("\\", "/").strip("/").split("/") if part]
    acc: list[str] = []
    for part in parts:
        acc.append(part)
        crumbs.append({"label": part, "path": "/".join(acc)})
    return crumbs


def _parent(path: str) -> str:
    if not path:
        return ""
    parent = str(Path(path).parent)
    return "" if parent == "." else parent


def _kept_query(**params: str) -> str:
    clean = {key: value for key, value in params.items() if value}
    return urlencode(clean)


def _mark_in_set(request: Request, files: list[dict]) -> None:
    in_set = set(restore_set.list_ids(session_key(request)))
    for item in files:
        item["in_set"] = item["id"] in in_set


@router.get("/search", response_class=HTMLResponse)
def search_page(
    request: Request,
    q: str = "",
    drive: str = "",
    path: str = "",
    file_type: str = Query("", alias="type"),
    origin: str = "",
    ext: str = "",
    year: str = "",
    month: str = "",
    hidden: str = "",
    sort: str = "path",
    view: str = "files",
    title: str = "",
    day: str = "",
    folder: str = "",
    page: int = 1,
    fpage: int = 1,
    seek: str = "",
):
    """One catalog page.

    A name query searches every drive. A drive with no query walks that
    drive's folders. Moments group the same folder across drives by date.
    """
    q = (q or "").strip()
    drive = (drive or "").strip()
    path = (path or "").replace("\\", "/").strip("/")
    month = (month or "").strip()
    if month.isdigit() and 1 <= int(month) <= 12:
        month = f"{int(month):02d}"
    elif month:
        month = ""
    view = (view or "files").strip().lower()
    if view not in {"files", "moments", "moment"}:
        view = "files"
    if view == "moment" and not (title or day or folder):
        view = "moments"
    hide_hidden = hidden != "1"
    if not drive and not q:
        path = ""

    mode = "start"
    results = None
    folder_files = None
    dirs: list[dict] = []
    next_seek = ""
    years: list[dict] = []
    months: list[dict] = []
    moments: list[dict] = []
    moment = None

    if view == "moment":
        mode = "moment"
        moment = list_moment_photos(
            folder,
            day,
            title=title,
            file_type=file_type,
            hide_hidden=hide_hidden,
            page=page,
        )
        if not year and day:
            year = day[:4]
        if not month and day and len(day) >= 7:
            month = day[5:7]
    elif view == "moments":
        mode = "moments"
        years = list_years(file_type=file_type, hide_hidden=hide_hidden)
        if not year and "year" not in request.query_params and years:
            year = max(years, key=lambda item: item["count"])["year"]
        months = (
            list_months(year, file_type=file_type, hide_hidden=hide_hidden) if year else []
        )
        if year and not month and "month" not in request.query_params and months:
            month = months[-1]["month"]
        moments = list_moments(
            file_type=file_type,
            year=year,
            month=month,
            drive=drive,
            q=q,
            hide_hidden=hide_hidden,
        )
    elif q:
        mode = "search"
        results = search_files(
            q,
            drive=drive,
            path=path,
            file_type=file_type,
            origin=origin,
            ext=ext,
            year=year,
            month=month,
            hide_hidden=hide_hidden,
            sort=sort,
            page=page,
            folder_page=fpage,
        )
        _mark_in_set(request, results["files"])
    elif drive:
        mode = "folder"
        listed = list_subdirs(drive, path, hide_hidden=hide_hidden, seek=seek)
        dirs = listed["dirs"]
        next_seek = listed["next_seek"]
        folder_files = list_folder_files(
            drive,
            path,
            hide_hidden=hide_hidden,
            file_type=file_type,
            origin=origin,
            ext=ext,
            year=year,
            month=month,
            sort=sort,
            page=page,
        )
        _mark_in_set(request, folder_files["files"])

    walk = _kept_query(
        type=file_type,
        origin=origin,
        ext=ext,
        year=year,
        month=month,
        sort="" if sort == "path" else sort,
        hidden="" if hide_hidden else "1",
    )
    moment_keep = _kept_query(
        type=file_type,
        drive=drive,
        q=q,
        hidden="" if hide_hidden else "1",
    )
    return templates.TemplateResponse(
        "search.html",
        page_vars(
            request,
            q=q,
            drive=drive,
            path=path,
            parent=_parent(path) if drive else "",
            crumbs=_crumbs(drive, path) if drive else [],
            file_type=file_type,
            origin=origin,
            ext=ext,
            year=year,
            month=month,
            hide_hidden=hide_hidden,
            sort=sort,
            view=view,
            form_view="moments" if view in {"moments", "moment"} else "files",
            mode=mode,
            results=results,
            dirs=dirs,
            next_seek=next_seek,
            folder_files=folder_files,
            years=years,
            months=months,
            moments=moments,
            moment=moment,
            title=title,
            day=day,
            folder=folder,
            walk=walk,
            moment_keep=moment_keep,
            drives=get_drive_names(),
            types=get_file_types(),
            month_choices=MONTHS,
        ),
    )


def _back(request: Request, nxt: str) -> str:
    nxt = (nxt or "").strip()
    if nxt.startswith("/") and not nxt.startswith("//"):
        return nxt
    referer = request.headers.get("referer") or ""
    if referer.startswith("/") and not referer.startswith("//"):
        return referer
    return "/search"


def _added_note(added: int, total: int) -> str:
    if added:
        noun = "file" if added == 1 else "files"
        return f"Added {added:,} {noun} to the restore set."
    if total:
        return "Already in the restore set."
    return "Nothing in that selection is in the catalog."


@router.post("/search/bulk")
async def search_bulk(request: Request):
    """Download, move, or save the checkboxes on the current search page."""
    form = await request.form()
    action = str(form.get("action") or "").strip()
    back = _back(request, str(form.get("next") or ""))
    file_ids = parse_file_ids(form.getlist("file_id"))
    folders = parse_folders(form.getlist("folder"))
    if folders is None:
        set_notice(request, "That folder selection is not valid.")
        return RedirectResponse(back, status_code=303)
    if len(file_ids) > MAX_CHECKED_FILES or len(folders) > MAX_CHECKED_FOLDERS:
        set_notice(request, "That selection is too large. Select fewer items.")
        return RedirectResponse(back, status_code=303)
    if not file_ids and not folders:
        set_notice(request, "Select at least one file or folder.")
        return RedirectResponse(back, status_code=303)

    if action == "move":
        if len(file_ids) == 1 and not folders:
            query = urlencode({"kind": "file", "file_id": file_ids[0], "next": back})
            return RedirectResponse(f"/catalog/move?{query}", status_code=303)
        if len(folders) == 1 and not file_ids:
            drive, path = folders[0]
            query = urlencode({"kind": "folder", "drive": drive, "path": path, "next": back})
            return RedirectResponse(f"/catalog/move?{query}", status_code=303)
        save_pick(session_key(request), file_ids, folders)
        query = urlencode({"kind": "selection", "next": back})
        return RedirectResponse(f"/catalog/move?{query}", status_code=303)

    records = collect_records(file_ids, folders)
    ids = [int(row["id"]) for row in records]
    if action == "restore":
        added = restore_set.add_ids(session_key(request), ids) if ids else 0
        set_notice(request, _added_note(added, len(ids)))
        return RedirectResponse(back, status_code=303)
    if action != "download":
        set_notice(request, "That action is not valid.")
        return RedirectResponse(back, status_code=303)
    if not ids:
        set_notice(request, "Nothing in that selection is in the catalog.")
        return RedirectResponse(back, status_code=303)

    plan = download_mode(records)
    if plan["mode"] == "rejected":
        set_notice(request, plan["error"])
        return RedirectResponse(back, status_code=303)
    added = restore_set.add_ids(session_key(request), plan["ids"])
    note = _added_note(added, len(plan["ids"]))
    if plan["mode"] == "file":
        set_notice(request, note)
        return RedirectResponse(f"/download/{plan['ids'][0]}", status_code=303)
    if plan["mode"] == "rejected":
        set_notice(request, plan["error"])
        return RedirectResponse(back, status_code=303)
    if plan["mode"] == "restore":
        set_notice(
            request,
            note + " It is large, so download the ZIP from the restore set.",
        )
        return RedirectResponse("/restore-set", status_code=303)

    bucket = get_bucket_name()
    if not bucket:
        set_notice(request, "Set a B2 bucket in Settings before downloading. " + note)
        return RedirectResponse("/settings#connection", status_code=303)
    try:
        b2 = shared_manager(bucket)
    except Exception as exc:  # noqa: BLE001 - credentials / network
        set_notice(request, note + f" Could not connect to B2: {exc}")
        return RedirectResponse(back, status_code=303)

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
