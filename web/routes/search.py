"""GET /search — name search, one drive's folders, and moments."""
from pathlib import Path
from urllib.parse import urlencode

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse

from core import restore_set
from core.catalog import (
    get_drive_names,
    get_file_types,
    list_folder_files,
    list_subdirs,
    search_files,
)
from core.library import (
    MONTH_NAMES,
    list_moment_photos,
    list_moments,
    list_months,
    list_years,
)
from web.templating import page_vars, session_key, templates

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
