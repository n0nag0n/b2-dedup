"""GET /browse — folder listing for one drive."""
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from core.catalog import get_drive_names, list_folder_files, list_subdirs
from core import restore_set
from web.templating import page_vars, session_key, templates

router = APIRouter()


def _crumbs(drive: str, path: str) -> list[dict]:
    crumbs = [{"label": drive, "path": ""}]
    if not path:
        return crumbs
    parts = [p for p in path.replace("\\", "/").strip("/").split("/") if p]
    acc = []
    for part in parts:
        acc.append(part)
        crumbs.append({"label": part, "path": "/".join(acc)})
    return crumbs


@router.get("/browse", response_class=HTMLResponse)
def browse_page(
    request: Request,
    drive: str = "",
    path: str = "",
    page: int = 1,
    seek: str = "",
    hidden: str = "",
):
    drives = get_drive_names()
    if not drive:
        return templates.TemplateResponse(
            "browse.html",
            page_vars(request, drive="", path="", drives=drives, listing=None),
        )

    hide_hidden = hidden != "1"
    prefix = path.strip("/")
    dirs = list_subdirs(drive, prefix, hide_hidden=hide_hidden, seek=seek)
    files = list_folder_files(drive, prefix, hide_hidden=hide_hidden, page=page)
    in_set = set(restore_set.list_ids(session_key(request)))
    for item in files["files"]:
        item["in_set"] = item["id"] in in_set

    parent = ""
    if prefix:
        parent = str(Path(prefix).parent)
        if parent == ".":
            parent = ""

    return templates.TemplateResponse(
        "browse.html",
        page_vars(
            request,
            drive=drive,
            path=prefix,
            parent=parent,
            crumbs=_crumbs(drive, prefix),
            drives=drives,
            dirs=dirs["dirs"],
            next_seek=dirs["next_seek"],
            files=files,
            hide_hidden=hide_hidden,
            listing=True,
        ),
    )
