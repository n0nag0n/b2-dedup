"""GET /search — global FTS across every drive."""
from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse

from core.catalog import get_drive_names, get_file_types, search_files
from web.templating import page_vars, session_key, templates
from core import restore_set

router = APIRouter()


@router.get("/search", response_class=HTMLResponse)
def search_page(
    request: Request,
    q: str = "",
    drive: str = "",
    file_type: str = Query("", alias="type"),
    origin: str = "",
    ext: str = "",
    year: str = "",
    hidden: str = "",
    sort: str = "path",
    page: int = 1,
    fpage: int = 1,
):
    hide_hidden = hidden != "1"
    results = None
    if q.strip():
        results = search_files(
            q,
            drive=drive,
            file_type=file_type,
            origin=origin,
            ext=ext,
            year=year,
            hide_hidden=hide_hidden,
            sort=sort,
            page=page,
            folder_page=fpage,
        )
        in_set = set(restore_set.list_ids(session_key(request)))
        for item in results["files"]:
            item["in_set"] = item["id"] in in_set

    return templates.TemplateResponse(
        "search.html",
        page_vars(
            request,
            q=q,
            drive=drive,
            file_type=file_type,
            origin=origin,
            ext=ext,
            year=year,
            hide_hidden=hide_hidden,
            sort=sort,
            results=results,
            drives=get_drive_names(),
            types=get_file_types(),
        ),
    )
