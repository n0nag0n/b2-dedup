"""GET /file/{id} — inspector + other copies."""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from core.catalog import get_copies, get_file
from core import restore_set
from core.thumbs import thumb_url
from web.templating import page_vars, session_key, templates

router = APIRouter()


@router.get("/file/{file_id}", response_class=HTMLResponse)
def file_page(request: Request, file_id: int):
    rec = get_file(file_id)
    if not rec:
        return templates.TemplateResponse(
            "not_found.html",
            page_vars(request, message="That file is not in the catalog."),
            status_code=404,
        )
    copies = get_copies(rec["hash"], exclude_id=rec["id"]) if rec.get("hash") else {
        "total": 1,
        "copies": [],
    }
    rec["thumb_url"] = thumb_url(rec.get("hash") or "")
    return templates.TemplateResponse(
        "file.html",
        page_vars(
            request,
            file=rec,
            copies=copies,
            in_set=restore_set.contains(session_key(request), rec["id"]),
        ),
    )
