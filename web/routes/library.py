"""Old library and timeline links, plus thumbnail files."""
from __future__ import annotations

import re
from urllib.parse import urlencode

from fastapi import APIRouter, Query
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse

from core.paths import THUMBS_DIR

router = APIRouter()
_HASH = re.compile(r"^[0-9a-f]{64}$")


def _target(**params: str) -> str:
    clean = {key: value for key, value in params.items() if value not in ("", None)}
    query = urlencode(clean)
    return "/search" if not query else f"/search?{query}"


@router.get("/library")
def library_page(
    year: str = "",
    month: str = "",
    drive: str = "",
    file_type: str = Query("", alias="type"),
    page: int = 1,
):
    # A drive alone opens that drive. A type or date opens moments.
    view = "moments" if (year or month or file_type) else ""
    return RedirectResponse(
        _target(
            view=view,
            type=file_type,
            year=year,
            month=month,
            drive=drive,
            page=str(page) if page and int(page) > 1 else "",
        ),
        status_code=302,
    )


@router.get("/timeline")
def timeline_page(
    year: str = "",
    month: str = "",
    file_type: str = Query("", alias="type"),
):
    return RedirectResponse(
        _target(view="moments", type=file_type, year=year, month=month),
        status_code=302,
    )


@router.get("/moment")
def moment_page(
    folder: str = "",
    title: str = "",
    day: str = "",
    file_type: str = Query("", alias="type"),
    page: int = 1,
):
    return RedirectResponse(
        _target(
            view="moment",
            folder=folder,
            title=title,
            day=day,
            type=file_type,
            page=str(page) if page and int(page) > 1 else "",
        ),
        status_code=302,
    )


@router.get("/thumbs/{file_hash}")
def thumb_file(file_hash: str):
    if not _HASH.fullmatch(file_hash):
        return HTMLResponse("Not found", status_code=404)
    path = THUMBS_DIR / file_hash[:2] / f"{file_hash}.jpg"
    if not path.is_file():
        return HTMLResponse("Not found", status_code=404)
    return FileResponse(path, media_type="image/jpeg", headers={"Cache-Control": "public, max-age=86400"})
