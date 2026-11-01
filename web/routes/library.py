"""Library grid, timeline / moments, and thumbnail files."""
from __future__ import annotations

import re

from fastapi import APIRouter, Query, Request
from fastapi.responses import FileResponse, HTMLResponse

from core.catalog import get_drive_names, get_file_types
from core.library import (
    dates_status,
    list_library,
    list_moment_photos,
    list_moments,
    list_months,
    list_years,
)
from core.paths import THUMBS_DIR
from core.thumbs import thumbs_status
from web.templating import page_vars, templates

router = APIRouter()
_HASH = re.compile(r"^[0-9a-f]{64}$")


def _library_context(request: Request, file_type: str = "", **extra):
    years = list_years(file_type=file_type)
    extra.setdefault("file_type", file_type)
    extra.setdefault("types", get_file_types())
    extra.setdefault("years", years)
    return page_vars(
        request,
        dates=dates_status(),
        thumbs=thumbs_status(),
        drives=get_drive_names(),
        **extra,
    )


@router.get("/library", response_class=HTMLResponse)
def library_page(
    request: Request,
    year: str = "",
    month: str = "",
    drive: str = "",
    file_type: str = Query("", alias="type"),
    page: int = 1,
):
    results = list_library(
        file_type=file_type, year=year, month=month, drive=drive, page=page
    )
    months = list_months(year, file_type=file_type) if year else []
    return templates.TemplateResponse(
        "library.html",
        _library_context(
            request,
            file_type=file_type,
            view="grid",
            year=year,
            month=month,
            drive=drive,
            months=months,
            results=results,
        ),
    )


@router.get("/timeline", response_class=HTMLResponse)
def timeline_page(
    request: Request,
    year: str = "",
    month: str = "",
    file_type: str = Query("", alias="type"),
):
    years = list_years(file_type=file_type)
    if not year and years:
        year = max(years, key=lambda item: item["count"])["year"]
    months = list_months(year, file_type=file_type) if year else []
    if year and not month and months:
        month = months[-1]["month"]
    moments = list_moments(file_type=file_type, year=year, month=month) if year else []
    return templates.TemplateResponse(
        "timeline.html",
        _library_context(
            request,
            file_type=file_type,
            years=years,
            view="timeline",
            year=year,
            month=month,
            months=months,
            moments=moments,
        ),
    )


@router.get("/moment", response_class=HTMLResponse)
def moment_page(
    request: Request,
    folder: str = "",
    title: str = "",
    day: str = "",
    file_type: str = Query("", alias="type"),
    page: int = 1,
):
    results = list_moment_photos(
        folder, day, title=title, file_type=file_type, page=page
    )
    return templates.TemplateResponse(
        "moment.html",
        _library_context(
            request,
            file_type=file_type,
            view="moment",
            year=day[:4] if day else "",
            month=day[5:7] if day and len(day) >= 7 else "",
            results=results,
            folder=folder,
            day=day,
        ),
    )


@router.get("/thumbs/{file_hash}")
def thumb_file(file_hash: str):
    if not _HASH.fullmatch(file_hash):
        return HTMLResponse("Not found", status_code=404)
    path = THUMBS_DIR / file_hash[:2] / f"{file_hash}.jpg"
    if not path.is_file():
        return HTMLResponse("Not found", status_code=404)
    return FileResponse(path, media_type="image/jpeg", headers={"Cache-Control": "public, max-age=86400"})
