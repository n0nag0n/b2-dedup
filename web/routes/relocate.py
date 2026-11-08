"""Move a file or folder in the catalog and in B2."""
from __future__ import annotations

from fastapi import APIRouter, Form, Request
from fastapi.responses import RedirectResponse

from core.config import get_bucket_name, normalize_drive_name
from core.jobs import enqueue
from core.relocate import RelocateError, plan_from_payload
from web.templating import set_notice

router = APIRouter()


def _next_url(request: Request, nxt: str, fallback: str) -> str:
    nxt = (nxt or "").strip()
    if nxt.startswith("/") and not nxt.startswith("//"):
        return nxt
    referer = request.headers.get("referer") or ""
    if referer.startswith("/") and not referer.startswith("//"):
        return referer
    return fallback


def _drive(raw: str, fallback: str = "") -> str | None:
    text = (raw or "").strip() or fallback
    return normalize_drive_name(text)


@router.post("/catalog/move")
def catalog_move(
    request: Request,
    kind: str = Form(...),
    drive: str = Form(""),
    path: str = Form(""),
    into: str = Form(""),
    dest_drive: str = Form(""),
    dest_path: str = Form(""),
    file_id: str = Form(""),
    next: str = Form(""),
):
    fallback = "/search"
    back = _next_url(request, next, fallback)
    source_drive = _drive(drive)
    target_drive = _drive(dest_drive, source_drive or "")
    if kind in {"folder", "contents"} and not source_drive:
        set_notice(request, "Drive name is required and cannot contain slashes.")
        return RedirectResponse(back, status_code=303)
    if kind == "file":
        target_drive = _drive(dest_drive)
    if not target_drive:
        set_notice(request, "Drive name is required and cannot contain slashes.")
        return RedirectResponse(back, status_code=303)

    payload = {
        "kind": (kind or "").strip(),
        "source_drive": source_drive or "",
        "source_path": (path or "").strip(),
        "into": (into or "").strip(),
        "dest_drive": target_drive,
        "dest_path": (dest_path or "").strip(),
        "file_id": int(file_id) if str(file_id).strip().isdigit() else 0,
        "bucket": get_bucket_name(),
    }
    try:
        plan = plan_from_payload(payload)
    except RelocateError as exc:
        set_notice(request, str(exc))
        return RedirectResponse(back, status_code=303)
    except ValueError as exc:
        set_notice(request, str(exc))
        return RedirectResponse(back, status_code=303)

    if plan["needs_b2"] and not payload["bucket"]:
        set_notice(request, "Set a B2 bucket in Settings first. Nothing was moved.")
        return RedirectResponse(back, status_code=303)

    job_id = enqueue("relocate", payload)
    set_notice(
        request,
        f"Move job #{job_id} queued ({plan['count']} file(s) to {plan['dest_drive']}).",
    )
    return RedirectResponse(f"/jobs/{job_id}", status_code=303)
