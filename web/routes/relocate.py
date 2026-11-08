"""Move a file or folder in the catalog and in B2."""
from __future__ import annotations

from urllib.parse import urlencode

from fastapi import APIRouter, Form, Query, Request
from fastapi.responses import RedirectResponse

from core.catalog import get_drive_names, get_file, list_subdirs
from core.config import get_bucket_name, normalize_drive_name
from core.jobs import enqueue
from core.relocate import (
    RelocateError,
    catalog_has_prefix,
    clean_catalog_path,
    clean_name,
    default_move_folder,
    destination_paths,
    plan_from_payload,
    preview_moves,
)
from web.templating import page_vars, set_notice, templates

router = APIRouter()

_MOVE_KEYS = (
    "kind",
    "drive",
    "path",
    "file_id",
    "dest_drive",
    "dest",
    "name",
    "new",
    "seek",
    "next",
)


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


def _move_href(params: dict) -> str:
    ordered = []
    for key in _MOVE_KEYS:
        if key not in params:
            continue
        value = params[key]
        if value is None:
            continue
        if value == "" and key not in {"dest", "path"}:
            continue
        ordered.append((key, str(value)))
    return "/catalog/move?" + urlencode(ordered)


def _join(parent: str, name: str) -> str:
    if not name:
        return parent
    if not parent:
        return name
    return f"{parent}/{name}"


def _parent(path: str) -> str:
    if not path or "/" not in path:
        return ""
    return path.rsplit("/", 1)[0]


def _inside(child: str, source: str) -> bool:
    if not source:
        return False
    return child == source or child.startswith(source + "/")


def _fields(pairs: list[tuple[str, str]]) -> list[dict]:
    return [{"name": name, "value": value} for name, value in pairs]


@router.get("/catalog/move")
def move_page(
    request: Request,
    kind: str = Query(""),
    drive: str = Query(""),
    path: str = Query(""),
    file_id: str = Query(""),
    dest_drive: str = Query(""),
    dest: str | None = Query(None),
    name: str = Query(""),
    new: str = Query(""),
    seek: str = Query(""),
    next: str = Query(""),
):
    """Pick an existing folder and show the path before anything moves."""
    fallback = "/search"
    back = _next_url(request, next, fallback)
    kind = (kind or "").strip()
    if kind not in {"folder", "contents", "file"}:
        set_notice(request, "That move is not valid.")
        return RedirectResponse(back, status_code=303)

    record = None
    source_drive = _drive(drive) or ""
    source_path = (path or "").strip()
    numeric_id = int(file_id) if str(file_id).strip().isdigit() else 0
    name_error = ""
    file_name = ""
    if kind == "file":
        record = get_file(numeric_id) if numeric_id else None
        if record is None:
            return templates.TemplateResponse(
                "not_found.html",
                page_vars(request, message="That file is not in the catalog."),
                status_code=404,
            )
        source_drive = record["drive_name"]
        file_name = record["name"]
        if (name or "").strip():
            try:
                file_name = clean_name(name)
            except RelocateError as exc:
                name_error = str(exc)
                file_name = record["name"]
    elif not source_drive:
        set_notice(request, "Drive name is required and cannot contain slashes.")
        return RedirectResponse(back, status_code=303)

    target = _drive(dest_drive, source_drive) or ""
    if not target:
        set_notice(request, "Drive name is required and cannot contain slashes.")
        return RedirectResponse(back, status_code=303)

    try:
        if dest is None:
            folder = default_move_folder(
                kind,
                "" if kind == "file" else source_path,
                record["file_path"] if record else "",
            )
        else:
            folder = clean_catalog_path(dest) if dest.strip() else ""
    except RelocateError as exc:
        set_notice(request, str(exc))
        return RedirectResponse(back, status_code=303)

    base = {
        "kind": kind,
        "dest_drive": target,
        "dest": folder,
        "next": back,
    }
    if kind == "file":
        base["file_id"] = str(record["id"])
        base["name"] = file_name
    else:
        base["drive"] = source_drive
        base["path"] = source_path

    new_error = ""
    new_name = ""
    if (new or "").strip():
        try:
            new_name = clean_name(new)
        except RelocateError as exc:
            new_error = str(exc)
    if new_name and catalog_has_prefix(target, _join(folder, new_name)):
        taken = dict(base)
        taken["dest"] = _join(folder, new_name)
        return RedirectResponse(_move_href(taken), status_code=303)

    into = _join(folder, new_name) if new_name else folder
    try:
        described = destination_paths(
            kind,
            source_drive,
            "" if kind == "file" else source_path,
            into,
            target,
            file_path=record["file_path"] if record else "",
            file_name=file_name,
        )
    except RelocateError as exc:
        set_notice(request, str(exc))
        return RedirectResponse(back, status_code=303)

    examples: list[dict] = []
    more_examples = False
    if not described["blocked"] and kind == "file":
        examples = [{"before": described["from_label"], "after": described["to_label"]}]
    elif not described["blocked"]:
        found = preview_moves(kind, source_drive, source_path, into, target, limit=4)
        more_examples = len(found) > 3
        examples = found[:3]
        if not examples:
            described = dict(described)
            described["blocked"] = "Nothing is stored at that path."
            described["to_label"] = ""
            described["placement"] = ""

    listed = list_subdirs(target, folder, seek=seek)
    nest_root = "" if kind == "file" or target != source_drive else described["source_path"]
    folders = []
    for child in listed["dirs"]:
        item = {"name": child["name"], "href": "", "note": ""}
        if _inside(child["path"], nest_root):
            if kind == "folder" and child["path"] == nest_root:
                item["note"] = "This is the folder you are moving."
            elif kind == "contents" and child["path"] == nest_root:
                item["note"] = "These files are already in this folder."
            else:
                item["note"] = "That folder is inside what you are moving."
        else:
            chosen = dict(base)
            chosen["dest"] = child["path"]
            item["href"] = _move_href(chosen)
        folders.append(item)

    crumbs = []
    at_top = not folder and not new_name
    root = dict(base)
    root["dest"] = ""
    crumbs.append({"label": target, "href": "" if at_top else _move_href(root)})
    walked: list[str] = []
    parts = folder.split("/") if folder else []
    for index, part in enumerate(parts):
        walked.append(part)
        here = "/".join(walked)
        current = index == len(parts) - 1 and not new_name
        crumb = dict(base)
        crumb["dest"] = here
        crumbs.append({"label": part, "href": "" if current else _move_href(crumb)})
    if new_name:
        crumbs.append({"label": new_name, "href": ""})

    if new_name:
        up_href = _move_href(base)
    elif folder:
        parent = dict(base)
        parent["dest"] = _parent(folder)
        up_href = _move_href(parent)
    else:
        up_href = ""

    more_href = ""
    if listed["next_seek"]:
        more = dict(base)
        more["seek"] = listed["next_seek"]
        more_href = _move_href(more)

    drives = list(get_drive_names())
    for extra in (source_drive, target):
        if extra and extra not in drives:
            drives.append(extra)

    stay = [("kind", kind), ("dest_drive", target), ("dest", folder), ("next", back)]
    if kind == "file":
        stay.append(("file_id", str(record["id"])))
    else:
        stay.extend([("drive", source_drive), ("path", source_path)])
    drive_hidden = [(key, "" if key == "dest" else value) for key, value in stay if key != "dest_drive"]
    if kind == "file":
        drive_hidden.append(("name", file_name))
        name_hidden = list(stay)
        new_hidden = [*stay, ("name", file_name)]
    else:
        name_hidden = list(stay)
        new_hidden = list(stay)
    if new_name:
        name_hidden.append(("new", new_name))

    post = []
    if not described["blocked"]:
        post = [
            ("kind", kind),
            ("drive", source_drive if kind != "file" else record["drive_name"]),
            ("path", "" if kind == "file" else described["source_path"]),
            ("into", "" if kind == "file" else described["into"]),
            ("dest_drive", target),
            ("dest_path", described["catalog_dest"] if kind == "file" else ""),
            ("file_id", str(record["id"]) if record else ""),
            ("next", back),
        ]

    return templates.TemplateResponse(
        "move.html",
        page_vars(
            request,
            title=described["title"],
            from_label=described["from_label"],
            to_label=described["to_label"],
            placement=described["placement"],
            blocked=described["blocked"],
            examples=examples,
            more_examples=more_examples,
            crumbs=crumbs,
            up_href=up_href,
            folders=folders,
            more_href=more_href,
            where_label=f"{target}/{folder}" if folder else target,
            drives=drives,
            dest_drive=target,
            drive_fields=_fields(drive_hidden),
            name_fields=_fields(name_hidden),
            new_fields=_fields(new_hidden),
            post_fields=_fields([item for item in post if item[0] != "file_id" or item[1]]),
            file_name=file_name,
            show_name=kind == "file",
            name_error=name_error,
            name_open=bool(name_error or (name or "").strip()),
            new_name=new_name,
            new_error=new_error,
            new_open=bool(new_name or new_error or (new or "").strip()),
            creating=bool(new_name),
            back_href=back,
        ),
    )


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
