"""GET /file/{id} — inspector, inline preview, and other copies."""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, Response, StreamingResponse

from core import restore_set
from core.catalog import get_copies, get_file
from core.config import get_bucket_name
from core.download import (
    content_disposition,
    open_remote,
    resolve_source,
    shared_manager,
    stream_downloaded_file,
)
from core.preview import (
    SVG_CSP,
    TEXT_MAX_BYTES,
    UnsatisfiableRange,
    decode_preview_text,
    describe_preview,
    parse_byte_range,
    preview_etag,
    preview_media_type,
)
from core.thumbs import thumb_url
from web.templating import page_vars, session_key, templates

router = APIRouter()

_STREAM_KINDS = frozenset({"image", "pdf", "audio", "video"})


def _plain(message: str, status_code: int) -> PlainTextResponse:
    return PlainTextResponse(
        message,
        status_code=status_code,
        headers={"X-Content-Type-Options": "nosniff", "Cache-Control": "private, no-store"},
    )


def _preview_headers(rec: dict, filename: str, *, length: int | None = None) -> dict[str, str]:
    headers = {
        "Content-Disposition": content_disposition(filename, inline=True),
        "X-Content-Type-Options": "nosniff",
        "Cache-Control": "private, max-age=300",
    }
    if length:
        headers["Content-Length"] = str(length)
    etag = preview_etag(rec)
    if etag:
        headers["ETag"] = etag
    return headers


def _read_limited(downloaded, limit: int) -> bytes:
    """Pull a small preview into memory and stop the B2 pump once the cap is hit."""
    chunks: list[bytes] = []
    total = 0
    generator = stream_downloaded_file(downloaded)
    try:
        for chunk in generator:
            total += len(chunk)
            if total > limit:
                raise ValueError("file is too large to preview")
            chunks.append(chunk)
    finally:
        generator.close()
    return b"".join(chunks)


def load_text_preview(rec: dict) -> str:
    """Download a small text file and return it as Unicode.

    The file page inlines this so the text is visible without a second round
    trip. HTML in the vault is still plain text; the template escapes it.
    """
    bucket = get_bucket_name()
    if not bucket:
        raise RuntimeError("Set a B2 bucket in Settings before previewing.")
    b2 = shared_manager(bucket)
    source = resolve_source(b2, rec)
    downloaded = open_remote(b2, source)
    return decode_preview_text(_read_limited(downloaded, TEXT_MAX_BYTES))


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
    preview = describe_preview(rec, thumb_available=bool(rec["thumb_url"]))
    text_preview = ""
    text_error = ""
    if preview["kind"] == "text":
        try:
            text_preview = load_text_preview(rec)
        except ValueError as exc:
            text_error = str(exc)
        except Exception:  # noqa: BLE001 - missing object, credentials, network
            text_error = "Could not load this preview from B2."
    return templates.TemplateResponse(
        "file.html",
        page_vars(
            request,
            file=rec,
            copies=copies,
            preview=preview,
            text_preview=text_preview,
            text_error=text_error,
            in_set=restore_set.contains(session_key(request), rec["id"]),
        ),
    )


@router.get("/preview/{file_id}")
def preview_file(request: Request, file_id: int):
    """Stream one file inline so the file page can show it.

    Text is decoded and returned as ``text/plain`` so HTML in the vault
    cannot run as a page. Images, PDF, audio, and video stream with Range
    support so a player can seek.
    """
    rec = get_file(file_id)
    if not rec:
        return _plain("That file is not in the catalog.", 404)

    plan = describe_preview(rec)
    kind = plan["kind"]
    if kind not in _STREAM_KINDS and kind != "text":
        return _plain("This file cannot be previewed.", 404)

    filename = rec.get("name") or rec["file_path"]
    if kind == "text":
        return _preview_text(rec, filename)

    bucket = get_bucket_name()
    if not bucket:
        return _plain("Set a B2 bucket in Settings before previewing.", 409)

    try:
        b2 = shared_manager(bucket)
    except Exception as exc:  # noqa: BLE001 - credentials / network
        return _plain(f"Could not connect to B2: {exc}", 502)

    try:
        source = resolve_source(b2, rec)
    except Exception as exc:  # noqa: BLE001 - missing pointer, bad JSON
        return _plain(
            f"B2 does not have the bytes for {rec['drive_name']}/{rec['file_path']}: {exc}",
            404,
        )

    return _preview_stream(request, b2, source, rec, filename, kind)


def _preview_text(rec: dict, filename: str):
    try:
        text = load_text_preview(rec)
    except ValueError as exc:
        status = 413 if "too large" in str(exc) else 415
        return _plain(str(exc), status)
    except Exception as exc:  # noqa: BLE001 - missing object, no creds
        return _plain(
            f"B2 does not have the bytes for {rec['drive_name']}/{rec['file_path']}: {exc}",
            404,
        )
    body = text.encode("utf-8")
    headers = _preview_headers(rec, filename, length=len(body))
    return Response(content=body, media_type="text/plain; charset=utf-8", headers=headers)


def _preview_stream(request: Request, b2, source: str, rec: dict, filename: str, kind: str):
    size = int(rec.get("size") or 0)
    try:
        byte_range = parse_byte_range(request.headers.get("range"), size)
    except UnsatisfiableRange:
        return Response(
            status_code=416,
            headers={
                "Content-Range": f"bytes */{size}",
                "Accept-Ranges": "bytes",
                "X-Content-Type-Options": "nosniff",
                "Cache-Control": "private, no-store",
            },
        )

    try:
        downloaded = open_remote(b2, source, byte_range=byte_range)
    except Exception as exc:  # noqa: BLE001 - missing object, bad range, no creds
        return _plain(
            f"B2 does not have the bytes for {rec['drive_name']}/{rec['file_path']}: {exc}",
            404,
        )

    headers = _preview_headers(rec, filename)
    headers["Accept-Ranges"] = "bytes"
    status = 200
    if byte_range is not None:
        start, end = byte_range
        status = 206
        headers["Content-Length"] = str(end - start + 1)
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"
    else:
        try:
            length = int(downloaded.download_version.content_length or size or 0)
        except Exception:  # noqa: BLE001 - fall back to the catalog size
            length = size
        if length:
            headers["Content-Length"] = str(length)
    media_type = preview_media_type(rec, kind)
    if media_type == "image/svg+xml":
        headers["Content-Security-Policy"] = SVG_CSP

    return StreamingResponse(
        stream_downloaded_file(downloaded),
        status_code=status,
        media_type=media_type,
        headers=headers,
    )
