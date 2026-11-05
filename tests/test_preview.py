"""Inline file previews: what can render, and the /preview stream."""
import types

import pytest

from core import download as dl
from core.preview import (
    IMAGE_MAX_BYTES,
    TEXT_MAX_BYTES,
    UNPLAYABLE_VIDEO_NOTE,
    UnsatisfiableRange,
    decode_preview_text,
    describe_preview,
    parse_byte_range,
    preview_media_type,
)


def rec(**over):
    base = {
        "id": 7,
        "name": "dice.jpg",
        "file_path": "photos/dice.jpg",
        "parent_path": "photos",
        "drive_name": "DriveA",
        "size": 1024,
        "size_fmt": "1.0 KB",
        "mime_type": "image/jpeg",
        "file_type": "Image",
        "is_original": True,
        "upload_path": "DriveA/photos/dice.jpg",
        "hash": "ab" * 32,
        "mtime_fmt": "2000-12-14 18:00",
        "taken_fmt": "",
        "taken_at_source": None,
    }
    base.update(over)
    return base


class FakeDownloaded:
    def __init__(self, data: bytes):
        self.data = data
        self.download_version = types.SimpleNamespace(content_length=len(data))
        self.response = None

    def save(self, file, allow_seeking=None):
        file.write(self.data)


# ---------------------------------------------------------------- classify


def test_browser_image_text_pdf_and_audio():
    assert describe_preview(rec())["kind"] == "image"
    assert describe_preview(rec(name="notes.txt", mime_type="text/plain"))["kind"] == "text"
    assert describe_preview(rec(name="page.html", mime_type="text/html"))["kind"] == "text"
    assert describe_preview(rec(name="doc.pdf", mime_type="application/pdf"))["kind"] == "pdf"
    assert describe_preview(rec(name="song.mp3", mime_type="audio/mpeg"))["kind"] == "audio"
    assert describe_preview(rec(name="readme", mime_type="application/octet-stream"))["kind"] == "text"
    assert describe_preview(rec(name="archive.zip", mime_type="application/zip"))["kind"] == "none"


def test_videos_preview_even_when_they_are_large():
    huge = 4 * 1024 ** 3
    for name, mime in (
        ("clip.mp4", "video/mp4"),
        ("clip.MP4", "video/mp4"),
        ("clip.mov", "video/quicktime"),
        ("clip.m4v", "video/mp4"),
        ("clip.webm", "video/webm"),
    ):
        plan = describe_preview(rec(name=name, file_path=f"Videos/{name}", mime_type=mime, size=huge))
        assert plan == {"kind": "video", "note": ""}, name


def test_unplayable_video_explains_instead_of_embedding_a_player():
    plan = describe_preview(rec(name="old.avi", mime_type="video/x-msvideo", file_type="Video"))
    assert plan["kind"] == "none"
    assert plan["note"] == UNPLAYABLE_VIDEO_NOTE
    mkv = describe_preview(rec(name="film.mkv", mime_type="video/x-matroska"))
    assert mkv["kind"] == "none"


def test_extension_wins_over_a_misleading_mime():
    plan = describe_preview(rec(name="clip.mp4", mime_type="application/octet-stream"))
    assert plan["kind"] == "video"
    assert preview_media_type(rec(name="clip.mov", mime_type="application/octet-stream"), "video") == (
        "video/quicktime"
    )


def test_text_and_image_have_a_size_cap():
    text = describe_preview(rec(name="big.log", mime_type="text/plain", size=TEXT_MAX_BYTES + 1))
    assert text["kind"] == "none"
    assert "too large to preview" in text["note"]
    image = describe_preview(rec(size=IMAGE_MAX_BYTES + 1))
    assert image["kind"] == "none"
    assert "too large" in image["note"]


def test_oversized_image_falls_back_to_a_thumbnail():
    plan = describe_preview(rec(size=IMAGE_MAX_BYTES + 1), thumb_available=True)
    assert plan["kind"] == "thumb"
    assert "thumbnail" in plan["note"]


def test_raw_uses_thumbnail_when_one_exists():
    plan = describe_preview(
        rec(name="shot.cr2", mime_type="image/x-canon-cr2", file_type="Image"),
        thumb_available=True,
    )
    assert plan["kind"] == "thumb"
    missing = describe_preview(rec(name="shot.cr2", mime_type="image/x-canon-cr2"))
    assert missing["kind"] == "none"
    assert "does not display" in missing["note"]


def test_html_preview_type_is_plain_text():
    assert preview_media_type(rec(name="page.html", mime_type="text/html"), "text").startswith(
        "text/plain"
    )


def test_decode_preview_text_utf8_bom_and_utf16():
    assert decode_preview_text("hi café".encode()) == "hi café"
    assert decode_preview_text(b"\xef\xbb\xbfhello") == "hello"
    assert decode_preview_text("hi".encode("utf-16")) == "hi"
    with pytest.raises(ValueError):
        decode_preview_text(b"\x00\x01\x02 not text")


def test_parse_byte_range():
    assert parse_byte_range(None, 10) is None
    assert parse_byte_range("bytes=0-3", 10) == (0, 3)
    assert parse_byte_range("bytes=4-", 10) == (4, 9)
    assert parse_byte_range("bytes=-4", 10) == (6, 9)
    assert parse_byte_range("bytes=0-99", 10) == (0, 9)
    assert parse_byte_range("bytes=0-1,2-3", 10) is None
    assert parse_byte_range("bytes=0-", 0) is None
    with pytest.raises(UnsatisfiableRange):
        parse_byte_range("bytes=10-12", 10)
    with pytest.raises(UnsatisfiableRange):
        parse_byte_range("bytes=5-4", 10)


def test_open_remote_forwards_an_inclusive_range():
    seen = {}

    class Bucket:
        def download_file_by_name(self, name, range_=None):
            seen["call"] = (name, range_)
            return "downloaded"

    b2 = types.SimpleNamespace(bucket=Bucket())
    assert dl.open_remote(b2, "Drive/clip.mp4", byte_range=(0, 10)) == "downloaded"
    assert seen["call"] == ("Drive/clip.mp4", (0, 10))
    dl.open_remote(b2, "Drive/clip.mp4")
    assert seen["call"] == ("Drive/clip.mp4", None)


def test_inline_content_disposition():
    header = dl.content_disposition("café.mp4", inline=True)
    assert header.startswith("inline;")
    assert "filename*=UTF-8''caf%C3%A9.mp4" in header


# ---------------------------------------------------------------- routes


def _patch_catalog(monkeypatch, record):
    from web.routes import files as routes

    monkeypatch.setattr(routes, "get_file", lambda file_id: record if file_id == record["id"] else None)
    monkeypatch.setattr(routes, "get_copies", lambda *args, **kwargs: {"total": 1, "copies": []})
    monkeypatch.setattr(routes, "load_text_preview", lambda rec: "hello <vault>")
    monkeypatch.setattr(routes.restore_set, "contains", lambda *args, **kwargs: False)
    return routes


def test_file_page_embeds_image_text_pdf_and_video(monkeypatch):
    from fastapi.testclient import TestClient

    from web.app import app

    cases = [
        (rec(), '<img src="/preview/7"', True),
        (rec(name="notes.txt", file_path="notes.txt", mime_type="text/plain", file_type="Document"), "hello &lt;vault&gt;", False),
        (rec(name="doc.pdf", file_path="doc.pdf", mime_type="application/pdf", file_type="Document"), 'class="file-preview-frame"', True),
        (
            rec(name="clip.mp4", file_path="Videos/clip.mp4", mime_type="video/mp4", file_type="Video", size=4 * 1024 ** 3),
            '<video controls playsinline preload="metadata" src="/preview/7"',
            True,
        ),
        (
            rec(name="song.mp3", file_path="song.mp3", mime_type="audio/mpeg", file_type="Audio"),
            '<audio controls preload="metadata" src="/preview/7"',
            True,
        ),
    ]
    client = TestClient(app)
    for record, marker, uses_preview_url in cases:
        _patch_catalog(monkeypatch, record)
        page = client.get("/file/7")
        assert page.status_code == 200, record["name"]
        assert marker in page.text, record["name"]
        if uses_preview_url:
            assert f"/preview/{record['id']}" in page.text
        else:
            assert "<script>alert" not in page.text
            assert 'class="file-preview-text"' in page.text


def test_file_page_does_not_embed_avi_or_zip(monkeypatch):
    from fastapi.testclient import TestClient

    from web.app import app

    client = TestClient(app)
    _patch_catalog(monkeypatch, rec(name="old.avi", file_path="old.avi", mime_type="video/x-msvideo", file_type="Video"))
    page = client.get("/file/7")
    assert page.status_code == 200
    assert "<video" not in page.text
    assert "does not play in the browser" in page.text

    _patch_catalog(monkeypatch, rec(name="bundle.zip", file_path="bundle.zip", mime_type="application/zip", file_type="Archive"))
    page = client.get("/file/7")
    assert "<video" not in page.text
    assert 'src="/preview/7"' not in page.text
    assert "file-preview" not in page.text


def _patch_stream(monkeypatch, record, blobs):
    from web.routes import files as routes

    calls = []

    def open_remote(b2, path, byte_range=None):
        calls.append((path, byte_range))
        data = blobs[path]
        if byte_range is not None:
            start, end = byte_range
            data = data[start:end + 1]
        return FakeDownloaded(data)

    monkeypatch.setattr(routes, "get_file", lambda file_id: record if file_id == record["id"] else None)
    monkeypatch.setattr(routes, "get_bucket_name", lambda: "bucket")
    monkeypatch.setattr(routes, "shared_manager", lambda bucket=None: object())
    monkeypatch.setattr(routes, "resolve_source", lambda b2, r: r["upload_path"])
    monkeypatch.setattr(routes, "open_remote", open_remote)
    return calls


def test_preview_streams_video_with_range_and_inline_type(monkeypatch):
    from fastapi.testclient import TestClient

    from web.app import app

    payload = b"abcdefghijklmnopqrstuvwxyz"
    record = rec(
        name="clip.mp4",
        file_path="Videos/clip.mp4",
        mime_type="video/mp4",
        file_type="Video",
        size=len(payload),
        upload_path="DriveA/Videos/clip.mp4",
    )
    calls = _patch_stream(monkeypatch, record, {"DriveA/Videos/clip.mp4": payload})
    response = TestClient(app).get("/preview/7", headers={"Range": "bytes=0-3"})

    assert response.status_code == 206
    assert response.content == b"abcd"
    assert response.headers["content-type"].startswith("video/mp4")
    assert response.headers["content-range"] == f"bytes 0-3/{len(payload)}"
    assert response.headers["accept-ranges"] == "bytes"
    assert response.headers["content-disposition"].startswith("inline;")
    assert calls == [("DriveA/Videos/clip.mp4", (0, 3))]


def test_preview_range_past_the_end_is_416(monkeypatch):
    from fastapi.testclient import TestClient

    from web.app import app

    record = rec(name="clip.mp4", file_path="clip.mp4", mime_type="video/mp4", size=10)
    _patch_stream(monkeypatch, record, {})
    response = TestClient(app).get("/preview/7", headers={"Range": "bytes=40-50"})
    assert response.status_code == 416
    assert response.headers["content-range"] == "bytes */10"


def test_preview_text_is_plain_and_html_is_not_executed(monkeypatch):
    from fastapi.testclient import TestClient

    from web.app import app

    body = b"<html><script>alert(1)</script>hello</html>"
    record = rec(
        name="page.html",
        file_path="page.html",
        mime_type="text/html",
        file_type="Code",
        size=len(body),
        upload_path="DriveA/page.html",
    )
    _patch_stream(monkeypatch, record, {"DriveA/page.html": body})
    response = TestClient(app).get("/preview/7")

    assert response.status_code == 200
    assert response.text == body.decode()
    assert response.headers["content-type"].startswith("text/plain")
    assert "text/html" not in response.headers["content-type"]


def test_preview_svg_blocks_scripts(monkeypatch):
    from fastapi.testclient import TestClient

    from web.app import app

    body = b"<svg><script>alert(1)</script></svg>"
    record = rec(
        name="icon.svg",
        file_path="icon.svg",
        mime_type="image/svg+xml",
        size=len(body),
        upload_path="DriveA/icon.svg",
    )
    _patch_stream(monkeypatch, record, {"DriveA/icon.svg": body})
    response = TestClient(app).get("/preview/7")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("image/svg+xml")
    assert "script-src" not in response.headers.get("content-security-policy", "")
    assert "sandbox" in response.headers["content-security-policy"]


def test_preview_refuses_avi_without_calling_b2(monkeypatch):
    from fastapi.testclient import TestClient

    from web.app import app

    record = rec(name="old.avi", file_path="old.avi", mime_type="video/x-msvideo", size=100)
    calls = _patch_stream(monkeypatch, record, {})
    response = TestClient(app).get("/preview/7")
    assert response.status_code == 404
    assert calls == []


def test_preview_missing_row_is_404(monkeypatch):
    from fastapi.testclient import TestClient

    from web.app import app
    from web.routes import files as routes

    monkeypatch.setattr(routes, "get_file", lambda file_id: None)
    response = TestClient(app).get("/preview/999")
    assert response.status_code == 404
