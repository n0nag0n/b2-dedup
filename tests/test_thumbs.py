"""Thumbnail render and job, without talking to B2."""
from io import BytesIO
from types import SimpleNamespace

from PIL import Image

from core.db import connect
from core.ingest import insert_file_record
from core.thumbs import (
    generate_thumb_from_bytes,
    run_thumbs,
    thumb_exists,
    thumb_path,
    thumbs_pending,
)


def _isolate(tmp_path, monkeypatch):
    db_path = tmp_path / "b2_dedup.db"
    thumbs = tmp_path / "thumbs"
    monkeypatch.setattr("core.db.DB_PATH", db_path)
    monkeypatch.setattr("core.paths.DB_PATH", db_path)
    monkeypatch.setattr("core.paths.THUMBS_DIR", thumbs)
    monkeypatch.setattr("core.thumbs.THUMBS_DIR", thumbs)
    from core.db import thread_local

    if hasattr(thread_local, "connection"):
        try:
            thread_local.connection.close()
        except Exception:
            pass
        delattr(thread_local, "connection")
    from core.db import init_db

    init_db()
    return thumbs


def _jpeg(color="green") -> bytes:
    buf = BytesIO()
    Image.new("RGB", (120, 80), color).save(buf, format="JPEG", quality=85)
    return buf.getvalue()


def test_render_writes_thumb_by_hash(tmp_path, monkeypatch):
    thumbs = _isolate(tmp_path, monkeypatch)
    file_hash = "ab" + "cd" * 31
    info = generate_thumb_from_bytes(file_hash, _jpeg())
    assert info["w"] <= 400
    assert info["h"] <= 400
    assert thumb_exists(file_hash)
    assert thumb_path(file_hash).is_relative_to(thumbs)
    assert thumb_path(file_hash).stat().st_size > 0


class FakeB2:
    def __init__(self, blobs: dict[str, bytes]):
        self.blobs = blobs

    def download_file_content(self, remote_path: str) -> bytes:
        return self.blobs[remote_path]

    def download_file_range(self, remote_path: str, start: int, end: int) -> bytes:
        return self.blobs[remote_path][start:end + 1]


def test_thumbs_job_uses_catalog_originals(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    file_hash = "11" * 32
    jpeg = _jpeg("navy")
    conn = connect()
    try:
        insert_file_record(
            conn.cursor(),
            file_hash,
            len(jpeg),
            "Drive",
            "2016/July 2016/pic.jpg",
            "Drive/2016/July 2016/pic.jpg",
            1,
            {
                "mtime": "2016-07-04T12:00:00+00:00",
                "ctime": None,
                "atime": None,
                "mime_type": "image/jpeg",
                "file_type": "Image",
            },
        )
        conn.commit()
    finally:
        conn.close()

    assert thumbs_pending()
    stats = run_thumbs(
        "test",
        limit=10,
        b2=FakeB2({"Drive/2016/July 2016/pic.jpg": jpeg}),
    )
    assert stats["created"] == 1
    assert thumb_exists(file_hash)
    assert thumbs_pending() == []
