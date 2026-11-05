"""Download helpers: naming, pointer resolution, limits, and ZIP building."""
import io
import json
import types
import zipfile
from pathlib import Path

import pytest

from core import download as dl
from core.config import (
    DEFAULT_DOWNLOAD_MAX_BYTES,
    DEFAULT_DOWNLOAD_MAX_FILES,
    DEFAULT_DOWNLOAD_STREAM_THRESHOLD,
)


class FakeDownloaded:
    """Stands in for b2sdk's DownloadedFile."""

    def __init__(self, data: bytes):
        self.data = data
        self.download_version = types.SimpleNamespace(content_length=len(data))
        self.response = None
        self.saved_to = None

    def save(self, file, allow_seeking=None):
        file.write(self.data)

    def save_to(self, path):
        self.saved_to = Path(path)
        Path(path).write_bytes(self.data)


class FakeBucket:
    def __init__(self, blobs: dict[str, bytes]):
        self.blobs = blobs
        self.opened: list[str] = []

    def download_file_by_name(self, name):
        if name not in self.blobs:
            raise FileNotFoundError(name)
        self.opened.append(name)
        return FakeDownloaded(self.blobs[name])


class FakeB2:
    def __init__(self, blobs: dict[str, bytes]):
        self.blobs = blobs
        self.bucket = FakeBucket(blobs)

    def download_file_content(self, name):
        if name not in self.blobs:
            raise FileNotFoundError(name)
        return self.blobs[name]


def record(**over):
    base = {
        "id": 1,
        "hash": "a" * 64,
        "size": 4,
        "drive_name": "DriveA",
        "file_path": "Photos/cat.jpg",
        "upload_path": "DriveA/Photos/cat.jpg",
        "is_original": True,
    }
    base.update(over)
    return base


# ---------------------------------------------------------------- naming


def test_safe_filename_strips_paths_and_controls():
    assert dl.safe_filename("C:\\Users\\me\\report.pdf") == "report.pdf"
    assert dl.safe_filename("../../etc/passwd") == "passwd"
    assert dl.safe_filename("bad\x00name\x1f.txt") == "badname.txt"
    assert dl.safe_filename("") == "download"
    assert dl.safe_filename("...") == "download"


def test_safe_filename_keeps_long_extensions():
    name = dl.safe_filename("x" * 400 + ".tar.gz")
    assert name.endswith(".gz")
    assert len(name) <= 150


def test_content_disposition_handles_unicode():
    header = dl.content_disposition("café.jpg")
    assert header.startswith("attachment;")
    assert 'filename="caf_.jpg"' in header
    assert "filename*=UTF-8''caf%C3%A9.jpg" in header


def test_content_disposition_quotes_are_removed():
    header = dl.content_disposition('we"ird name.txt')
    assert '"we_ird name.txt"' in header


def test_content_disposition_treats_backslash_as_separator():
    header = dl.content_disposition("folder\\name.txt")
    assert 'filename="name.txt"' in header


def test_zip_arcname_drops_traversal_and_leading_slashes():
    assert dl.zip_arcname("Drive", "../../etc/passwd") == "Drive/etc/passwd"
    assert dl.zip_arcname("Drive", "/abs/path.txt") == "Drive/abs/path.txt"
    assert dl.zip_arcname("", "a//b/./c.txt") == "a/b/c.txt"
    assert dl.zip_arcname("Drive", "bad\x00name.txt") == "Drive/badname.txt"


def test_unique_arcname_keeps_collisions():
    used = set()
    assert dl._unique_arcname("Drive/a.txt", used) == "Drive/a.txt"
    assert dl._unique_arcname("Drive/a.txt", used) == "Drive/a (2).txt"
    assert dl._unique_arcname("Drive/a.txt", used) == "Drive/a (3).txt"


# ------------------------------------------------------- pointer resolution


def test_resolve_source_prefers_upload_path():
    b2 = FakeB2({"DriveA/Photos/cat.jpg": b"x"})
    rec = record(upload_path="elsewhere/cat.jpg")
    assert dl.resolve_source(b2, rec) == "elsewhere/cat.jpg"


def test_resolve_source_falls_back_to_remote_for_originals():
    b2 = FakeB2({})
    rec = record(upload_path=None, is_original=True)
    assert dl.resolve_source(b2, rec) == "DriveA/Photos/cat.jpg"


def test_resolve_source_reads_pointer_for_copies():
    pointer = json.dumps({"original_path": "DriveB/real.jpg"}).encode()
    b2 = FakeB2({"DriveA/Photos/cat.jpg.b2ptr": pointer})
    rec = record(upload_path=None, is_original=False, hash="b" * 64)
    assert dl.resolve_source(b2, rec) == "DriveB/real.jpg"


# ---------------------------------------------------------------- limits


def test_selection_error_uses_defaults(monkeypatch):
    monkeypatch.setattr(dl, "get_download_max_files", lambda: 10)
    monkeypatch.setattr(dl, "get_download_max_bytes", lambda: 1000)
    assert dl.selection_error(10, 1000) is None
    assert "over the 10-file limit" in dl.selection_error(11, 10)
    assert "over the 1000.0 B limit" in dl.selection_error(1, 1001)


def test_default_limits_are_the_planned_ones():
    assert DEFAULT_DOWNLOAD_MAX_FILES == 1000
    assert DEFAULT_DOWNLOAD_MAX_BYTES == 5 * 1024 ** 3
    assert DEFAULT_DOWNLOAD_STREAM_THRESHOLD == 500 * 1024 ** 2


def test_forced_to_stream_uses_threshold(monkeypatch):
    monkeypatch.setattr(dl, "get_download_stream_threshold", lambda: 100)
    assert dl.forced_to_stream(100) is False
    assert dl.forced_to_stream(101) is True


# ------------------------------------------------------------- zip builds


@pytest.fixture()
def exports_dir(tmp_path, monkeypatch):
    target = tmp_path / "exports"
    monkeypatch.setattr(dl, "EXPORTS_DIR", target)
    return target


def test_build_zip_dedupes_content_and_names_by_drive(exports_dir):
    b2 = FakeB2(
        {
            "DriveA/Photos/cat.jpg": b"meow",
            "DriveB/Photos/cat.jpg": b"meow",
        }
    )
    records = [
        record(id=1, drive_name="DriveA"),
        record(id=2, drive_name="DriveB", is_original=False, upload_path="DriveA/Photos/cat.jpg"),
    ]
    dest = exports_dir / "1.zip"

    problems = dl.build_zip(records, b2, dest)

    assert problems == []
    # Both rows share a hash, so the bytes were fetched once.
    assert b2.bucket.opened == ["DriveA/Photos/cat.jpg"]
    with zipfile.ZipFile(dest) as archive:
        assert archive.namelist() == [
            "DriveA/Photos/cat.jpg",
            "DriveB/Photos/cat.jpg",
        ]
        assert archive.read("DriveB/Photos/cat.jpg") == b"meow"


def test_build_zip_records_missing_files_and_keeps_going(exports_dir):
    b2 = FakeB2({"DriveA/Photos/cat.jpg": b"meow"})
    records = [
        record(id=1),
        record(id=2, hash="c" * 64, file_path="Photos/gone.jpg", upload_path=None, is_original=False),
    ]
    dest = exports_dir / "2.zip"

    problems = dl.build_zip(records, b2, dest)

    assert len(problems) == 1
    assert "gone.jpg" in problems[0]
    with zipfile.ZipFile(dest) as archive:
        names = archive.namelist()
        assert "DriveA/Photos/cat.jpg" in names
        assert "_download-errors.txt" in names
        assert "gone.jpg" in archive.read("_download-errors.txt").decode()


def test_stream_zip_yields_readable_archive(exports_dir):
    b2 = FakeB2(
        {
            "DriveA/Photos/cat.jpg": b"meow" * 1000,
            "DriveA/Notes/readme.txt": b"hello",
        }
    )
    records = [
        record(id=1),
        record(id=2, hash="d" * 64, file_path="Notes/readme.txt", upload_path="DriveA/Notes/readme.txt"),
    ]

    data = b"".join(dl.stream_zip(records, b2))

    assert zipfile.is_zipfile(io.BytesIO(data))
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        assert archive.read("DriveA/Photos/cat.jpg") == b"meow" * 1000
        assert archive.read("DriveA/Notes/readme.txt") == b"hello"


def test_stream_zip_notes_missing_files(exports_dir):
    b2 = FakeB2({})
    records = [record(id=1)]

    data = b"".join(dl.stream_zip(records, b2))

    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        names = archive.namelist()
        assert "_download-errors.txt" in names
        assert "cat.jpg" in archive.read("_download-errors.txt").decode()


def test_stream_downloaded_file_round_trips():
    downloaded = FakeDownloaded(b"0123456789" * 1000)
    chunks = list(dl.stream_downloaded_file(downloaded, chunk_size=64))
    assert b"".join(chunks) == b"0123456789" * 1000
    assert len(chunks) > 1


# ---------------------------------------------------------------- cleanup


def test_cleanup_exports_keeps_newest_and_drops_expired(exports_dir, monkeypatch):
    exports_dir.mkdir(parents=True, exist_ok=True)
    old = exports_dir / "old.zip"
    old.write_bytes(b"x")
    for index in range(7):
        path = exports_dir / f"new{index}.zip"
        path.write_bytes(b"y")
    monkeypatch.setattr(dl, "get_download_expiry_hours", lambda: 24)
    monkeypatch.setattr(dl, "EXPORT_KEEP", 3)

    dl.cleanup_exports()

    remaining = sorted(p.name for p in exports_dir.glob("*.zip"))
    assert len(remaining) == 3
    assert "old.zip" not in remaining


def test_export_status_reports_missing_file(exports_dir):
    job = {"id": 12, "type": "zip_export", "status": "done", "created_at": "2026-11-04T10:00:00+00:00"}
    info = dl.export_status(job)
    assert info["exists"] is False
    assert info["url"] == "/exports/12.zip"
    assert info["name"] == "storageinsights-20261104-12.zip"

    exports_dir.mkdir(parents=True, exist_ok=True)
    dl.export_path(12).write_bytes(b"zip")
    assert dl.export_status(job)["exists"] is True


# ---------------------------------------------------------------- routes


def _fake_record(**over):
    base = {
        "id": 1,
        "name": "cat.jpg",
        "file_path": "Photos/cat.jpg",
        "drive_name": "DriveA",
        "mime_type": "image/jpeg",
        "size": 4,
        "is_original": True,
        "upload_path": "DriveA/Photos/cat.jpg",
    }
    base.update(over)
    return base


def test_single_file_download_streams_with_headers(monkeypatch):
    from fastapi.testclient import TestClient

    from web.app import app
    from web.routes import downloads as routes

    rec = _fake_record()
    monkeypatch.setattr(routes, "get_file", lambda file_id: rec if file_id == 1 else None)
    monkeypatch.setattr(routes, "get_bucket_name", lambda: "bucket")
    monkeypatch.setattr(routes, "shared_manager", lambda bucket=None: object())
    monkeypatch.setattr(routes, "resolve_source", lambda b2, r: "DriveA/Photos/cat.jpg")
    monkeypatch.setattr(routes, "open_remote", lambda b2, path: FakeDownloaded(b"meow"))

    response = TestClient(app).get("/download/1")

    assert response.status_code == 200
    assert response.content == b"meow"
    assert response.headers["content-disposition"].startswith("attachment;")
    assert 'filename="cat.jpg"' in response.headers["content-disposition"]
    assert response.headers["content-length"] == "4"
    assert response.headers["x-content-type-options"] == "nosniff"


def test_single_file_download_missing_row_is_404(monkeypatch):
    from fastapi.testclient import TestClient

    from web.app import app
    from web.routes import downloads as routes

    monkeypatch.setattr(routes, "get_file", lambda file_id: None)

    response = TestClient(app).get("/download/999999")

    assert response.status_code == 404
    assert "not in the catalog" in response.text


def test_single_file_download_b2_error_is_404(monkeypatch):
    from fastapi.testclient import TestClient

    from web.app import app
    from web.routes import downloads as routes

    monkeypatch.setattr(routes, "get_file", lambda file_id: _fake_record())
    monkeypatch.setattr(routes, "get_bucket_name", lambda: "bucket")
    monkeypatch.setattr(routes, "shared_manager", lambda bucket=None: object())

    def missing(b2, rec):
        raise FileNotFoundError("no such object")

    monkeypatch.setattr(routes, "resolve_source", missing)

    response = TestClient(app).get("/download/1")

    assert response.status_code == 404
    assert "does not have the bytes" in response.text


def test_empty_restore_set_redirects(monkeypatch):
    from fastapi.testclient import TestClient

    from web.app import app
    from web.routes import downloads as routes

    monkeypatch.setattr(routes, "_selection", lambda request: ([], 0))

    response = TestClient(app).get("/restore-set/download.zip", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/restore-set"


def test_jobs_dispatch_zip_export(monkeypatch):
    from core import download, jobs

    called: list[int] = []
    monkeypatch.setattr(download, "run_zip_export_job", lambda job: called.append(job["id"]))

    jobs._run_job({"id": 5, "type": "zip_export", "payload": {}})

    assert called == [5]
