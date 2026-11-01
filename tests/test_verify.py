"""Catalog vs B2 verify: sample, full, and missing originals."""
from types import SimpleNamespace

from core.db import connect
from core.ingest import insert_file_record
from core.verify import run_verify


class FakeB2:
    def __init__(self, names):
        self.names = set(names)

    def file_exists(self, remote_path: str) -> bool:
        return remote_path in self.names

    def list_files(self, prefix: str = ""):
        for name in sorted(self.names):
            if name.startswith(prefix):
                yield SimpleNamespace(file_name=name), None


def _isolate(tmp_path, monkeypatch):
    db_path = tmp_path / "b2_dedup.db"
    cfg_path = tmp_path / "config.json"
    monkeypatch.setattr("core.db.DB_PATH", db_path)
    monkeypatch.setattr("core.paths.DB_PATH", db_path)
    monkeypatch.setattr("core.config.GUI_CONFIG_PATH", cfg_path)
    monkeypatch.setattr("core.paths.GUI_CONFIG_PATH", cfg_path)
    from core.db import thread_local

    if hasattr(thread_local, "connection"):
        try:
            thread_local.connection.close()
        except Exception:
            pass
        delattr(thread_local, "connection")
    from core.db import init_db

    init_db()
    return db_path


def _meta():
    return {
        "mtime": None,
        "ctime": None,
        "atime": None,
        "mime_type": "image/jpeg",
        "file_type": "Image",
    }


def _add_original(drive, path, remote, size=10):
    conn = connect()
    try:
        insert_file_record(conn.cursor(), "h" + path, size, drive, path, remote, 1, _meta())
        conn.commit()
    finally:
        conn.close()


def test_sample_reports_missing(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    _add_original("Drive", "a.jpg", "Drive/a.jpg")
    _add_original("Drive", "b.jpg", "Drive/b.jpg")
    result = run_verify(mode="sample", sample_size=10, b2=FakeB2({"Drive/a.jpg"}), bucket="test")
    assert result["checked"] == 2
    assert result["found"] == 1
    assert result["missing"] == 1
    assert "Drive/b.jpg" in result["missing_paths"]


def test_full_diffs_bucket_listing(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    _add_original("Drive", "keep.jpg", "Drive/keep.jpg")
    _add_original("Drive", "gone.jpg", "Drive/gone.jpg")
    result = run_verify(
        mode="full",
        b2=FakeB2({"Drive/keep.jpg", "Drive/keep.jpg.b2ptr", "__b2_dedup_metadata__/b2_dedup.db"}),
        bucket="test",
    )
    assert result["mode"] == "full"
    assert result["checked"] == 2
    assert result["found"] == 1
    assert result["missing"] == 1
    assert result["missing_paths"] == ["Drive/gone.jpg"]


def test_scan_only_originals_are_not_missing(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    _add_original("Drive", "uploaded.jpg", "Drive/uploaded.jpg")
    conn = connect()
    try:
        insert_file_record(conn.cursor(), "scan", 4, "Drive", "scanned.jpg", None, 1, _meta())
        conn.commit()
    finally:
        conn.close()
    result = run_verify(mode="full", b2=FakeB2({"Drive/uploaded.jpg"}), bucket="test")
    assert result["checked"] == 1
    assert result["found"] == 1
    assert result["missing"] == 0
    assert result["no_remote"] == 1


def test_job_dispatch_knows_verify(monkeypatch):
    called = {}

    def fake(job):
        called["id"] = job["id"]

    monkeypatch.setattr("core.verify.run_verify_job", fake)
    from core.jobs import _run_job

    _run_job({"id": 9, "type": "verify", "payload": {"mode": "sample"}})
    assert called["id"] == 9


def test_last_verify_is_saved(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    _add_original("Drive", "a.jpg", "Drive/a.jpg")
    run_verify(mode="sample", sample_size=5, b2=FakeB2({"Drive/a.jpg"}), bucket="test")
    from core.config import get_last_verify
    from core.health import get_vault_health

    last = get_last_verify()
    assert last["missing"] == 0
    assert last["found"] == 1
    health = get_vault_health()
    assert health["verify"]["state"] == "ok"
