"""Scratch staging and browser upload endpoints (no B2)."""
import io
import os
import time

import pytest
from fastapi.testclient import TestClient

from core import scratch
from core.config import (
    get_scratch_drive_name,
    get_scratch_retention_days,
    set_scratch_drive_name,
    set_scratch_retention_days,
)
from web.app import app


def _isolate(tmp_path, monkeypatch):
    monkeypatch.setattr("core.scratch.SCRATCH_DIR", tmp_path / "scratch")
    monkeypatch.setattr("core.config.GUI_CONFIG_PATH", tmp_path / "config.json")


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("photos/2024/a.jpg", "photos/2024/a.jpg"),
        ("photos\\2024\\a.jpg", "photos/2024/a.jpg"),
        ("photos/./a.jpg", "photos/a.jpg"),
        ("C:\\Users\\me\\a.jpg", "Users/me/a.jpg"),
        ("/leading/slash.txt", None),
        ("../escape.txt", None),
        ("photos/../../escape.txt", None),
        ("~/.ssh/id_rsa", None),
        ("", None),
    ],
)
def test_sanitize_rel_path(raw, expected):
    if expected is None:
        with pytest.raises(scratch.ScratchError):
            scratch.sanitize_rel_path(raw)
    else:
        assert scratch.sanitize_rel_path(raw) == expected


def test_join_prefix_rejects_escapes():
    assert scratch.join_prefix("receipts/2026", "a.pdf") == "receipts/2026/a.pdf"
    assert scratch.join_prefix("", "a.pdf") == "a.pdf"
    with pytest.raises(scratch.ScratchError):
        scratch.join_prefix("../bad", "a.pdf")


def test_save_upload_stages_and_skips_junk(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    batch = scratch.new_batch_id()

    saved = scratch.save_upload(io.BytesIO(b"hello"), batch, "photos/a.jpg", "1700000000000")
    assert saved["skipped"] is False
    assert saved["bytes"] == 5
    staged = tmp_path / "scratch" / batch / "photos" / "a.jpg"
    assert staged.read_bytes() == b"hello"
    assert abs(staged.stat().st_mtime - 1700000000) < 2

    skipped = scratch.save_upload(
        io.BytesIO(b"junk"), batch, "site/node_modules/x.js"
    )
    assert skipped["skipped"] is True
    assert not (tmp_path / "scratch" / batch / "site").exists()


def test_upload_endpoint_stages_file(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    batch = scratch.new_batch_id()
    client = TestClient(app)
    response = client.post(
        "/scratch/upload",
        data={"batch": batch, "path": "photos/a.jpg", "prefix": "drop"},
        files={"file": ("a.jpg", b"hello", "image/jpeg")},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["path"] == "drop/photos/a.jpg"
    assert (tmp_path / "scratch" / batch / "drop" / "photos" / "a.jpg").read_bytes() == b"hello"


def test_upload_endpoint_rejects_traversal(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    batch = scratch.new_batch_id()
    client = TestClient(app)
    response = client.post(
        "/scratch/upload",
        data={"batch": batch, "path": "../../etc/passwd"},
        files={"file": ("passwd", b"nope", "text/plain")},
    )
    assert response.status_code == 400
    assert response.json()["ok"] is False


def test_upload_endpoint_handles_multi_megabyte_file(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    batch = scratch.new_batch_id()
    data = b"x" * (2 * 1024 * 1024 + 123)
    client = TestClient(app)
    response = client.post(
        "/scratch/upload",
        data={"batch": batch, "path": "big.bin"},
        files={"file": ("big.bin", data, "application/octet-stream")},
    )
    assert response.status_code == 200
    assert response.json()["bytes"] == len(data)
    staged = tmp_path / "scratch" / batch / "big.bin"
    assert staged.stat().st_size == len(data)


def test_finish_enqueues_ingest_with_cleanup(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    batch = scratch.new_batch_id()
    scratch.save_upload(io.BytesIO(b"hello"), batch, "a.txt")

    captured = {}

    def fake_enqueue(job_type, payload):
        captured["type"] = job_type
        captured["payload"] = payload
        return 42

    monkeypatch.setattr("web.routes.scratch.enqueue", fake_enqueue)
    monkeypatch.setattr("web.routes.scratch.get_bucket_name", lambda: "my-bucket")

    client = TestClient(app)
    response = client.post(
        "/scratch/finish",
        data={"batch": batch, "drive_name": "Scratch", "bucket": "my-bucket"},
        headers={"Accept": "application/json"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["ok"] is True
    assert data["job_id"] == 42
    payload = captured["payload"]
    assert captured["type"] == "ingest"
    assert payload["cleanup_source"] is True
    assert payload["drive_name"] == "Scratch"
    assert payload["scan_only"] is False
    assert payload["source"].endswith(batch)
    assert payload["drive_root"].endswith(batch)


def test_finish_requires_bucket(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    batch = scratch.new_batch_id()
    scratch.save_upload(io.BytesIO(b"hello"), batch, "a.txt")
    monkeypatch.setattr("web.routes.scratch.get_bucket_name", lambda: "")

    client = TestClient(app)
    response = client.post(
        "/scratch/finish",
        data={"batch": batch, "drive_name": "Scratch", "bucket": ""},
        headers={"Accept": "application/json"},
    )
    assert response.status_code == 400
    assert "bucket" in response.json()["error"].lower()


def test_cleanup_after_ingest_is_limited_to_scratch(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    root = tmp_path / "scratch"
    batch = root / scratch.new_batch_id()
    batch.mkdir(parents=True)
    (batch / "a.txt").write_bytes(b"hello")

    assert scratch.cleanup_after_ingest(batch) is True
    assert not batch.exists()

    disk = tmp_path / "OldSeagate"
    disk.mkdir()
    (disk / "a.txt").write_bytes(b"hello")
    assert scratch.cleanup_after_ingest(disk) is False
    assert (disk / "a.txt").exists()


def test_stale_batches_expire(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    root = tmp_path / "scratch"
    old = root / "20200101-000000-deadbeef"
    fresh = root / scratch.new_batch_id()
    old.mkdir(parents=True)
    fresh.mkdir(parents=True)
    (old / "a.txt").write_bytes(b"hello")
    (fresh / "b.txt").write_bytes(b"hello")
    ten_days_ago = time.time() - 10 * 86400
    os.utime(old, (ten_days_ago, ten_days_ago))

    assert scratch.cleanup_stale_batches(days=7) == 1
    assert not old.exists()
    assert fresh.exists()


def test_active_batch_is_not_cleaned(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    monkeypatch.setattr("core.db.DB_PATH", tmp_path / "b2_dedup.db")
    from core.db import init_db, thread_local
    from core.jobs import enqueue

    if hasattr(thread_local, "connection"):
        thread_local.connection.close()
        delattr(thread_local, "connection")
    init_db()

    batch = scratch.new_batch_id()
    scratch.save_upload(io.BytesIO(b"hello"), batch, "a.txt")
    batch_path = tmp_path / "scratch" / batch
    enqueue(
        "ingest",
        {
            "source": str(batch_path),
            "drive_name": "Scratch",
            "drive_root": str(batch_path),
        },
    )

    assert scratch.batch_is_active(batch) is True
    assert scratch.cleanup_batch(batch) is False
    assert batch_path.exists()
    assert scratch.clear_batches() == 0
    assert batch_path.exists()


def test_ingest_job_cleans_staged_batch(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    monkeypatch.setattr("core.db.DB_PATH", tmp_path / "b2_dedup.db")
    from core.db import connect, init_db, thread_local

    if hasattr(thread_local, "connection"):
        thread_local.connection.close()
        delattr(thread_local, "connection")
    init_db()

    batch = scratch.new_batch_id()
    scratch.save_upload(io.BytesIO(b"hello"), batch, "photos/a.jpg")
    batch_path = tmp_path / "scratch" / batch

    from core.ingest import run_ingest_job

    run_ingest_job(
        {
            "id": 1,
            "payload": {
                "source": str(batch_path),
                "drive_name": "Scratch",
                "drive_root": str(batch_path),
                "scan_only": True,
                "cleanup_source": True,
            },
        }
    )

    assert not batch_path.exists()
    conn = connect(readonly=True)
    try:
        row = conn.execute(
            "SELECT drive_name, file_path, is_original FROM files WHERE drive_name = 'Scratch'"
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row["file_path"] == "photos/a.jpg"
    assert row["is_original"] == 1


def test_scratch_page_renders(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    client = TestClient(app)
    response = client.get("/scratch")
    assert response.status_code == 200
    assert "Drop files or folders here" in response.text
    assert "scratch.js" in response.text


def test_scratch_page_lists_staged_batches(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    batch = scratch.new_batch_id()
    scratch.save_upload(io.BytesIO(b"hello"), batch, "a.txt")

    client = TestClient(app)
    response = client.get("/scratch")
    assert response.status_code == 200
    assert batch in response.text
    assert "Ingest now" in response.text
    assert "a.txt" not in response.text  # the listing stays metadata-only


def test_scratch_page_marks_active_batch(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    batch = scratch.new_batch_id()
    monkeypatch.setattr(
        "web.routes.scratch.list_batches",
        lambda: [
            {
                "id": batch,
                "path": str(tmp_path / "scratch" / batch),
                "files": 1,
                "bytes": 5,
                "bytes_fmt": "5 B",
                "when": "today",
                "mtime": 0,
                "active": True,
            }
        ],
    )
    client = TestClient(app)
    response = client.get("/scratch")
    assert response.status_code == 200
    assert "ingest running" in response.text
    assert f"/scratch/finish" not in response.text


def test_settings_exposes_scratch_controls(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    client = TestClient(app)
    response = client.get("/settings")
    assert response.status_code == 200
    assert "Scratch drive" in response.text
    assert "Clear staged uploads" in response.text


def test_settings_saves_scratch_name_and_retention(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    client = TestClient(app)
    response = client.post(
        "/settings/scratch",
        data={"drive_name": "PhoneDrop", "retention_days": "3"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert get_scratch_drive_name() == "PhoneDrop"
    assert get_scratch_retention_days() == 3


def test_settings_clears_staged_batches(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    batch = scratch.new_batch_id()
    scratch.save_upload(io.BytesIO(b"hello"), batch, "a.txt")
    batch_path = tmp_path / "scratch" / batch

    client = TestClient(app)
    response = client.post("/settings/scratch/clear", follow_redirects=False)
    assert response.status_code == 303
    assert not batch_path.exists()


def test_scratch_config_round_trip(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    assert get_scratch_drive_name() == "Scratch"
    assert set_scratch_drive_name("Dropbox") == "Dropbox"
    assert get_scratch_drive_name() == "Dropbox"
    assert set_scratch_drive_name("bad/name") == "Dropbox"
    assert set_scratch_retention_days(3) == 3
    assert get_scratch_retention_days() == 3
    assert set_scratch_retention_days("nope") == 7
