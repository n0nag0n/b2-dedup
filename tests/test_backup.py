"""Catalog snapshot and backup-status helpers (no B2)."""
import sqlite3

from core.backup import backup_status, snapshot_catalog
from core.catalog import delete_drive, get_drive_names
from core.config import update_config
from core.db import connect
from core.health import get_vault_health
from core.ingest import insert_file_record


def _isolate(tmp_path, monkeypatch):
    db_path = tmp_path / "b2_dedup.db"
    cfg_path = tmp_path / "config.json"
    monkeypatch.setattr("core.db.DB_PATH", db_path)
    monkeypatch.setattr("core.paths.DB_PATH", db_path)
    monkeypatch.setattr("core.backup.DB_PATH", db_path)
    monkeypatch.setattr("core.backup.DATA_DIR", tmp_path)
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


def _add(drive, path):
    conn = connect()
    try:
        insert_file_record(
            conn.cursor(), "h", 8, drive, path, f"{drive}/{path}", 1,
            {"mtime": None, "ctime": None, "atime": None, "mime_type": None, "file_type": "Other"},
        )
        conn.commit()
    finally:
        conn.close()


def test_backup_status_without_bucket(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    status = backup_status()
    assert status["state"] == "no_bucket"
    health = get_vault_health()
    assert health["backup"]["state"] == "no_bucket"
    assert health["verify"]["state"] == "never"


def test_backup_status_ok_and_stale(tmp_path, monkeypatch):
    db_path = _isolate(tmp_path, monkeypatch)
    update_config(bucket_name="test-bucket")
    assert backup_status()["state"] == "never"

    st = db_path.stat()
    update_config(db_backup_local_mtime=st.st_mtime, db_backup_local_size=st.st_size)
    assert backup_status()["state"] == "ok"

    update_config(db_backup_local_size=st.st_size + 99)
    assert backup_status()["state"] == "stale"


def test_snapshot_is_a_usable_copy(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    _add("DriveA", "photo.jpg")
    dest = tmp_path / "snap.db"
    snapshot_catalog(dest)
    assert dest.exists()
    conn = sqlite3.connect(dest)
    try:
        n = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    finally:
        conn.close()
    assert n == 1


def test_delete_drive_removes_only_that_drive(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    _add("Keep", "a.jpg")
    _add("Drop", "b.jpg")
    assert get_drive_names() == ["Drop", "Keep"]
    removed = delete_drive("Drop")
    assert removed == 1
    assert get_drive_names() == ["Keep"]
