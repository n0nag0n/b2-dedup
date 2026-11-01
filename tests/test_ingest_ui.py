"""Ingest volume picker pages (no Docker / host-drives mount)."""
from fastapi.testclient import TestClient

from web.app import app


def test_ingest_home_is_a_volume_picker():
    client = TestClient(app)
    response = client.get("/ingest")
    assert response.status_code == 200
    body = response.text
    assert "Plugged-in disks" in body
    assert "Use this disk" in body or "No removable disks found" in body
    assert "HOST_DRIVES" not in body
    assert "Docker" not in body
    assert "/host_drives" not in body


def test_settings_explains_disk_locations():
    client = TestClient(app)
    response = client.get("/settings")
    assert response.status_code == 200
    assert "Where disks show up" in response.text
    assert "Linux" in response.text or "macOS" in response.text or "Windows" in response.text


def test_ingest_rejects_path_outside_allowed_roots():
    client = TestClient(app)
    response = client.get("/ingest", params={"path": "/etc"}, follow_redirects=False)
    assert response.status_code == 303
    assert response.headers["location"] == "/ingest"


def test_add_folder_then_browse(tmp_path, monkeypatch):
    cfg = tmp_path / "cfg.json"
    monkeypatch.setattr("core.config.GUI_CONFIG_PATH", cfg)
    disk = tmp_path / "OldSeagate"
    (disk / "Photos").mkdir(parents=True)

    client = TestClient(app)
    added = client.post(
        "/ingest/add-folder",
        data={"folder": str(disk)},
        follow_redirects=False,
    )
    assert added.status_code == 303
    location = added.headers["location"]
    assert "OldSeagate" in location

    page = client.get(location)
    assert page.status_code == 200
    assert "Ingest this folder" in page.text
    assert "OldSeagate" in page.text
    assert "Photos" in page.text


def test_preview_page_renders(tmp_path, monkeypatch):
    cfg = tmp_path / "cfg.json"
    monkeypatch.setattr("core.config.GUI_CONFIG_PATH", cfg)
    disk = tmp_path / "CameraCard"
    (disk / "DCIM").mkdir(parents=True)
    (disk / "DCIM" / "IMG.JPG").write_bytes(b"photo")

    client = TestClient(app)
    client.post("/ingest/add-folder", data={"folder": str(disk)})
    response = client.get(
        "/ingest/preview",
        params={
            "path": str(disk),
            "drive_name": "CameraCard",
            "drive_root": str(disk),
            "mode": "scan-only",
        },
    )
    assert response.status_code == 200
    assert "Preview" in response.text
    assert "Start scan-only job" in response.text


def test_add_folder_rejects_system_root(tmp_path, monkeypatch):
    monkeypatch.setattr("core.config.GUI_CONFIG_PATH", tmp_path / "cfg.json")
    client = TestClient(app)
    response = client.post(
        "/ingest/add-folder",
        data={"folder": "/"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"] == "/ingest"
