"""Catalog and B2 moves for a folder, a drive's contents, and one file."""
import json

from fastapi.testclient import TestClient

from core.db import connect, init_db
from core.ingest import insert_file_record
from core.pointers import create_pointer_content
from core.relocate import (
    RelocateError,
    execute_plan,
    plan_move_contents,
    plan_move_file,
    plan_move_folder,
    run_relocate_job,
)
from web.app import app


def _isolate(tmp_path, monkeypatch):
    db_path = tmp_path / "b2_dedup.db"
    monkeypatch.setattr("core.db.DB_PATH", db_path)
    monkeypatch.setattr("core.paths.DB_PATH", db_path)
    from core.db import thread_local

    if hasattr(thread_local, "connection"):
        try:
            thread_local.connection.close()
        except Exception:
            pass
        delattr(thread_local, "connection")
    init_db()
    return db_path


def _meta():
    return {
        "mtime": "2020-01-01T00:00:00+00:00",
        "ctime": None,
        "atime": None,
        "mime_type": "image/jpeg",
        "file_type": "Image",
    }


def _add(drive, path, remote, *, original=1, file_hash="h1", size=10):
    conn = connect()
    try:
        insert_file_record(
            conn.cursor(),
            file_hash,
            size,
            drive,
            path,
            remote,
            original,
            _meta(),
        )
        conn.commit()
        row = conn.execute(
            "SELECT id FROM files WHERE drive_name = ? AND file_path = ?",
            (drive, path),
        ).fetchone()
        return int(row["id"])
    finally:
        conn.close()


class FakeB2:
    def __init__(self):
        self.files = {}

    def copy_remote(self, src, dest):
        if src not in self.files:
            raise FileNotFoundError(src)
        if dest in self.files:
            raise FileExistsError(dest)
        self.files[dest] = self.files[src]

    def delete_remote(self, name):
        if name not in self.files:
            raise FileNotFoundError(name)
        del self.files[name]

    def upload_bytes(self, content, remote, content_type="application/json"):
        self.files[remote] = content

    def download_file_content(self, remote):
        if remote not in self.files:
            raise FileNotFoundError(remote)
        return self.files[remote]


def _paths():
    conn = connect(readonly=True)
    try:
        return [
            (row["drive_name"], row["file_path"], row["upload_path"], row["is_original"])
            for row in conn.execute(
                "SELECT drive_name, file_path, upload_path, is_original FROM files ORDER BY drive_name, file_path"
            )
        ]
    finally:
        conn.close()


def test_folder_move_keeps_the_name_and_rewrites_pointers(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    _add("Scratch", "Documents/a.jpg", "Scratch/Documents/a.jpg")
    _add("Scratch", "Videos/clip.mp4", "Scratch/Videos/clip.mp4", file_hash="h2")
    _add("OldDrive", "copies/a.jpg", None, original=0, file_hash="h1")
    b2 = FakeB2()
    b2.files["Scratch/Documents/a.jpg"] = b"photo"
    b2.files["Scratch/Videos/clip.mp4"] = b"video"
    b2.files["OldDrive/copies/a.jpg.b2ptr"] = create_pointer_content("h1", "Scratch/Documents/a.jpg")

    plan = plan_move_folder("Scratch", "Documents", "asus-backup", "Scratch")
    assert plan["count"] == 1
    result = execute_plan(b2, plan)
    assert result["errors"] == []
    assert ("Scratch", "asus-backup/Documents/a.jpg", "Scratch/asus-backup/Documents/a.jpg", 1) in _paths()
    assert ("Scratch", "Videos/clip.mp4", "Scratch/Videos/clip.mp4", 1) in _paths()
    assert "Scratch/Documents/a.jpg" not in b2.files
    assert b2.files["Scratch/asus-backup/Documents/a.jpg"] == b"photo"
    pointer = json.loads(b2.files["OldDrive/copies/a.jpg.b2ptr"].decode())
    assert pointer["original_path"] == "Scratch/asus-backup/Documents/a.jpg"
    assert pointer["original_hash"] == "h1"


def test_contents_of_a_drive_nest_under_the_folder(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    _add("Scratch", "Documents/a.jpg", "Scratch/Documents/a.jpg")
    _add("Scratch", "my_photos/b.jpg", "Scratch/my_photos/b.jpg", file_hash="h2")
    _add("Scratch", "myXphotos/c.jpg", "Scratch/myXphotos/c.jpg", file_hash="h3")
    _add("Scratch", "asus-backup/keep.txt", "Scratch/asus-backup/keep.txt", file_hash="h4")
    only = plan_move_folder("Scratch", "my_photos", "nested", "Scratch")
    assert [item["old_path"] for item in only["moves"]] == ["my_photos/b.jpg"]

    b2 = FakeB2()
    for name in (
        "Scratch/Documents/a.jpg",
        "Scratch/my_photos/b.jpg",
        "Scratch/myXphotos/c.jpg",
        "Scratch/asus-backup/keep.txt",
    ):
        b2.files[name] = name.encode()

    plan = plan_move_contents("Scratch", "", "asus-backup", "Scratch")
    result = execute_plan(b2, plan)
    assert result["errors"] == []
    stored = {path for _drive, path, _remote, _orig in _paths()}
    assert "asus-backup/Documents/a.jpg" in stored
    assert "asus-backup/my_photos/b.jpg" in stored
    assert "asus-backup/myXphotos/c.jpg" in stored
    assert "asus-backup/keep.txt" in stored
    assert "myXphotos/c.jpg" not in stored
    assert b2.files["Scratch/asus-backup/my_photos/b.jpg"] == b"Scratch/my_photos/b.jpg"


def test_contents_inside_a_folder_leave_the_folder_name_behind(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    _add("Scratch", "Work/a.jpg", None)
    plan = plan_move_contents("Scratch", "Work", "archive", "Scratch")
    assert plan["moves"][0]["new_path"] == "archive/a.jpg"


def test_refuses_to_move_a_folder_into_itself_or_onto_an_existing_file(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    _add("Scratch", "Documents/a.jpg", "Scratch/Documents/a.jpg")
    _add("Scratch", "asus-backup/Documents/a.jpg", "Scratch/asus-backup/Documents/a.jpg", file_hash="h2")
    try:
        plan_move_folder("Scratch", "Documents", "Documents/sorted", "Scratch")
        raised = False
    except RelocateError:
        raised = True
    assert raised
    try:
        plan_move_folder("Scratch", "Documents", "asus-backup", "Scratch")
        raised = False
    except RelocateError as exc:
        raised = True
        assert "already have files" in str(exc)
    assert raised
    assert ("Scratch", "Documents/a.jpg", "Scratch/Documents/a.jpg", 1) in _paths()


def test_file_move_and_duplicate_pointer_follow_the_new_path(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    original_id = _add("Scratch", "Documents/a.jpg", "Scratch/Documents/a.jpg")
    duplicate_id = _add("Scratch", "copies/a.jpg", None, original=0)
    b2 = FakeB2()
    b2.files["Scratch/Documents/a.jpg"] = b"photo"
    b2.files["Scratch/copies/a.jpg.b2ptr"] = create_pointer_content("h1", "Scratch/Documents/a.jpg")

    file_plan = plan_move_file(duplicate_id, "Scratch", "asus-backup/copies/a.jpg")
    assert execute_plan(b2, file_plan)["errors"] == []
    pointer = json.loads(b2.files["Scratch/asus-backup/copies/a.jpg.b2ptr"].decode())
    assert pointer["original_path"] == "Scratch/Documents/a.jpg"
    assert "Scratch/copies/a.jpg.b2ptr" not in b2.files

    original = plan_move_file(original_id, "Phone", "inbox/a.jpg")
    assert execute_plan(b2, original)["errors"] == []
    assert b2.files["Phone/inbox/a.jpg"] == b"photo"
    assert "Scratch/Documents/a.jpg" not in b2.files
    followed = json.loads(b2.files["Scratch/asus-backup/copies/a.jpg.b2ptr"].decode())
    assert followed["original_path"] == "Phone/inbox/a.jpg"


def test_move_route_queues_a_job_and_search_offers_it(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    file_id = _add("Scratch", "Documents/a.jpg", None)
    captured = {}

    def fake_enqueue(kind, payload):
        captured["kind"] = kind
        captured["payload"] = payload
        return 42

    monkeypatch.setattr("web.routes.relocate.enqueue", fake_enqueue)
    client = TestClient(app)
    page = client.get("/search", params={"drive": "Scratch"})
    assert page.status_code == 200
    assert "Move everything here into" in page.text
    assert "Documents" in page.text
    file_page = client.get(f"/file/{file_id}")
    assert file_page.status_code == 200
    assert "Move in catalog and B2" in file_page.text

    response = client.post(
        "/catalog/move",
        data={"kind": "contents", "drive": "Scratch", "path": "", "into": "asus-backup", "next": "/search?drive=Scratch"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"] == "/jobs/42"
    assert captured["kind"] == "relocate"
    assert captured["payload"]["into"] == "asus-backup"

    # Catalog-only rows do not need B2. Running the job updates the path.
    run_relocate_job({"id": 1, "payload": captured["payload"]}, b2=None)
    assert ("Scratch", "asus-backup/Documents/a.jpg", None, 1) in _paths()


def test_scratch_page_uses_the_staging_folder_name(tmp_path, monkeypatch):
    from core.config import set_scratch_staging_dir
    from core.scratch import default_vault_prefix

    monkeypatch.setattr("core.scratch.SCRATCH_DIR", tmp_path / "scratch")
    monkeypatch.setattr("core.config.GUI_CONFIG_PATH", tmp_path / "config.json")
    assert default_vault_prefix() == ""
    chosen = tmp_path / "asus-backup"
    chosen.mkdir()
    set_scratch_staging_dir(str(chosen))
    assert default_vault_prefix() == "asus-backup"

    client = TestClient(app)
    page = client.get("/scratch")
    assert page.status_code == 200
    assert 'id="scratch-prefix"' in page.text
    assert 'value="asus-backup"' in page.text
    assert "only the waiting area" in page.text
