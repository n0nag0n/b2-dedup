"""Checked rows on Search: download, restore set, and move."""
from urllib.parse import parse_qs, urlparse

from fastapi.testclient import TestClient

from core.db import connect, init_db
from core.ingest import insert_file_record
from core.relocate import RelocateError, plan_from_payload, plan_move_selection, run_relocate_job
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


def _add(drive, path, remote=None, *, file_hash="h1", size=10):
    conn = connect()
    try:
        insert_file_record(
            conn.cursor(),
            file_hash,
            size,
            drive,
            path,
            remote,
            1,
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


def _paths():
    conn = connect(readonly=True)
    try:
        return [
            (row["drive_name"], row["file_path"])
            for row in conn.execute(
                "SELECT drive_name, file_path FROM files ORDER BY file_path"
            )
        ]
    finally:
        conn.close()


def test_selection_move_keeps_folder_names_and_flattens_loose_files(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    photo = _add("Scratch", "Photos/a.jpg", file_hash="a")
    nested = _add("Scratch", "Documents/sub/b.jpg", file_hash="b")
    _add("Scratch", "Documents/c.jpg", file_hash="c")
    plan = plan_move_selection(
        [photo, nested],
        [("Scratch", "Documents")],
        "inbox",
        "Scratch",
    )
    assert plan["count"] == 3
    moved = {item["old_path"]: item["new_path"] for item in plan["moves"]}
    assert moved["Photos/a.jpg"] == "inbox/a.jpg"
    assert moved["Documents/sub/b.jpg"] == "inbox/Documents/sub/b.jpg"
    assert moved["Documents/c.jpg"] == "inbox/Documents/c.jpg"

    again = plan_from_payload({
        "kind": "selection",
        "file_ids": [photo, nested],
        "folders": [["Scratch", "Documents"]],
        "into": "inbox",
        "dest_drive": "Scratch",
    })
    assert again["count"] == 3


def test_selection_move_rejects_two_files_with_the_same_name(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    first = _add("Scratch", "Photos/a.jpg", file_hash="a")
    second = _add("Scratch", "Other/a.jpg", file_hash="b")
    failed = False
    try:
        plan_move_selection([first, second], [], "inbox", "Scratch")
    except RelocateError as exc:
        failed = True
        assert "would be used twice" in str(exc)
    assert failed


def test_selection_move_skips_a_file_already_in_that_folder(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    file_id = _add("Scratch", "inbox/a.jpg", file_hash="a")
    failed = False
    try:
        plan_move_selection([file_id], [], "inbox", "Scratch")
    except RelocateError as exc:
        failed = True
        assert "already in that folder" in str(exc)
    assert failed


def test_search_checkboxes_download_move_and_restore(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    photo = _add("Scratch", "Photos/a.jpg", file_hash="a", size=20)
    other = _add("Scratch", "Other/b.jpg", file_hash="b", size=30)
    _add("Scratch", "Documents/c.jpg", file_hash="c", size=40)
    monkeypatch.setattr("core.bulk.forced_to_stream", lambda _size: False)
    monkeypatch.setattr("core.bulk.selection_error", lambda _count, _size: None)
    monkeypatch.setattr("web.routes.search.get_bucket_name", lambda: "test-bucket")
    monkeypatch.setattr("web.routes.search.shared_manager", lambda _bucket: object())
    monkeypatch.setattr(
        "web.routes.search.stream_zip",
        lambda records, _b2: iter([b"PK"]),
    )
    captured = {}

    def fake_enqueue(kind, payload):
        captured["kind"] = kind
        captured["payload"] = payload
        return 7

    monkeypatch.setattr("web.routes.relocate.enqueue", fake_enqueue)
    client = TestClient(app)

    page = client.get("/search", params={"drive": "Scratch"})
    assert page.status_code == 200
    assert 'id="pick-form"' in page.text
    assert "Select all on this page" in page.text
    assert f'value="{photo}"' in page.text
    assert "Scratch&#9;Documents" in page.text
    assert 'aria-label="Select folder Documents"' in page.text

    empty = client.post(
        "/search/bulk",
        data={"action": "download", "next": "/search?drive=Scratch"},
        follow_redirects=False,
    )
    assert empty.status_code == 303
    missed = client.get(empty.headers["location"])
    assert "Select at least one" in missed.text

    added = client.post(
        "/search/bulk",
        data={
            "action": "restore",
            "file_id": [str(photo), str(other)],
            "next": "/search?drive=Scratch",
        },
        follow_redirects=False,
    )
    assert added.status_code == 303
    assert "drive=Scratch" in added.headers["location"]
    notice = client.get(added.headers["location"])
    assert "Added 2 files to the restore set." in notice.text

    again = client.post(
        "/search/bulk",
        data={"action": "restore", "file_id": str(photo), "next": "/search"},
        follow_redirects=True,
    )
    assert "Already in the restore set." in again.text

    one = client.post(
        "/search/bulk",
        data={"action": "download", "file_id": str(photo), "next": "/search"},
        follow_redirects=False,
    )
    assert one.status_code == 303
    assert one.headers["location"] == f"/download/{photo}"

    zipped = client.post(
        "/search/bulk",
        data={
            "action": "download",
            "folder": "Scratch\tDocuments",
            "file_id": str(other),
            "next": "/search",
        },
    )
    assert zipped.status_code == 200
    assert zipped.headers["content-type"].startswith("application/zip")
    assert zipped.content == b"PK"

    monkeypatch.setattr("core.bulk.forced_to_stream", lambda _size: True)
    large = client.post(
        "/search/bulk",
        data={"action": "download", "file_id": [str(photo), str(other)], "next": "/search"},
        follow_redirects=False,
    )
    assert large.status_code == 303
    assert large.headers["location"] == "/restore-set"
    large_page = client.get("/restore-set")
    assert "download the ZIP from the restore set" in large_page.text

    single = client.post(
        "/search/bulk",
        data={"action": "move", "file_id": str(photo), "next": "/search?drive=Scratch"},
        follow_redirects=False,
    )
    single_qs = parse_qs(urlparse(single.headers["location"]).query)
    assert single_qs["kind"] == ["file"]
    assert single_qs["file_id"] == [str(photo)]

    folder = client.post(
        "/search/bulk",
        data={"action": "move", "folder": "Scratch\tDocuments", "next": "/search"},
        follow_redirects=False,
    )
    folder_qs = parse_qs(urlparse(folder.headers["location"]).query)
    assert folder_qs["kind"] == ["folder"]
    assert folder_qs["path"] == ["Documents"]

    both = client.post(
        "/search/bulk",
        data={
            "action": "move",
            "file_id": [str(photo), str(other)],
            "next": "/search?drive=Scratch",
        },
        follow_redirects=False,
    )
    assert both.status_code == 303
    chooser = client.get(both.headers["location"])
    assert chooser.status_code == 200
    assert "Move 2 files" in chooser.text

    queued = client.post(
        "/catalog/move",
        data={
            "kind": "selection",
            "into": "inbox",
            "dest_drive": "Scratch",
            "next": "/search?drive=Scratch",
        },
        follow_redirects=False,
    )
    assert queued.status_code == 303
    assert queued.headers["location"] == "/jobs/7"
    assert captured["payload"]["kind"] == "selection"
    assert captured["payload"]["into"] == "inbox"
    assert set(captured["payload"]["file_ids"]) == {photo, other}

    run_relocate_job({"id": 1, "payload": captured["payload"]}, b2=None)
    assert ("Scratch", "inbox/a.jpg") in _paths()
    assert ("Scratch", "inbox/b.jpg") in _paths()
    assert ("Scratch", "Documents/c.jpg") in _paths()
