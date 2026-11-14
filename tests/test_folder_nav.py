"""Folder photo grid and previous/next on the file page."""
from core.catalog import choose_folder_layout, folder_neighbors, visual_label


def test_visual_label_names_photos_and_videos():
    assert visual_label(1, 0) == "1 photo"
    assert visual_label(2, 0) == "2 photos"
    assert visual_label(0, 1) == "1 video"
    assert visual_label(4, 2) == "4 photos and 2 videos"


def test_gallery_opens_for_a_visual_folder_and_can_be_forced_off():
    assert choose_folder_layout("", total=10, images=8, videos=1) == "photos"
    assert choose_folder_layout("", total=10, images=2, videos=1) == "list"
    assert choose_folder_layout("", total=3, images=3, videos=0) == "list"
    assert choose_folder_layout("", total=4, images=0, videos=4) == "photos"
    assert choose_folder_layout("", total=2, images=2, videos=0, file_type="Image") == "photos"
    assert choose_folder_layout("list", total=10, images=10, videos=0) == "list"
    assert choose_folder_layout("photos", total=5, images=0, videos=0) == "photos"


def _isolate(tmp_path, monkeypatch):
    monkeypatch.setattr("core.db.DB_PATH", tmp_path / "b2_dedup.db")
    from core.db import init_db

    init_db()


def _seed(meta, rows):
    from core.db import connect
    from core.ingest import insert_file_record

    conn = connect()
    try:
        cur = conn.cursor()
        ids = []
        for digest, path, kind, mime in rows:
            insert_file_record(
                cur, digest, 1024, "Camera", path, f"Camera/{path}", 1,
                {**meta, "mime_type": mime, "file_type": kind},
            )
            ids.append(cur.execute("SELECT id FROM files WHERE file_path = ?", (path,)).fetchone()[0])
        conn.commit()
    finally:
        conn.close()
    return ids


def test_neighbors_walk_photos_and_videos_and_skip_sidecars(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    meta = {"mtime": "2026-01-01T00:00:00", "ctime": None, "atime": None}
    jpg_a, xmp, clip, jpg_b = _seed(meta, [
        ("a" * 64, "trip/a.jpg", "Image", "image/jpeg"),
        ("b" * 64, "trip/a.xmp", "Other", "application/octet-stream"),
        ("c" * 64, "trip/b.mp4", "Video", "video/mp4"),
        ("d" * 64, "trip/c.jpg", "Image", "image/jpeg"),
    ])

    photo = folder_neighbors("Camera", "trip/b.mp4", clip, media_only=True)
    assert photo["total"] == 3
    assert photo["index"] == 2
    assert photo["noun"] == "photos and videos"
    assert photo["prev"]["id"] == jpg_a
    assert photo["next"]["id"] == jpg_b
    assert [item["name"] for item in photo["around"]] == ["a.jpg", "b.mp4", "c.jpg"]

    sidecar = folder_neighbors("Camera", "trip/a.xmp", xmp, media_only=False)
    assert sidecar["total"] == 4
    assert sidecar["noun"] == "files"
    assert sidecar["prev"]["name"] == "a.jpg"
    assert sidecar["next"]["name"] == "b.mp4"


def test_neighbors_follow_the_folder_sort(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    meta = {"mtime": "2026-01-01T00:00:00", "ctime": None, "atime": None}
    first, second = _seed(meta, [
        ("a" * 64, "trip/a.jpg", "Image", "image/jpeg"),
        ("b" * 64, "trip/b.jpg", "Image", "image/jpeg"),
    ])
    from core.db import connect

    conn = connect()
    try:
        conn.execute("UPDATE files SET best_taken_at = ? WHERE id = ?", ("2020-01-01T00:00:00", first))
        conn.execute("UPDATE files SET best_taken_at = ? WHERE id = ?", ("2024-01-01T00:00:00", second))
        conn.commit()
    finally:
        conn.close()

    newest_first = folder_neighbors(
        "Camera", "trip/a.jpg", first, sort="taken_desc", media_only=True
    )
    assert newest_first["sort"] == "taken_desc"
    assert newest_first["index"] == 2
    assert newest_first["prev"]["id"] == second
    assert newest_first["next"] is None


def test_folder_page_uses_a_photo_grid_for_images_and_videos(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    meta = {"mtime": "2026-01-01T00:00:00", "ctime": None, "atime": None}
    _seed(meta, [
        ("a" * 64, "trip/a.jpg", "Image", "image/jpeg"),
        ("b" * 64, "trip/b.jpg", "Image", "image/jpeg"),
        ("c" * 64, "trip/c.mp4", "Video", "video/mp4"),
        ("d" * 64, "trip/d.jpg", "Image", "image/jpeg"),
        ("e" * 64, "trip/notes.txt", "Document", "text/plain"),
    ])
    from fastapi.testclient import TestClient

    from web.app import app

    client = TestClient(app)
    page = client.get("/search", params={"drive": "Camera", "path": "trip"})
    assert page.status_code == 200
    assert 'class="photo-grid"' in page.text
    assert 'src="/thumbs/' + ("a" * 64) + '"' in page.text
    assert "fetches each photo from B2" in page.text
    assert "▶ Video" in page.text
    assert "3 photos and 1 video" in page.text
    assert "1 other file stays in the list" in page.text
    assert "notes.txt" not in page.text
    assert 'aria-current="page">Photos' in page.text

    listed = client.get("/search", params={"drive": "Camera", "path": "trip", "layout": "list"})
    assert 'class="photo-grid"' not in listed.text
    assert "notes.txt" in listed.text
    assert 'aria-current="page">List' in listed.text

    opened = client.get("/file/" + str(_id_for("trip/c.mp4")))
    assert opened.status_code == 200
    assert 'rel="prev"' in opened.text
    assert 'rel="next"' in opened.text
    assert "3 of 4 photos and videos" in opened.text
    assert "Photos in this folder" in opened.text
    assert "notes.txt" not in opened.text
    assert 'data-prev="/file/' in opened.text
    assert "ArrowRight" in opened.text


def _id_for(path: str) -> int:
    from core.db import connect

    conn = connect(readonly=True)
    try:
        return conn.execute("SELECT id FROM files WHERE file_path = ?", (path,)).fetchone()[0]
    finally:
        conn.close()
