"""Date backfill, library grid, and moment grouping."""
from core.db import connect
from core.ingest import insert_file_record
from core.library import backfill_dates, list_library, list_moment_photos, list_moments
from core.time import SOURCE_EXIF


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
    from core.db import init_db

    init_db()
    return db_path


def _meta(mtime=None):
    return {
        "mtime": mtime,
        "ctime": None,
        "atime": None,
        "mime_type": "image/jpeg",
        "file_type": "Image",
    }


def _add(drive, path, remote, size=10, mtime=None, original=1, file_hash=None):
    conn = connect()
    try:
        insert_file_record(
            conn.cursor(),
            file_hash or ("h" + path),
            size,
            drive,
            path,
            remote,
            original,
            _meta(mtime),
        )
        conn.commit()
    finally:
        conn.close()


def test_backfill_uses_folder_date(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    _add(
        "big-bertha",
        "Pictures/Family Life/2010/August 2010/IMG_1.JPG",
        "big-bertha/Pictures/Family Life/2010/August 2010/IMG_1.JPG",
        mtime="2020-01-01T00:00:00+00:00",
    )
    # Ingest already stamped the folder date; backfill should leave it in place
    # unless the row is still empty. Clear it to exercise the job path.
    conn = connect()
    try:
        conn.execute("UPDATE files SET best_taken_at = NULL, taken_at_source = NULL")
        conn.commit()
    finally:
        conn.close()
    stats = backfill_dates()
    assert stats["updated"] == 1
    conn = connect(readonly=True)
    try:
        row = conn.execute("SELECT best_taken_at, taken_at_source FROM files").fetchone()
    finally:
        conn.close()
    assert row["taken_at_source"] == "folder"
    assert row["best_taken_at"].startswith("2010-08")


def test_backfill_does_not_overwrite_exif(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    _add("D", "Vacation/IMG.JPG", "D/Vacation/IMG.JPG", mtime="2011-01-01T00:00:00+00:00")
    conn = connect()
    try:
        conn.execute(
            "UPDATE files SET best_taken_at = ?, taken_at_source = ?",
            ("2009-12-25T08:00:00+00:00", SOURCE_EXIF),
        )
        conn.commit()
    finally:
        conn.close()
    backfill_dates()
    conn = connect(readonly=True)
    try:
        row = conn.execute("SELECT best_taken_at, taken_at_source FROM files").fetchone()
    finally:
        conn.close()
    assert row["taken_at_source"] == SOURCE_EXIF
    assert row["best_taken_at"].startswith("2009-12-25")


def test_library_lists_original_images(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    _add("D", "2016/July 2016/a.jpg", "D/2016/July 2016/a.jpg", file_hash="a" * 64)
    _add(
        "D",
        "2016/July 2016/a-copy.jpg",
        None,
        original=0,
        file_hash="a" * 64,
    )
    _add("D", "readme.txt", "D/readme.txt", file_hash="b" * 64)
    conn = connect()
    try:
        conn.execute(
            "UPDATE files SET file_type = 'Document' WHERE file_path LIKE '%.txt'"
        )
        conn.commit()
    finally:
        conn.close()
    backfill_dates()
    photos = list_library(year="2016", file_type="Image")
    assert photos["total"] == 1
    assert photos["files"][0]["name"] == "a.jpg"


def test_moments_group_by_folder_and_day(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    _add(
        "big-bertha",
        "Desktop/Photos to organize/They're Both Home!/140CANON/IMG_1.JPG",
        "x/1.jpg",
        mtime="2014-07-02T12:00:00+00:00",
        file_hash="c" * 64,
    )
    _add(
        "big-bertha",
        "Desktop/Photos to organize/They're Both Home!/140CANON/IMG_2.JPG",
        "x/2.jpg",
        mtime="2014-07-02T13:00:00+00:00",
        file_hash="d" * 64,
    )
    backfill_dates()
    moments = list_moments(year="2014", month="07")
    assert moments
    assert moments[0]["title"] == "They're Both Home!"
    assert moments[0]["photos"] == 2
    assert moments[0]["day"] == "2014-07"
    photos = list_moment_photos(moments[0]["folder"], moments[0]["day"])
    assert photos["total"] == 2


def test_library_includes_documents_and_filters_by_type(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    _add("D", "2016/July 2016/a.jpg", "D/2016/July 2016/a.jpg", file_hash="a" * 64)
    conn = connect()
    try:
        insert_file_record(
            conn.cursor(),
            "b" * 64,
            20,
            "D",
            "2016/July 2016/notes.txt",
            "D/2016/July 2016/notes.txt",
            1,
            {
                "mtime": "2016-07-04T12:00:00+00:00",
                "ctime": None,
                "atime": None,
                "mime_type": "text/plain",
                "file_type": "Document",
            },
        )
        conn.commit()
    finally:
        conn.close()
    backfill_dates()

    everything = list_library(year="2016")
    names = {item["name"] for item in everything["files"]}
    assert names == {"a.jpg", "notes.txt"}

    docs = list_library(year="2016", file_type="Document")
    assert [item["name"] for item in docs["files"]] == ["notes.txt"]
    assert docs["files"][0]["file_type"] == "Document"

    moments = list_moments(year="2016", month="07")
    assert moments
    assert moments[0]["photos"] == 2
    docs_only = list_moments(year="2016", month="07", file_type="Document")
    assert docs_only
    assert docs_only[0]["photos"] == 1
    assert docs_only[0]["noun"] == "file"
