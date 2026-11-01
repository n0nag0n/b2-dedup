"""Junk-aware walk, incremental skip, and ingest preview."""
import os
from pathlib import Path

from core.ingest import (
    iter_source_files,
    mtimes_match,
    preview_ingest,
    process_file,
    signatures_match,
)
from core.volumes import resolve_allowed_path


def _write(path: Path, data: bytes = b"hello"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def test_mtimes_match_second_precision():
    assert mtimes_match("2019-06-05T00:59:14.123456+00:00", "2019-06-05T00:59:14+00:00")
    assert not mtimes_match("2019-06-05T00:59:15+00:00", "2019-06-05T00:59:14+00:00")
    assert not mtimes_match(None, "2019-06-05T00:59:14+00:00")


def test_signatures_match_size_and_mtime():
    assert signatures_match(12, "2019-06-05T00:59:14+00:00", 12, "2019-06-05T00:59:14+00:00")
    assert not signatures_match(13, "2019-06-05T00:59:14+00:00", 12, "2019-06-05T00:59:14+00:00")


def test_walk_skips_junk_keeps_git(tmp_path):
    _write(tmp_path / "keep" / "photo.jpg")
    _write(tmp_path / "site" / "node_modules" / "leftpad" / "index.js")
    _write(tmp_path / "proj" / "venv" / "lib" / "os.py")
    _write(tmp_path / "keep" / "._IMG_001.JPG")
    _write(tmp_path / "keep" / ".DS_Store")
    _write(tmp_path / "repo" / ".git" / "HEAD", b"ref")

    rels = [rel.as_posix() for _fp, rel in iter_source_files(tmp_path, tmp_path)]
    assert "keep/photo.jpg" in rels
    assert "repo/.git/HEAD" in rels
    assert not any("node_modules" in r for r in rels)
    assert not any("venv" in r for r in rels)
    assert "keep/._IMG_001.JPG" not in rels
    assert "keep/.DS_Store" not in rels


def test_preview_counts_junk_and_new(tmp_path):
    _write(tmp_path / "keep" / "photo.jpg", b"abcdefgh")
    _write(tmp_path / "site" / "node_modules" / "x" / "a.js")
    _write(tmp_path / "keep" / "._skip")

    preview = preview_ingest(tmp_path, "TestDrive", tmp_path, tracked={})
    assert preview["total_files"] >= 3
    assert preview["junk_skipped"] >= 2
    assert preview["already_tracked"] == 0
    assert preview["new_or_changed"] == 1
    assert preview["new_bytes"] == 8


def _isolate_db(tmp_path, monkeypatch):
    db_path = tmp_path / "b2_dedup.db"
    monkeypatch.setattr("core.db.DB_PATH", db_path)
    monkeypatch.setattr("core.paths.DB_PATH", db_path)
    monkeypatch.setattr("core.ingest.CACHE_PATH", tmp_path / "cache.json")
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


def test_unchanged_file_skips_hash(tmp_path, monkeypatch):
    _isolate_db(tmp_path, monkeypatch)
    source = tmp_path / "drive"
    photo = _write(source / "Pictures" / "IMG.JPG", b"original-bytes")
    rel = Path("Pictures/IMG.JPG")

    first = process_file((photo, rel, "TestDrive", True, False, None))
    assert first[0] == "scanned"

    def boom(*_a, **_k):
        raise AssertionError("incremental skip should not hash")

    monkeypatch.setattr("core.ingest.sha256_file", boom)
    second = process_file((photo, rel, "TestDrive", True, False, None))
    assert second[0] == "already_tracked"


def test_second_ingest_skips_unchanged_via_index(tmp_path, monkeypatch):
    _isolate_db(tmp_path, monkeypatch)
    source = tmp_path / "drive"
    _write(source / "Pictures" / "IMG.JPG", b"original-bytes")
    from core.ingest import run_ingest

    first = run_ingest(source, "TestDrive", None, scan_only=True, workers=1, show_progress_bar=False)
    assert first["stats"]["scanned"] == 1
    second = run_ingest(source, "TestDrive", None, scan_only=True, workers=1, show_progress_bar=False)
    assert second["stats"]["already_tracked"] == 1
    assert second["stats"]["scanned"] == 0


def test_changed_file_is_rehashed(tmp_path, monkeypatch):
    _isolate_db(tmp_path, monkeypatch)
    source = tmp_path / "drive"
    photo = _write(source / "Pictures" / "IMG.JPG", b"original-bytes")
    rel = Path("Pictures/IMG.JPG")

    assert process_file((photo, rel, "TestDrive", True, False, None))[0] == "scanned"
    photo.write_bytes(b"changed-bytes-now")
    os.utime(photo, (photo.stat().st_atime + 10, photo.stat().st_mtime + 10))
    result = process_file((photo, rel, "TestDrive", True, False, None))
    assert result[0] == "scanned"


def test_host_path_stays_inside_root(tmp_path):
    (tmp_path / "ok").mkdir()
    roots = [tmp_path.resolve()]
    inside = resolve_allowed_path(str(tmp_path / "ok"), roots)
    assert inside is not None
    assert inside == (tmp_path / "ok").resolve()
    assert resolve_allowed_path("../outside", roots) is None
    assert resolve_allowed_path("../../etc/passwd", roots) is None
