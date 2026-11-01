"""Junk / search-hide rules."""
from core.junk import classify_path, is_ingest_skip, is_search_hidden


def test_appledouble_is_skipped():
    assert is_ingest_skip("Pictures/._IMG_001.JPG")
    assert is_ingest_skip("._print")


def test_node_modules_skipped_but_git_kept():
    assert is_ingest_skip("site/node_modules/leftpad/index.js")
    assert is_ingest_skip("proj/venv/lib/python3.12/os.py")
    assert is_ingest_skip("php/vendor/autoload.php")
    assert not is_ingest_skip("repo/.git/HEAD")
    assert not is_ingest_skip("repo/.hg/store/data/foo")


def test_git_hidden_from_search_not_from_ingest():
    assert is_search_hidden("repo/.git/HEAD")
    assert is_search_hidden("repo/.hg/requires")
    assert not is_ingest_skip("repo/.git/HEAD")


def test_normal_photo_kept():
    path = "Austin/Pictures/Family Life/2010/July/IMG_8730.JPG"
    assert not is_ingest_skip(path)
    assert not is_search_hidden(path)
    flags = classify_path(path)
    assert flags["is_junk"] is False
    assert flags["searchable"] is True


def test_ds_store_and_thumbs():
    assert is_ingest_skip("Somewhere/.DS_Store")
    assert is_ingest_skip("Windows/Thumbs.db")
