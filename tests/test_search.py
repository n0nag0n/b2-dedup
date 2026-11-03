"""Search SQL: FTS table name (not alias) and junk hiding."""
from core.catalog import _fts_query, search_files
from core.junk import is_search_hidden


def test_fts_query_splits_filename_punctuation():
    assert _fts_query("dice1.png") == "dice1 png*"
    assert _fts_query("file-name.txt") == "file name txt*"
    assert _fts_query("triplets") == "triplets*"
    assert _fts_query("IMG_8730") == "IMG_8730*"
    assert _fts_query("my photo") == "my photo*"
    assert _fts_query("...") == ""
    assert _fts_query('"already a phrase"') == '"already a phrase"'
    assert _fts_query("foo*") == "foo*"


def test_search_triplets_is_global_and_hides_junk():
    result = search_files("triplets", hide_hidden=True, per_page=20)
    assert result["total"] > 0
    for item in result["files"]:
        assert not is_search_hidden(item["file_path"])
        # Drive is a field, not a required filter.
        assert item["drive_name"]


def test_search_img_across_drives():
    result = search_files("IMG_8730", hide_hidden=True, per_page=10)
    assert result["total"] >= 1
    drives = {item["drive_name"] for item in result["files"]}
    assert drives


def test_search_filename_with_dot_finds_the_file():
    result = search_files("dice1.png", hide_hidden=True, per_page=20)
    paths = [item["file_path"] for item in result["files"]]
    assert any(path.endswith("dice1.png") for path in paths)


def test_search_punctuation_only_returns_no_rows():
    result = search_files("...", hide_hidden=True, per_page=20)
    assert result["files"] == []
    assert result["total"] == 0
