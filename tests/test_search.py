"""Search SQL: FTS table name (not alias) and junk hiding."""
from core.catalog import fts_query, search_files
from core.junk import is_search_hidden


def test_fts_query_splits_filename_punctuation():
    assert fts_query("dice1.png") == "dice1 png*"
    assert fts_query("file-name.txt") == "file name txt*"
    assert fts_query("triplets") == "triplets*"
    assert fts_query("IMG_8730") == "IMG_8730*"
    assert fts_query("my photo") == "my photo*"
    assert fts_query("...") == ""
    assert fts_query('"already a phrase"') == '"already a phrase"'
    assert fts_query("foo*") == "foo*"


def test_fts_query_keeps_wildcard_and_drops_punctuation():
    assert fts_query("dice*.png") == "dice* png*"
    assert fts_query("*.png") == "png*"
    assert fts_query("*") == ""
    assert fts_query("dice AND") == "dice*"
    assert fts_query('"dice"*') == '"dice"*'
    assert fts_query('"dice"') == '"dice"'


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


def test_search_wildcard_with_dot_finds_matching_files():
    result = search_files("dice*.png", hide_hidden=True, per_page=20)
    names = [item["file_path"].rsplit("/", 1)[-1] for item in result["files"]]
    assert "dice1.png" in names
    assert all(name.endswith(".png") for name in names)


def test_search_punctuation_only_returns_no_rows():
    result = search_files("...", hide_hidden=True, per_page=20)
    assert result["files"] == []
    assert result["total"] == 0
