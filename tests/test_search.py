"""Search SQL: FTS table name (not alias) and junk hiding."""
from core.catalog import _name_matcher, fts_query, search_files
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
    assert result["folders"] == []
    assert result["total"] == 0
    assert result["folder_total"] == 0


def test_search_groups_the_same_bytes_and_uses_the_filename():
    result = search_files("dice.jpg", hide_hidden=True, per_page=50)
    grouped = [item for item in result["files"] if item["place_count"] > 1]
    assert grouped
    for item in grouped:
        assert item["name"] == item["file_path"].replace("\\", "/").rsplit("/", 1)[-1]
        assert item["id"] not in {place["id"] for place in item["places"]}
        assert item["other_count"] == len(item["places"]) + item["places_more"]
        assert item["places_label"].startswith("Also in ")
    shown = {item["id"] for item in result["files"]}
    assert len(shown) == len(result["files"])


def test_search_backup_lists_folders_without_every_child_file():
    result = search_files("backup", hide_hidden=True, per_page=50, folder_per_page=100)
    assert result["folder_total"] > 1
    assert result["folders"]
    matcher = _name_matcher(fts_query("backup"))
    for folder in result["folders"]:
        assert matcher(folder["name"])
        assert folder["name"] == folder["path"].rsplit("/", 1)[-1]
    for item in result["files"]:
        assert matcher(item["name"])
    names = [item["name"].lower() for item in result["files"]]
    assert not any(name.startswith("dice") for name in names)
    assert result["total"] < 500


def test_search_page_shows_filename_places_and_folders():
    from fastapi.testclient import TestClient

    from web.app import app

    client = TestClient(app)
    page = client.get("/search", params={"q": "dice"})
    assert page.status_code == 200
    html = page.text
    assert 'class="file-name"' in html
    assert ">dice1.png</a>" in html
    assert "· copy" not in html
    assert 'aria-label="Download"' in html
    assert 'aria-label="Add to restore set"' in html
    assert "Also in " in html
    assert 'title="' in html
    # The full path stays on the tooltip; the link text is the file name.
    assert "Downloads/dice/dice1.png</a>" not in html

    backup = client.get("/search", params={"q": "backup"})
    assert backup.status_code == 200
    text = backup.text
    assert ">Folders</h2>" in text
    assert ">Files</h2>" in text
    assert "Austin Dell Backup" in text or ">Backup</a>" in text
    assert "No files or folders match" not in text
