"""Search SQL: FTS table name (not alias) and junk hiding."""
from core.catalog import search_files
from core.junk import is_search_hidden


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
