"""Library / timeline / settings pages render."""
from fastapi.testclient import TestClient

from web.app import app


def test_library_and_timeline_pages():
    client = TestClient(app)
    lib = client.get("/library")
    assert lib.status_code == 200
    assert "Library" in lib.text
    assert "Grid" in lib.text
    assert "Timeline" in lib.text
    assert "All types" in lib.text
    assert "Document" in lib.text or "Type" in lib.text

    timeline = client.get("/timeline")
    assert timeline.status_code == 200
    assert "Timeline" in timeline.text


def test_nav_includes_library():
    client = TestClient(app)
    home = client.get("/")
    assert home.status_code == 200
    assert 'href="/library"' in home.text
    assert ">Library</a>" in home.text


def test_settings_has_library_jobs():
    client = TestClient(app)
    page = client.get("/settings")
    assert page.status_code == 200
    assert "Library dates and previews" in page.text
    assert "Fill dates from folders" in page.text
    assert "Start thumbnail job" in page.text
    assert 'action="/settings/dates"' in page.text
    assert 'action="/settings/thumbs"' in page.text
