"""Search replaces the old library and timeline pages. Settings still renders."""
from fastapi.testclient import TestClient

from web.app import app


def test_old_library_routes_redirect_into_search():
    client = TestClient(app)
    library = client.get("/library", follow_redirects=False)
    assert library.status_code == 302
    assert library.headers["location"] == "/search"

    typed = client.get("/library", params={"type": "Image", "year": "2016"}, follow_redirects=False)
    assert typed.status_code == 302
    assert "view=moments" in typed.headers["location"]
    assert "type=Image" in typed.headers["location"]
    assert "year=2016" in typed.headers["location"]

    timeline = client.get("/timeline", params={"year": "2014", "month": "07"}, follow_redirects=False)
    assert timeline.status_code == 302
    location = timeline.headers["location"]
    assert location.startswith("/search?")
    assert "view=moments" in location
    assert "year=2014" in location
    assert "month=07" in location

    moment = client.get(
        "/moment",
        params={"title": "Home", "day": "2014-07"},
        follow_redirects=False,
    )
    assert moment.status_code == 302
    assert "view=moment" in moment.headers["location"]
    assert "day=2014-07" in moment.headers["location"]


def test_browse_redirects_into_search():
    client = TestClient(app)
    page = client.get(
        "/browse",
        params={"drive": "Family", "path": "Photos", "hidden": "1"},
        follow_redirects=False,
    )
    assert page.status_code == 302
    location = page.headers["location"]
    assert location.startswith("/search?")
    assert "drive=Family" in location
    assert "path=Photos" in location
    assert "hidden=1" in location


def test_nav_is_search_without_library_or_browse():
    client = TestClient(app)
    home = client.get("/")
    assert home.status_code == 200
    assert 'href="/search"' in home.text
    assert 'href="/search?view=moments"' in home.text
    assert 'href="/library"' not in home.text
    assert 'href="/browse' not in home.text
    assert ">Library</a>" not in home.text
    assert ">Browse</a>" not in home.text
    assert 'id="site-nav"' in home.text
    assert 'id="nav-toggle"' in home.text
    assert 'id="theme-toggle"' in home.text
    assert "/static/theme.js" in home.text


def test_login_has_theme_toggle(monkeypatch):
    monkeypatch.setenv("VAULT_PASSWORD", "secret")
    client = TestClient(app)
    page = client.get("/login")
    assert page.status_code == 200
    assert 'id="theme-toggle"' in page.text
    assert "theme-toggle-page" in page.text


def test_settings_has_date_and_preview_jobs():
    client = TestClient(app)
    page = client.get("/settings")
    assert page.status_code == 200
    assert "Dates and previews" in page.text
    assert "Fill dates from folders" in page.text
    assert "Start thumbnail job" in page.text
    assert 'action="/settings/dates"' in page.text
    assert 'action="/settings/thumbs"' in page.text


def test_settings_groups_sections():
    client = TestClient(app)
    page = client.get("/settings")
    text = page.text
    assert page.status_code == 200
    for href in ("#storage", "#ingest", "#catalog", "#danger"):
        assert f'href="{href}"' in text
    for anchor in ("connection", "downloads", "ingest", "scratch", "backup", "verify", "library", "danger"):
        assert f'id="{anchor}"' in text
    assert "Save connection" in text
    assert "Save download limits" in text
    assert "Save retention" in text
    assert "Junk rules" in text
