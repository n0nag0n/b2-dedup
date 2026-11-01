"""Jinja templates and shared page context."""
import secrets
from pathlib import Path

from fastapi import Request
from fastapi.templating import Jinja2Templates

from core import restore_set
from web.auth import password_required

WEB_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(WEB_DIR / "templates"))


def session_key(request: Request) -> str:
    key = request.session.get("sid")
    if not key:
        key = secrets.token_hex(16)
        request.session["sid"] = key
    return key


def pop_notice(request: Request) -> str:
    return request.session.pop("notice", "") or ""


def set_notice(request: Request, message: str) -> None:
    request.session["notice"] = message


def page_vars(request: Request, **extra) -> dict:
    key = session_key(request)
    extra.setdefault("request", request)
    extra.setdefault("password_on", password_required())
    extra.setdefault("restore_count", restore_set.count(key))
    extra.setdefault("notice", extra.get("notice") or pop_notice(request))
    extra.setdefault("session_key", key)
    return extra
