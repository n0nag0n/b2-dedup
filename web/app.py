"""StorageInsights FastAPI application."""
from __future__ import annotations

import os
import secrets

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from core.catalog import get_vault_overview
from core.db import init_db
from core.download import cleanup_exports
from core.health import get_vault_health
from core.jobs import recent_jobs, start_worker
from core.scratch import cleanup_stale_batches
from web.auth import password_required, vault_password
from web.routes import router
from web.templating import WEB_DIR, page_vars, templates

SESSION_SECRET = os.environ.get("VAULT_SESSION_SECRET") or secrets.token_hex(32)
LOGIN_PATH = "/login"


def create_app() -> FastAPI:
    app = FastAPI(title="StorageInsights", docs_url=None, redoc_url=None)
    app.mount("/static", StaticFiles(directory=str(WEB_DIR / "static")), name="static")
    app.include_router(router)

    @app.on_event("startup")
    def _startup():
        init_db()
        try:
            cleanup_exports()
        except Exception:  # noqa: BLE001 - cleanup must never block startup
            pass
        try:
            cleanup_stale_batches()
        except Exception:  # noqa: BLE001 - cleanup must never block startup
            pass
        start_worker()

    @app.middleware("http")
    async def _auth_gate(request: Request, call_next):
        if not password_required():
            return await call_next(request)
        path = request.url.path
        if path.startswith("/static/") or path == LOGIN_PATH:
            return await call_next(request)
        if request.session.get("authed"):
            return await call_next(request)
        return RedirectResponse(url=LOGIN_PATH, status_code=303)

    @app.get("/login", response_class=HTMLResponse)
    def login_form(request: Request):
        if not password_required():
            return RedirectResponse(url="/", status_code=303)
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": None},
        )

    @app.post("/login", response_class=HTMLResponse)
    def login_submit(request: Request, password: str = Form(...)):
        if secrets.compare_digest(password, vault_password()):
            request.session["authed"] = True
            return RedirectResponse(url="/", status_code=303)
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "That password did not match."},
            status_code=401,
        )

    @app.post("/logout")
    def logout(request: Request):
        request.session.clear()
        return RedirectResponse(url=LOGIN_PATH if password_required() else "/", status_code=303)

    @app.get("/", response_class=HTMLResponse)
    def home(request: Request):
        return templates.TemplateResponse(
            "home.html",
            page_vars(
                request,
                overview=get_vault_overview(),
                jobs=recent_jobs(5),
                health=get_vault_health(),
            ),
        )

    app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET, same_site="lax")
    return app


app = create_app()
