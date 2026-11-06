"""GET /browse — old folder links now open inside Search."""
from urllib.parse import urlencode

from fastapi import APIRouter
from fastapi.responses import RedirectResponse

router = APIRouter()


def _target(**params: str) -> str:
    clean = {key: value for key, value in params.items() if value not in ("", None)}
    query = urlencode(clean)
    return "/search" if not query else f"/search?{query}"


@router.get("/browse")
def browse_page(
    drive: str = "",
    path: str = "",
    page: int = 1,
    seek: str = "",
    hidden: str = "",
):
    return RedirectResponse(
        _target(
            drive=drive,
            path=path,
            page=str(page) if page and int(page) > 1 else "",
            seek=seek,
            hidden="1" if hidden == "1" else "",
        ),
        status_code=302,
    )
