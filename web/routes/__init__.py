"""HTTP routes for StorageInsights."""
from fastapi import APIRouter

from web.routes import (
    browse,
    downloads,
    files,
    ingest,
    jobs,
    library,
    relocate,
    restore,
    scratch,
    search,
    settings,
)

router = APIRouter()
router.include_router(search.router)
router.include_router(library.router)
router.include_router(browse.router)
router.include_router(files.router)
router.include_router(downloads.router)
router.include_router(relocate.router)
router.include_router(restore.router)
router.include_router(ingest.router)
router.include_router(scratch.router)
router.include_router(jobs.router)
router.include_router(settings.router)
