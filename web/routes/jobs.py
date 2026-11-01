"""Job status pages."""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from core.jobs import get_job, recent_jobs
from web.templating import page_vars, templates

router = APIRouter()


@router.get("/jobs", response_class=HTMLResponse)
def jobs_index(request: Request):
    return templates.TemplateResponse(
        "jobs.html",
        page_vars(request, jobs=recent_jobs(20), job=None),
    )


@router.get("/jobs/{job_id}", response_class=HTMLResponse)
def job_detail(request: Request, job_id: int):
    job = get_job(job_id)
    if not job:
        return templates.TemplateResponse(
            "not_found.html",
            page_vars(request, message="Job not found."),
            status_code=404,
        )
    return templates.TemplateResponse(
        "jobs.html",
        page_vars(request, jobs=recent_jobs(20), job=job),
    )
