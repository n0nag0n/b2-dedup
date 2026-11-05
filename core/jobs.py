"""SQLite-backed job queue and a single worker thread."""
from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone

from core.db import connect

_worker_started = False
_worker_lock = threading.Lock()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def enqueue(job_type: str, payload: dict) -> int:
    conn = connect()
    try:
        cur = conn.execute(
            "INSERT INTO jobs (type, status, payload_json, created_at, log_text) "
            "VALUES (?, 'pending', ?, ?, '')",
            (job_type, json.dumps(payload), _now()),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def get_job(job_id: int) -> dict | None:
    conn = connect(readonly=True)
    try:
        row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    finally:
        conn.close()
    return _job_row(row) if row else None


def recent_jobs(limit: int = 10) -> list[dict]:
    conn = connect(readonly=True)
    try:
        rows = conn.execute(
            "SELECT * FROM jobs ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    finally:
        conn.close()
    return [_job_row(r) for r in rows]


def _job_row(row) -> dict:
    payload = {}
    if row["payload_json"]:
        try:
            payload = json.loads(row["payload_json"])
        except json.JSONDecodeError:
            payload = {}
    done = int(row["progress_done"] or 0)
    total = int(row["progress_total"] or 0)
    return {
        "id": int(row["id"]),
        "type": row["type"],
        "status": row["status"],
        "payload": payload,
        "progress_done": done,
        "progress_total": total,
        "percent": int(done * 100 / total) if total else 0,
        "log_text": row["log_text"] or "",
        "error": row["error"],
        "created_at": row["created_at"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
    }


def _claim_next() -> dict | None:
    conn = connect()
    try:
        row = conn.execute(
            "SELECT * FROM jobs WHERE status = 'pending' ORDER BY id ASC LIMIT 1"
        ).fetchone()
        if not row:
            return None
        conn.execute(
            "UPDATE jobs SET status = 'running', started_at = ? WHERE id = ? AND status = 'pending'",
            (_now(), row["id"]),
        )
        conn.commit()
        if conn.total_changes == 0:
            return None
        return _job_row(row)
    finally:
        conn.close()


def update_progress(job_id: int, done: int, total: int, line: str | None = None) -> None:
    conn = connect()
    try:
        if line:
            conn.execute(
                "UPDATE jobs SET progress_done = ?, progress_total = ?, "
                "log_text = COALESCE(log_text, '') || ? WHERE id = ?",
                (done, total, line + "\n", job_id),
            )
        else:
            conn.execute(
                "UPDATE jobs SET progress_done = ?, progress_total = ? WHERE id = ?",
                (done, total, job_id),
            )
        conn.commit()
    finally:
        conn.close()


def finish_job(job_id: int, status: str, error: str | None = None) -> None:
    conn = connect()
    try:
        conn.execute(
            "UPDATE jobs SET status = ?, error = ?, finished_at = ? WHERE id = ?",
            (status, error, _now(), job_id),
        )
        conn.commit()
    finally:
        conn.close()


def _run_job(job: dict) -> None:
    if job["type"] == "restore":
        from core.restore import run_restore_job

        run_restore_job(job)
        return
    if job["type"] == "ingest":
        from core.ingest import run_ingest_job

        run_ingest_job(job)
        return
    if job["type"] == "verify":
        from core.verify import run_verify_job

        run_verify_job(job)
        return
    if job["type"] == "db_backup":
        from core.backup import run_backup_job

        run_backup_job(job)
        return
    if job["type"] == "db_restore":
        from core.backup import run_restore_catalog_job

        run_restore_catalog_job(job)
        return
    if job["type"] == "delete_drive":
        from core.backup import run_delete_drive_job

        run_delete_drive_job(job)
        return
    if job["type"] == "zip_export":
        from core.download import run_zip_export_job

        run_zip_export_job(job)
        return
    if job["type"] == "dates":
        from core.library import run_dates_job

        run_dates_job(job)
        return
    if job["type"] == "thumbs":
        from core.thumbs import run_thumbs_job

        run_thumbs_job(job)
        return
    finish_job(job["id"], "error", f"Unknown job type: {job['type']}")


def _worker_loop() -> None:
    while True:
        try:
            job = _claim_next()
            if job:
                try:
                    _run_job(job)
                except Exception as e:
                    finish_job(job["id"], "error", str(e))
            else:
                time.sleep(0.75)
        except Exception:
            time.sleep(2)


def start_worker() -> None:
    global _worker_started
    with _worker_lock:
        if _worker_started:
            return
        t = threading.Thread(target=_worker_loop, name="storageinsights-jobs", daemon=True)
        t.start()
        _worker_started = True
