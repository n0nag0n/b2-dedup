"""Check that catalog originals still exist on B2.

Sample mode probes a random subset (daily trust). Full mode lists the
bucket once and diffs it against every original with an upload_path.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from core.b2 import B2Manager
from core.catalog import count_originals, format_size, load_originals
from core.config import get_bucket_name, set_last_verify

DEFAULT_SAMPLE_SIZE = 200
MISSING_PREVIEW = 40


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _label(rec: dict) -> str:
    return rec.get("upload_path") or f"{rec.get('drive_name')}/{rec.get('file_path')}"


def _empty_result(*, mode: str, sample_size: int = 0) -> dict:
    return {
        "at": _now(),
        "mode": mode,
        "sample_size": sample_size,
        "checked": 0,
        "found": 0,
        "missing": 0,
        "errors": 0,
        "no_remote": 0,
        "originals": 0,
        "missing_paths": [],
        "error_paths": [],
    }


def format_verify_lines(result: dict) -> list[str]:
    mode = result.get("mode") or "sample"
    if mode == "full":
        mode_line = "Mode: full (every original with a B2 path)"
    else:
        mode_line = f"Mode: sample ({result.get('sample_size') or result.get('checked') or 0})"
    lines = [
        mode_line,
        f"Originals in catalog: {result.get('originals', 0):,}",
        f"Scan-only / no remote path: {result.get('no_remote', 0):,}",
        f"Checked: {result.get('checked', 0):,}",
        f"Found on B2: {result.get('found', 0):,}",
        f"Missing from B2: {result.get('missing', 0):,}",
        f"Lookup errors: {result.get('errors', 0):,}",
    ]
    missing_paths = result.get("missing_paths") or []
    if missing_paths:
        lines.append("Missing originals:")
        for path in missing_paths:
            lines.append(f"  {path}")
        extra = int(result.get("missing") or 0) - len(missing_paths)
        if extra > 0:
            lines.append(f"  … and {extra:,} more")
    return lines


def _check_one(b2, rec: dict) -> tuple[dict, bool | None, str | None]:
    try:
        return rec, bool(b2.file_exists(rec["upload_path"])), None
    except Exception as e:
        return rec, None, str(e)


def _collect(records: list[dict], outcomes: list[tuple], counts: dict) -> dict:
    missing_paths = []
    error_paths = []
    found = 0
    missing = 0
    errors = 0
    for rec, exists, err in outcomes:
        if err:
            errors += 1
            if len(error_paths) < MISSING_PREVIEW:
                error_paths.append(f"{_label(rec)}: {err}")
        elif exists:
            found += 1
        else:
            missing += 1
            if len(missing_paths) < MISSING_PREVIEW:
                missing_paths.append(_label(rec))
    return {
        "at": _now(),
        "checked": len(records),
        "found": found,
        "missing": missing,
        "errors": errors,
        "no_remote": int(counts.get("no_remote") or 0),
        "originals": int(counts.get("total") or 0),
        "missing_paths": missing_paths,
        "error_paths": error_paths,
    }


def verify_sample(b2, sample_size: int = DEFAULT_SAMPLE_SIZE, workers: int = 10, progress=None) -> dict:
    counts = count_originals()
    sample_size = max(1, int(sample_size))
    records = load_originals(sample=sample_size)
    result = _empty_result(mode="sample", sample_size=sample_size)
    result.update({"no_remote": counts["no_remote"], "originals": counts["total"]})
    if not records:
        return result

    if progress:
        progress(0, len(records), f"Checking {len(records):,} sampled originals…")

    outcomes = []
    workers = max(1, min(int(workers), 32))
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_check_one, b2, rec) for rec in records]
        for fut in as_completed(futures):
            outcomes.append(fut.result())
            done += 1
            if progress and (done == 1 or done == len(records) or done % 25 == 0):
                progress(done, len(records), f"{done}/{len(records)} checked")

    result.update(_collect(records, outcomes, counts))
    result["mode"] = "sample"
    result["sample_size"] = sample_size
    return result


def verify_full(b2, progress=None) -> dict:
    counts = count_originals()
    records = load_originals()
    result = _empty_result(mode="full")
    result.update({"no_remote": counts["no_remote"], "originals": counts["total"]})
    if not records:
        return result

    by_path = {rec["upload_path"]: rec for rec in records}
    if progress:
        progress(0, len(by_path), f"Listing B2 to compare {len(by_path):,} originals…")

    found_paths: set[str] = set()
    listed = 0
    try:
        for fv, _folder in b2.list_files(""):
            listed += 1
            name = getattr(fv, "file_name", None)
            if name in by_path:
                found_paths.add(name)
            if progress and listed % 2000 == 0:
                progress(
                    len(found_paths),
                    len(by_path),
                    f"Listed {listed:,} B2 objects; matched {len(found_paths):,}",
                )
    except Exception as e:
        result["errors"] = 1
        result["error_paths"] = [f"B2 list failed after {listed:,} objects: {e}"]
        result["checked"] = len(by_path)
        return result

    missing_recs = [rec for path, rec in by_path.items() if path not in found_paths]
    result.update(
        {
            "at": _now(),
            "mode": "full",
            "checked": len(by_path),
            "found": len(found_paths),
            "missing": len(missing_recs),
            "errors": 0,
            "missing_paths": [_label(r) for r in missing_recs[:MISSING_PREVIEW]],
            "error_paths": [],
        }
    )
    if progress:
        progress(len(by_path), len(by_path), f"Listed {listed:,} B2 objects.")
    return result


def run_verify(
    bucket: str | None = None,
    *,
    mode: str = "sample",
    sample_size: int = DEFAULT_SAMPLE_SIZE,
    workers: int = 10,
    progress=None,
    b2=None,
) -> dict:
    """Run a verify pass. `b2` is injectable for tests."""
    mode = "full" if mode == "full" else "sample"
    bucket = (bucket or get_bucket_name() or "").strip()
    if b2 is None:
        if not bucket:
            raise ValueError("No B2 bucket configured.")
        if progress:
            progress(0, 0, f"Connecting to B2 bucket {bucket}…")
        b2 = B2Manager(bucket)

    if mode == "full":
        result = verify_full(b2, progress=progress)
    else:
        result = verify_sample(b2, sample_size=sample_size, workers=workers, progress=progress)
    result["bucket"] = bucket
    set_last_verify({k: v for k, v in result.items() if k != "error_paths"})
    return result


def run_verify_job(job: dict) -> None:
    from core.config import get_workers
    from core.jobs import finish_job, update_progress

    job_id = job["id"]
    payload = job["payload"]
    mode = "full" if payload.get("mode") == "full" else "sample"
    try:
        sample_size = int(payload.get("sample_size") or DEFAULT_SAMPLE_SIZE)
    except (TypeError, ValueError):
        sample_size = DEFAULT_SAMPLE_SIZE
    try:
        workers = int(payload.get("workers") or get_workers())
    except (TypeError, ValueError):
        workers = 10
    bucket = (payload.get("bucket") or get_bucket_name() or "").strip()

    if not bucket:
        finish_job(job_id, "error", "No B2 bucket configured.")
        return

    def progress(done, total, line=None):
        update_progress(job_id, done, total, line)

    try:
        result = run_verify(
            bucket,
            mode=mode,
            sample_size=sample_size,
            workers=workers,
            progress=progress,
        )
    except Exception as e:
        finish_job(job_id, "error", str(e))
        return

    for line in format_verify_lines(result):
        update_progress(job_id, result["checked"], result["checked"] or 1, line)
    finish_job(job_id, "done")


def verify_action(args) -> None:
    """CLI: sample or full catalog-vs-B2 check."""
    from core.config import get_workers

    mode = "full" if getattr(args, "full", False) else "sample"
    sample_size = int(getattr(args, "sample", None) or DEFAULT_SAMPLE_SIZE)
    workers = int(getattr(args, "workers", None) or get_workers())
    bucket = (getattr(args, "bucket", None) or get_bucket_name() or "").strip()
    if not bucket:
        print("Error: set --bucket or configure a bucket in Settings.")
        return

    print(f"Bucket:  {bucket}")
    print(f"Mode:    {mode}" + ("" if mode == "full" else f" ({sample_size})"))
    print(f"Workers: {workers}")
    print()

    counts = count_originals()
    print(
        f"Catalog originals: {counts['total']:,} "
        f"({counts['with_remote']:,} with a B2 path, "
        f"{counts['no_remote']:,} scan-only)"
    )
    if counts["with_remote"]:
        print(f"Unique stored size (all originals): {format_size(_unique_bytes())}")
    print()

    try:
        result = run_verify(bucket, mode=mode, sample_size=sample_size, workers=workers)
    except Exception as e:
        print(f"Error: {e}")
        return

    for line in format_verify_lines(result):
        print(line)
    if result.get("error_paths"):
        print("Lookup errors:")
        for line in result["error_paths"]:
            print(f"  {line}")


def _unique_bytes() -> int:
    from core.db import connect

    conn = connect(readonly=True)
    try:
        row = conn.execute(
            "SELECT COALESCE(SUM(size), 0) FROM files WHERE is_original = 1"
        ).fetchone()
    finally:
        conn.close()
    return int(row[0] or 0)
