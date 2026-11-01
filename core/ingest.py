"""Upload / scan-only ingest.

CLI `upload`/`ingest` and the web ingest job both call `run_ingest`.
Skip hashing when the catalog already has the same path with matching
size and mtime. Never hash or upload ingest-junk (node_modules, AppleDouble, …).
"""
from __future__ import annotations

import json
import os
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timedelta, timezone
from pathlib import Path

from tqdm import tqdm

from core.b2 import B2Manager, sanitize_b2_path
from core.catalog import format_size
from core.db import get_thread_connection, init_db
from core.hash import sha256_file
from core.junk import INGEST_SKIP_DIR_NAMES, classify_path, is_ingest_skip
from core.paths import CACHE_PATH, FILE_COUNT_CACHE_DAYS, POINTER_EXTENSION
from core.pointers import create_pointer_content
from core.time import choose_taken_at, exif_datetime_from_path
from file_utils import get_file_metadata

EMPTY_STATS = {
    "scanned": 0,
    "duplicate_recorded": 0,
    "already_tracked": 0,
    "updated": 0,
    "uploaded": 0,
    "would_upload": 0,
    "exists": 0,
    "pointer_created": 0,
    "pointer_exists": 0,
    "would_create_pointer": 0,
    "race_duplicate": 0,
    "skipped_junk": 0,
    "error": 0,
}

STATUS_ICONS = {
    "uploaded": "↑",
    "scanned": "✓",
    "duplicate_recorded": "≡",
    "already_tracked": "○",
    "updated": "~",
    "exists": "○",
    "would_upload": "?",
    "pointer_created": "→",
    "pointer_exists": "○",
    "would_create_pointer": "?",
    "race_duplicate": "≡",
    "skipped_junk": "×",
    "error": "✗",
}


def load_file_count_cache() -> dict:
    """Load the file count cache from disk."""
    if CACHE_PATH.exists():
        try:
            with open(CACHE_PATH, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def save_file_count_cache(cache: dict):
    """Save the file count cache to disk."""
    try:
        with open(CACHE_PATH, "w") as f:
            json.dump(cache, f, indent=2)
    except IOError as e:
        print(f"⚠ Could not save file count cache: {e}")


def get_cached_file_count(source_path: Path, drive_name: str, refresh: bool = False) -> tuple:
    """Get file count from cache if valid, otherwise return (None, False)."""
    if refresh:
        return None, False

    cache = load_file_count_cache()
    cache_key = f"v2:{drive_name}:{source_path}"

    if cache_key in cache:
        entry = cache[cache_key]
        cached_time = datetime.fromisoformat(entry["timestamp"])
        if datetime.now() - cached_time < timedelta(days=FILE_COUNT_CACHE_DAYS):
            return entry["count"], True

    return None, False


def save_file_count_to_cache(source_path: Path, drive_name: str, count: int):
    """Save the file count to cache."""
    cache = load_file_count_cache()
    cache_key = f"v2:{drive_name}:{source_path}"
    cache[cache_key] = {
        "count": count,
        "timestamp": datetime.now().isoformat(),
        "path": str(source_path),
    }
    save_file_count_cache(cache)


def prune_ingest_dirs(dirs: list[str]) -> None:
    """Drop ingest-skip directories so os.walk does not descend into them."""
    dirs[:] = [
        name for name in dirs
        if name not in INGEST_SKIP_DIR_NAMES and not name.startswith(".Spotlight-")
    ]


def iter_source_files(source_path: Path, drive_root: Path):
    """Yield (filepath, rel_path) for files that should be hashed/uploaded.

    Skips symlinks, non-files, and ingest-junk. Prunes junk directories.
    `.git` / `.hg` are kept so a restore is complete.
    """
    def _onerror(_err):
        return

    for root, dirs, files in os.walk(source_path, onerror=_onerror):
        prune_ingest_dirs(dirs)
        root_path = Path(root)
        for filename in files:
            filepath = root_path / filename
            if filepath.is_symlink() or not filepath.is_file():
                continue
            try:
                rel_path = filepath.relative_to(drive_root)
            except ValueError:
                continue
            if is_ingest_skip(rel_path):
                continue
            yield filepath, rel_path


def mtimes_match(disk_iso: str | None, db_value) -> bool:
    """Compare mtimes at second precision (catalog values vary in subseconds)."""
    if not disk_iso or db_value is None:
        return False
    return str(disk_iso)[:19] == str(db_value)[:19]


def signatures_match(disk_size: int, disk_mtime: str | None, db_size, db_mtime) -> bool:
    """True when catalog size + mtime match the file on disk."""
    try:
        tracked_size = int(db_size)
    except (TypeError, ValueError):
        return False
    return tracked_size == int(disk_size) and mtimes_match(disk_mtime, db_mtime)


def disk_mtime_iso(stat_result) -> str:
    return datetime.fromtimestamp(stat_result.st_mtime, tz=timezone.utc).isoformat()


def load_tracked_signatures(drive_name: str) -> dict[str, tuple]:
    """Map file_path → (id, hash, size, file_mtime, is_original, upload_path)."""
    from core.db import connect

    conn = connect(readonly=True)
    try:
        rows = conn.execute(
            "SELECT id, hash, file_path, size, file_mtime, is_original, upload_path "
            "FROM files WHERE drive_name = ?",
            (drive_name,),
        ).fetchall()
    finally:
        conn.close()
    return {
        row["file_path"]: (
            int(row["id"]),
            row["hash"],
            int(row["size"] or 0),
            row["file_mtime"],
            int(row["is_original"] or 0),
            row["upload_path"],
        )
        for row in rows
    }


def preview_ingest(
    source_path: Path,
    drive_name: str,
    drive_root: Path | None = None,
    tracked: dict[str, tuple] | None = None,
) -> dict:
    """Walk a tree without hashing. Used by the ingest wizard.

    `new_bytes` is the size of files that are not incrementally skipped.
    True unique-after-dedup bytes are only known after hashing.
    """
    source_path = Path(source_path)
    drive_root = Path(drive_root) if drive_root else source_path
    if tracked is None:
        tracked = load_tracked_signatures(drive_name) if drive_name else {}

    total = 0
    junk = 0
    already = 0
    new_count = 0
    new_bytes = 0

    def _onerror(_err):
        return

    for root, dirs, files in os.walk(source_path, onerror=_onerror):
        # Count files inside junk dirs, then prune so we do not walk them.
        junk_dirs = [
            name for name in dirs
            if name in INGEST_SKIP_DIR_NAMES or name.startswith(".Spotlight-")
        ]
        for name in junk_dirs:
            junk_root = Path(root) / name
            try:
                for _jroot, _jdirs, jfiles in os.walk(junk_root):
                    junk += len(jfiles)
                    total += len(jfiles)
            except OSError:
                pass
        prune_ingest_dirs(dirs)

        root_path = Path(root)
        for filename in files:
            filepath = root_path / filename
            total += 1
            if filepath.is_symlink() or not filepath.is_file():
                continue
            try:
                rel_path = filepath.relative_to(drive_root)
            except ValueError:
                continue
            if is_ingest_skip(rel_path):
                junk += 1
                continue
            try:
                stat = filepath.stat()
            except OSError:
                new_count += 1
                continue
            key = rel_path.as_posix()
            existing = tracked.get(key)
            mtime = disk_mtime_iso(stat)
            if existing and signatures_match(stat.st_size, mtime, existing[2], existing[3]):
                already += 1
                continue
            new_count += 1
            new_bytes += int(stat.st_size)

    return {
        "total_files": total,
        "junk_skipped": junk,
        "already_tracked": already,
        "new_or_changed": new_count,
        "new_bytes": new_bytes,
        "new_bytes_fmt": format_size(new_bytes),
        "ingestable": total - junk,
    }


def count_files_with_progress(source_path: Path, drive_root: Path | None = None) -> int:
    """Count ingestable files (junk excluded) with progress per top-level directory."""
    drive_root = Path(drive_root) if drive_root else source_path
    total_count = 0

    try:
        top_level_items = list(source_path.iterdir())
    except OSError as e:
        print(f"  ⚠ Could not read {source_path}: {e}")
        return 0

    top_level_dirs = [d for d in top_level_items if d.is_dir() and not d.is_symlink()]
    top_level_files = [f for f in top_level_items if f.is_file() and not f.is_symlink()]

    for filepath in top_level_files:
        try:
            rel = filepath.relative_to(drive_root)
        except ValueError:
            continue
        if is_ingest_skip(rel):
            continue
        total_count += 1
    if top_level_files:
        print(f"  [root files]: {total_count:,} ingestable")

    visible_dirs = [
        d for d in sorted(top_level_dirs)
        if d.name not in INGEST_SKIP_DIR_NAMES and not d.name.startswith(".Spotlight-")
    ]
    skipped_dirs = len(top_level_dirs) - len(visible_dirs)
    if skipped_dirs:
        print(f"  skipping {skipped_dirs} junk director{'y' if skipped_dirs == 1 else 'ies'}")

    for i, subdir in enumerate(visible_dirs, 1):
        dir_count = 0
        try:
            for _filepath, _rel in iter_source_files(subdir, drive_root):
                dir_count += 1
        except PermissionError:
            print(f"  [{i}/{len(visible_dirs)}] {subdir.name}/: ⚠ Permission denied")
            continue

        total_count += dir_count
        print(f"  [{i}/{len(visible_dirs)}] {subdir.name}/: {dir_count:,} files (total: {total_count:,})")

    return total_count


def _taken_fields(file_path, meta, local_path=None):
    exif_dt = None
    path = local_path or meta.get("local_path")
    if path:
        try:
            exif_dt = exif_datetime_from_path(path)
        except Exception:
            exif_dt = None
    return choose_taken_at(file_path, meta.get("mtime"), exif_dt=exif_dt)


def insert_file_record(c, file_hash, actual_size, drive_name, file_path, upload_path, is_original, meta):
    """INSERT OR IGNORE a files row. Returns cursor.rowcount (0 if ignored)."""
    flags = classify_path(file_path)
    taken_at, taken_source = _taken_fields(file_path, meta)
    c.execute(
        "INSERT OR IGNORE INTO files "
        "(hash, size, drive_name, file_path, upload_path, is_original, created_at, "
        " file_mtime, file_ctime, file_atime, mime_type, file_type, is_junk, searchable, "
        " best_taken_at, taken_at_source) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            file_hash, actual_size, drive_name, file_path, upload_path, is_original,
            datetime.now(timezone.utc).isoformat(),
            meta["mtime"], meta["ctime"], meta["atime"], meta["mime_type"], meta["file_type"],
            1 if flags["is_junk"] else 0,
            1 if flags["searchable"] else 0,
            taken_at,
            taken_source,
        ),
    )
    return c.rowcount


def update_file_record(c, file_id, file_hash, actual_size, upload_path, is_original, meta, file_path):
    """Refresh an existing occurrence after the file on disk changed."""
    flags = classify_path(file_path)
    taken_at, taken_source = _taken_fields(file_path, meta)
    c.execute(
        "UPDATE files SET hash = ?, size = ?, upload_path = ?, is_original = ?, "
        "file_mtime = ?, file_ctime = ?, file_atime = ?, mime_type = ?, file_type = ?, "
        "is_junk = ?, searchable = ?, best_taken_at = ?, taken_at_source = ? WHERE id = ?",
        (
            file_hash, actual_size, upload_path, is_original,
            meta["mtime"], meta["ctime"], meta["atime"], meta["mime_type"], meta["file_type"],
            1 if flags["is_junk"] else 0,
            1 if flags["searchable"] else 0,
            taken_at,
            taken_source,
            file_id,
        ),
    )
    return c.rowcount


# Legacy name used by comments / older call sites
_insert_file_record = insert_file_record


def _save_record(c, existing, file_hash, actual_size, drive_name, file_path, upload_path, is_original, meta):
    if existing:
        return update_file_record(
            c, existing[0], file_hash, actual_size, upload_path, is_original, meta, file_path,
        )
    return insert_file_record(
        c, file_hash, actual_size, drive_name, file_path, upload_path, is_original, meta,
    )


def process_file(args_tuple):
    if len(args_tuple) == 7:
        filepath, rel_path, drive_name, scan_only, dry_run, b2, existing = args_tuple
    else:
        filepath, rel_path, drive_name, scan_only, dry_run, b2 = args_tuple
        existing = None

    conn = get_thread_connection()
    c = conn.cursor()

    remote_name = sanitize_b2_path(f"{drive_name}/{rel_path.as_posix()}")
    file_path = rel_path.as_posix()

    try:
        if is_ingest_skip(file_path):
            return "skipped_junk", filepath

        try:
            stat = filepath.stat()
        except OSError as e:
            return "error", filepath, str(e)

        disk_mtime = disk_mtime_iso(stat)
        if existing is None:
            c.execute(
                "SELECT id, hash, size, file_mtime, is_original, upload_path "
                "FROM files WHERE drive_name = ? AND file_path = ?",
                (drive_name, file_path),
            )
            existing = c.fetchone()
        if existing and signatures_match(stat.st_size, disk_mtime, existing[2], existing[3]):
            return "already_tracked", filepath

        file_hash, actual_size = sha256_file(filepath)

        meta = get_file_metadata(filepath)
        if "error" in meta:
            meta = {
                "mtime": disk_mtime, "ctime": None, "atime": None,
                "mime_type": None, "file_type": "Unknown",
            }
        meta["local_path"] = filepath

        existing_id = existing[0] if existing else -1
        same_hash = bool(existing and existing[1] == file_hash)

        c.execute(
            "SELECT upload_path FROM files WHERE hash = ? AND is_original = 1 AND id != ? LIMIT 1",
            (file_hash, existing_id),
        )
        other_original = c.fetchone()

        if existing and same_hash:
            if dry_run:
                return "already_tracked", filepath
            _save_record(
                c, existing, file_hash, actual_size, drive_name, file_path,
                existing[5] if existing[4] else (other_original[0] if other_original else existing[5]),
                1 if existing[4] else (0 if other_original else 1),
                meta,
            )
            conn.commit()
            return "updated", filepath

        if other_original:
            original_upload_path = other_original[0]

            if scan_only:
                if dry_run:
                    return "would_create_pointer", filepath
                _save_record(
                    c, existing, file_hash, actual_size, drive_name, file_path, None, 0, meta,
                )
                conn.commit()
                return "duplicate_recorded", filepath

            pointer_content = create_pointer_content(file_hash, original_upload_path)
            pointer_remote_path = remote_name + POINTER_EXTENSION

            if b2.file_exists(pointer_remote_path):
                if not dry_run:
                    _save_record(
                        c, existing, file_hash, actual_size, drive_name, file_path, None, 0, meta,
                    )
                    conn.commit()
                return "pointer_exists", filepath

            if dry_run:
                return "would_create_pointer", filepath

            b2.upload_bytes(pointer_content, pointer_remote_path)
            _save_record(
                c, existing, file_hash, actual_size, drive_name, file_path, None, 0, meta,
            )
            conn.commit()
            return "pointer_created", filepath

        if scan_only:
            if dry_run:
                return "would_upload", filepath
            _save_record(
                c, existing, file_hash, actual_size, drive_name, file_path, None, 1, meta,
            )
            conn.commit()
            return "scanned", filepath

        if b2.file_exists(remote_name):
            if not dry_run:
                _save_record(
                    c, existing, file_hash, actual_size, drive_name, file_path, remote_name, 1, meta,
                )
                conn.commit()
            return "exists", filepath

        if dry_run:
            return "would_upload", filepath

        b2.upload_file(filepath, remote_name)
        rowcount = _save_record(
            c, existing, file_hash, actual_size, drive_name, file_path, remote_name, 1, meta,
        )
        conn.commit()
        if rowcount == 0 and not existing:
            return "race_duplicate", filepath
        return "uploaded", filepath

    except Exception as e:
        return "error", filepath, str(e)


def format_summary_lines(stats: dict, *, scan_only: bool, dry_run: bool) -> list[str]:
    lines = ["Summary:"]
    if scan_only:
        lines.append(f"  New files scanned (original):  {stats['scanned']:,}")
        lines.append(f"  Duplicates recorded:           {stats['duplicate_recorded']:,}")
        lines.append(f"  Already in database:           {stats['already_tracked']:,}")
        if stats.get("updated"):
            lines.append(f"  Metadata updated:              {stats['updated']:,}")
    else:
        lines.append(f"  Uploaded (original):           {stats['uploaded']:,}")
        lines.append(f"  Pointer files created:         {stats['pointer_created']:,}")
        lines.append(f"  Already exists in bucket:      {stats['exists']:,}")
        lines.append(f"  Pointer already exists:        {stats['pointer_exists']:,}")
        lines.append(f"  Already in database:           {stats['already_tracked']:,}")
        if stats.get("updated"):
            lines.append(f"  Metadata updated:              {stats['updated']:,}")
        if dry_run:
            lines.append(f"  Would upload (dry-run):        {stats['would_upload']:,}")
            lines.append(f"  Would create pointer:          {stats['would_create_pointer']:,}")
    if stats.get("skipped_junk"):
        lines.append(f"  Junk skipped:                  {stats['skipped_junk']:,}")
    if stats["error"]:
        lines.append(f"  Errors:                        {stats['error']:,}")
    return lines


def run_ingest(
    source: Path,
    drive_name: str,
    bucket: str | None,
    *,
    drive_root: Path | None = None,
    scan_only: bool = False,
    dry_run: bool = False,
    workers: int = 10,
    refresh_count: bool = False,
    verbose: bool = False,
    progress=None,
    show_progress_bar: bool = True,
) -> dict:
    """Run an ingest. `progress(done, total, line=None)` is optional (jobs)."""
    source_path = Path(source).resolve()
    if not source_path.is_dir():
        raise ValueError(f"{source_path} is not a valid directory")

    if drive_root:
        drive_root = Path(drive_root).resolve()
        try:
            source_path.relative_to(drive_root)
        except ValueError as exc:
            raise ValueError(f"Source {source_path} is not inside drive root {drive_root}") from exc
    else:
        drive_root = source_path

    if scan_only and dry_run:
        dry_run = False

    init_db()

    b2 = None if scan_only else B2Manager(bucket)

    cached_count, was_cached = get_cached_file_count(source_path, drive_name, refresh_count)
    if was_cached:
        total_files = cached_count
        if progress:
            progress(0, total_files, f"Using cached file count: {total_files:,}")
        elif show_progress_bar:
            print(f"Using cached file count: {total_files:,} files")
            print("  (Use --refresh-count to force re-count)\n")
    else:
        if progress:
            progress(0, 0, "Counting files (junk excluded)…")
            total_files = sum(1 for _ in iter_source_files(source_path, drive_root))
        else:
            print("Counting files...")
            total_files = count_files_with_progress(source_path, drive_root)
            print(f"\nTotal: {total_files:,} ingestable files (cached for {FILE_COUNT_CACHE_DAYS} days)\n")
        save_file_count_to_cache(source_path, drive_name, total_files)
        if progress:
            progress(0, total_files, f"{total_files:,} ingestable files")

    tracked = load_tracked_signatures(drive_name)

    stats = dict(EMPTY_STATS)
    errors = []

    def file_generator():
        for filepath, rel_path in iter_source_files(source_path, drive_root):
            existing = tracked.get(rel_path.as_posix())
            yield (filepath, rel_path, drive_name, scan_only, dry_run, b2, existing)

    def _handle_result(result):
        err_msg = ""
        if len(result) == 3:
            status, filepath, err = result
            errors.append((filepath, err))
            err_msg = f": {err}"
        else:
            status, filepath = result
        stats[status] = stats.get(status, 0) + 1
        return status, filepath, err_msg

    done = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        max_in_flight = workers * 2
        pending = set()
        file_iter = file_generator()

        pbar = None
        if show_progress_bar and progress is None:
            pbar = tqdm(total=total_files, desc="Processing", unit="file")

        try:
            for task in file_iter:
                pending.add(executor.submit(process_file, task))
                if len(pending) >= max_in_flight:
                    break

            while pending:
                finished, pending = wait(pending, return_when=FIRST_COMPLETED)

                for future in finished:
                    result = future.result()
                    status, filepath, err_msg = _handle_result(result)
                    done += 1
                    line = None
                    if verbose or status == "error":
                        line = f"  {STATUS_ICONS.get(status, ' ')} [{status}] {filepath}{err_msg}"
                    elif done == 1 or done == total_files or done % 50 == 0:
                        line = f"{done}/{total_files} {filepath}"
                    if pbar is not None:
                        if verbose and line:
                            pbar.write(line)
                        pbar.update(1)
                    if progress:
                        progress(done, total_files or done, line)

                    try:
                        new_task = next(file_iter)
                        pending.add(executor.submit(process_file, new_task))
                    except StopIteration:
                        pass
        finally:
            if pbar is not None:
                pbar.close()

    return {"stats": stats, "errors": errors, "total_files": total_files}


def run_ingest_job(job: dict) -> None:
    """Background ingest used by the job worker."""
    from core.config import get_bucket_name
    from core.jobs import finish_job, update_progress

    job_id = job["id"]
    payload = job["payload"]
    source = Path(payload.get("source") or "")
    drive_name = (payload.get("drive_name") or "").strip()
    drive_root = payload.get("drive_root") or None
    scan_only = bool(payload.get("scan_only"))
    dry_run = bool(payload.get("dry_run"))
    workers = int(payload.get("workers") or 10)
    bucket = (payload.get("bucket") or get_bucket_name() or "").strip()

    if not drive_name:
        finish_job(job_id, "error", "Drive name is required.")
        return
    if not source.is_dir():
        finish_job(job_id, "error", f"Source is not a directory: {source}")
        return
    if not scan_only and not bucket:
        finish_job(job_id, "error", "No B2 bucket configured.")
        return

    mode = "scan-only" if scan_only else ("dry-run" if dry_run else "upload")
    update_progress(
        job_id, 0, 0,
        f"Ingest {mode}: {source} as {drive_name}/  workers={workers}",
    )

    def progress(done, total, line=None):
        update_progress(job_id, done, total, line)

    try:
        result = run_ingest(
            source,
            drive_name,
            bucket or None,
            drive_root=Path(drive_root) if drive_root else None,
            scan_only=scan_only,
            dry_run=dry_run,
            workers=max(1, workers),
            progress=progress,
            show_progress_bar=False,
        )
    except Exception as e:
        finish_job(job_id, "error", str(e))
        return

    stats = result["stats"]
    errors = result["errors"]
    for line in format_summary_lines(stats, scan_only=scan_only, dry_run=dry_run):
        update_progress(job_id, result["total_files"], result["total_files"], line)
    if errors:
        update_progress(job_id, result["total_files"], result["total_files"], "First 5 errors:")
        for fp, err in errors[:5]:
            update_progress(job_id, result["total_files"], result["total_files"], f"  {fp}: {err}")

    finish_job(job_id, "done")


def upload_action(args):
    """Handle the upload / ingest subcommand."""
    if args.scan_only and args.dry_run:
        print("Note: --dry-run is ignored in --scan-only mode")

    source_path = Path(args.source).resolve()
    if not source_path.is_dir():
        print(f"Error: {source_path} is not a valid directory")
        return

    if args.drive_root:
        drive_root = Path(args.drive_root).resolve()
        try:
            source_path.relative_to(drive_root)
        except ValueError:
            print(f"Error: Source {source_path} is not inside drive root {drive_root}")
            return
    else:
        drive_root = source_path

    mode = "SCAN-ONLY" if args.scan_only else ("DRY-RUN" if args.dry_run else "FULL (upload)")
    print(f"Source:  {source_path}")
    if args.drive_root:
        print(f"Root:    {drive_root}")
    print(f"Bucket:  {args.bucket}")
    print(f"Drive:   {args.drive_name}/")
    print(f"Mode:    {mode}")
    print(f"Workers: {args.workers}\n")

    try:
        result = run_ingest(
            source_path,
            args.drive_name,
            args.bucket,
            drive_root=drive_root,
            scan_only=args.scan_only,
            dry_run=args.dry_run,
            workers=args.workers,
            refresh_count=args.refresh_count,
            verbose=args.verbose,
            show_progress_bar=True,
        )
    except Exception as e:
        print(f"Error: {e}")
        return

    print()
    for line in format_summary_lines(result["stats"], scan_only=args.scan_only, dry_run=args.dry_run):
        print(line)

    if result["errors"]:
        print("\nFirst 5 errors:")
        for fp, err in result["errors"][:5]:
            print(f"  {fp}: {err}")
