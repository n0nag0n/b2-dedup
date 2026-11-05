"""Browser downloads: one file at a time, or a ZIP of a selection.

Everything here streams from B2. Nothing is buffered whole in memory, so the
vault can live on a server and still hand a multi-GB file to a browser.

Single files stream straight through. Bulk downloads come in two shapes:

* *streamed ZIP* — built while it is sent, no staging on the server's disk;
  no resume, and the browser cannot show a total size.
* *prepared ZIP* — built as a background job into ``data/exports``, then
  served as a real file, so it can be resumed and retried.
"""
from __future__ import annotations

import json
import os
import shutil
import threading
import time
import urllib.parse
import zipfile
from datetime import datetime
from pathlib import Path

from core.b2 import B2Manager, sanitize_b2_path
from core.catalog import format_size
from core.config import (
    get_bucket_name,
    get_download_expiry_hours,
    get_download_max_bytes,
    get_download_max_files,
    get_download_stream_threshold,
)
from core.paths import CHUNK_SIZE, EXPORTS_DIR, POINTER_EXTENSION

# Extensions that are already compressed; deflating them only burns CPU.
STORED_EXTENSIONS = frozenset(
    {
        "jpg", "jpeg", "png", "gif", "webp", "heic", "heif", "avif", "bmp",
        "cr2", "cr3", "nef", "arw", "dng", "raf", "orf", "rw2", "srw",
        "mp3", "m4a", "aac", "ogg", "flac", "opus", "wav",
        "mp4", "mov", "m4v", "avi", "mkv", "webm", "wmv", "mpg", "mpeg",
        "zip", "gz", "bz2", "xz", "7z", "rar", "zst", "cab", "iso", "dmg",
        "pdf", "docx", "xlsx", "pptx", "odt", "epub", "jar", "apk",
    }
)
DEFLATE_MAX_BYTES = 32 * 1024 * 1024
EXPORT_TMP_SUFFIX = ".part"
EXPORT_KEEP = 5

_manager_lock = threading.Lock()
_managers: dict[str, B2Manager] = {}


def shared_manager(bucket: str | None = None) -> B2Manager:
    """One authorized B2 client per bucket, reused across requests.

    b2sdk re-authorizes on its own when a token expires, so a long-lived
    client is safe and saves an authorize round trip on every click.
    """
    bucket = (bucket or get_bucket_name() or "").strip()
    if not bucket:
        raise RuntimeError("No B2 bucket configured.")
    with _manager_lock:
        manager = _managers.get(bucket)
        if manager is None:
            manager = B2Manager(bucket)
            _managers[bucket] = manager
        return manager


def resolve_source(b2, rec: dict) -> str:
    """B2 object path that holds this catalog row's bytes.

    Pointer rows (``is_original = 0``) either carry the original's upload path
    directly or point at it from a ``.b2ptr`` file. Using ``upload_path`` when
    it is present skips an extra B2 read.
    """
    remote = sanitize_b2_path(f"{rec['drive_name']}/{rec['file_path']}")
    upload_path = (rec.get("upload_path") or "").strip()
    if upload_path:
        return upload_path
    if rec.get("is_original"):
        return remote
    pointer_content = b2.download_file_content(remote + POINTER_EXTENSION)
    pointer = json.loads(pointer_content.decode("utf-8"))
    return pointer["original_path"]


def safe_filename(name: str) -> str:
    """A single path component that is safe to put in a download header."""
    name = str(name or "").replace("\\", "/").rsplit("/", 1)[-1]
    name = "".join(ch for ch in name if ord(ch) >= 32 and ch != "\x7f")
    name = name.strip().strip(".")
    if not name:
        return "download"
    if len(name) > 150:
        stem, dot, ext = name.rpartition(".")
        if dot and len(ext) <= 12:
            name = stem[: 150 - len(ext) - 1] + "." + ext
        else:
            name = name[:150]
    return name


def content_disposition(filename: str, *, inline: bool = False) -> str:
    """RFC 6266 header with a UTF-8 name for non-ASCII files.

    ``inline`` lets the browser render the body (a preview). The default
    still downloads it as an attachment.
    """
    name = safe_filename(filename)
    fallback = "".join(
        ch if 32 <= ord(ch) < 127 and ch not in '"\\' else "_" for ch in name
    ) or "download"
    kind = "inline" if inline else "attachment"
    return f"{kind}; filename=\"{fallback}\"; filename*=UTF-8''{urllib.parse.quote(name)}"


def zip_arcname(drive_name: str, file_path: str) -> str:
    """``drive/relative/path`` with traversal and control characters removed."""
    parts = []
    for chunk in f"{drive_name}/{file_path}".replace("\\", "/").split("/"):
        chunk = "".join(ch for ch in chunk if ord(ch) >= 32 and ch != "\x7f")
        chunk = chunk.strip()
        if not chunk or chunk in (".", ".."):
            continue
        if chunk.endswith("."):
            chunk = chunk.rstrip(".") or chunk
        parts.append(chunk)
    return "/".join(parts) or "unnamed"


def _unique_arcname(name: str, used: set[str]) -> str:
    """Two paths can sanitize to the same name; keep both."""
    candidate = name
    counter = 2
    while candidate in used:
        stem, dot, ext = name.rpartition("/")[-1].rpartition(".")
        head = name.rpartition("/")[0]
        if dot and stem:
            leaf = f"{stem} ({counter}).{ext}"
        else:
            leaf = f"{name.rpartition('/')[-1]} ({counter})"
        candidate = f"{head}/{leaf}" if head else leaf
        counter += 1
    used.add(candidate)
    return candidate


def _extension(name: str) -> str:
    leaf = name.replace("\\", "/").rsplit("/", 1)[-1]
    return leaf.rsplit(".", 1)[-1].lower() if "." in leaf else ""


def _compress_type(name: str, size: int) -> int:
    if _extension(name) in STORED_EXTENSIONS:
        return zipfile.ZIP_STORED
    if size <= DEFLATE_MAX_BYTES:
        return zipfile.ZIP_DEFLATED
    return zipfile.ZIP_STORED


def _zip_date_time(value) -> tuple[int, int, int, int, int, int]:
    """ZIP timestamps start at 1980; fall back there when a date is unusable."""
    if value:
        try:
            moment = datetime.fromisoformat(str(value))
            if moment.year >= 1980:
                return (
                    moment.year,
                    moment.month,
                    moment.day,
                    moment.hour,
                    moment.minute,
                    min(moment.second, 59),
                )
        except (TypeError, ValueError):
            pass
    return (1980, 1, 1, 0, 0, 0)


def _group_by_content(records: list[dict]) -> list[list[dict]]:
    """Rows that share a hash share bytes, so fetch them once."""
    groups: dict[str, list[dict]] = {}
    for rec in records:
        key = rec.get("hash") or f"id:{rec['id']}"
        groups.setdefault(key, []).append(rec)
    return list(groups.values())


def selection_error(count: int, total_bytes: int) -> str | None:
    """Human-readable reason a selection is too big, or None when it is fine."""
    max_files = get_download_max_files()
    max_bytes = get_download_max_bytes()
    if count > max_files:
        return (
            f"{count:,} files is over the {max_files:,}-file limit for one download. "
            "Remove some files, or raise the limit in Settings."
        )
    if total_bytes > max_bytes:
        return (
            f"{format_size(total_bytes)} is over the {format_size(max_bytes)} "
            "limit for one download. Remove some files, or raise the limit in Settings."
        )
    return None


def forced_to_stream(total_bytes: int) -> bool:
    return total_bytes > get_download_stream_threshold()


# --------------------------------------------------------------------------
# Streaming one B2 object


def stream_downloaded_file(downloaded, chunk_size: int = CHUNK_SIZE):
    """Yield a b2sdk ``DownloadedFile`` in chunks without buffering it.

    b2sdk writes to a file object, so a pipe plus a worker thread turns that
    into an iterator. The pipe also gives natural backpressure: the download
    thread blocks when the browser falls behind.
    """
    read_fd, write_fd = os.pipe()
    errors: list[BaseException] = []

    def _pump() -> None:
        try:
            with os.fdopen(write_fd, "wb") as sink:
                downloaded.save(sink, allow_seeking=False)
        except BaseException as exc:  # noqa: BLE001 - re-raised below
            errors.append(exc)
            try:
                os.close(write_fd)
            except OSError:
                pass
        finally:
            response = getattr(downloaded, "response", None)
            if response is not None:
                try:
                    response.close()
                except Exception:  # noqa: BLE001 - best effort
                    pass

    thread = threading.Thread(target=_pump, name="b2-download", daemon=True)
    thread.start()
    try:
        with os.fdopen(read_fd, "rb") as source:
            while True:
                chunk = source.read(chunk_size)
                if not chunk:
                    break
                yield chunk
    finally:
        thread.join(timeout=10)
    if errors:
        raise errors[0]


def open_remote(b2, remote_path: str, byte_range: tuple[int, int] | None = None):
    """Open a B2 object for streaming. Raises if it is missing or forbidden.

    ``byte_range`` is an inclusive ``(start, end)`` pair, used so a video or
    PDF preview can seek without pulling the whole object first.
    """
    if byte_range is None:
        return b2.bucket.download_file_by_name(remote_path)
    return b2.bucket.download_file_by_name(remote_path, range_=byte_range)


# --------------------------------------------------------------------------
# ZIP building


def _progress(progress, done: int, total: int, line: str | None = None) -> None:
    if progress:
        progress(done, total, line)


def build_zip(records: list[dict], b2, dest_path: Path, progress=None) -> list[str]:
    """Write the selection to ``dest_path``, fetching each unique hash once.

    Returns a list of human-readable problems; files that failed are left out
    of the archive so the rest of the selection still arrives.
    """
    dest_path = Path(dest_path)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = EXPORTS_DIR / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    groups = _group_by_content(records)
    total = len(records)
    done = 0
    used: set[str] = set()
    problems: list[str] = []
    job_tag = dest_path.stem

    with zipfile.ZipFile(dest_path, "w", allowZip64=True) as archive:
        for group in groups:
            source_rec = next(
                (r for r in group if (r.get("upload_path") or "").strip()),
                group[0],
            )
            tmp_path = tmp_dir / f"{job_tag}-{source_rec['id']}{EXPORT_TMP_SUFFIX}"
            try:
                source = resolve_source(b2, source_rec)
                downloaded = open_remote(b2, source)
                downloaded.save_to(tmp_path)
            except Exception as exc:  # noqa: BLE001 - one bad file should not stop the rest
                label = f"{group[0]['drive_name']}/{group[0]['file_path']}"
                problems.append(f"{label}: {exc}")
                done += len(group)
                _progress(progress, done, total, f"skipped {label}: {exc}")
                try:
                    tmp_path.unlink()
                except OSError:
                    pass
                continue

            try:
                for rec in group:
                    arcname = _unique_arcname(
                        zip_arcname(rec["drive_name"], rec["file_path"]), used
                    )
                    archive.write(
                        tmp_path,
                        arcname,
                        compress_type=_compress_type(arcname, int(rec.get("size") or 0)),
                    )
                    done += 1
            finally:
                try:
                    tmp_path.unlink()
                except OSError:
                    pass
            label = f"{group[0]['drive_name']}/{group[0]['file_path']}"
            _progress(progress, done, total, f"{done}/{total} {label}")

        if problems:
            archive.writestr(
                _unique_arcname("_download-errors.txt", used),
                "\n".join(problems),
            )

    return problems


def _write_stream_archive(sink, records: list[dict], b2) -> None:
    """Build a ZIP into a non-seekable sink, entry by entry."""
    used: set[str] = set()
    problems: list[str] = []
    archive = zipfile.ZipFile(sink, "w", allowZip64=True)
    try:
        for rec in records:
            label = f"{rec['drive_name']}/{rec['file_path']}"
            try:
                source = resolve_source(b2, rec)
                downloaded = open_remote(b2, source)
            except Exception as exc:  # noqa: BLE001 - skip one bad file
                problems.append(f"{label}: {exc}")
                continue
            arcname = _unique_arcname(
                zip_arcname(rec["drive_name"], rec["file_path"]), used
            )
            info = zipfile.ZipInfo(arcname, date_time=_zip_date_time(rec.get("file_mtime")))
            info.compress_type = _compress_type(arcname, int(rec.get("size") or 0))
            info.external_attr = 0o644 << 16
            with archive.open(info, "w", force_zip64=True) as entry:
                for chunk in stream_downloaded_file(downloaded):
                    entry.write(chunk)
        if problems:
            manifest = _unique_arcname("_download-errors.txt", used)
            with archive.open(manifest, "w") as entry:
                entry.write("\n".join(problems).encode("utf-8"))
        archive.close()
    except BaseException:
        # Drop the central directory so the client sees a broken archive
        # instead of one that looks complete but hides a half-downloaded file.
        archive.fp = None
        raise


def stream_zip(records: list[dict], b2):
    """Yield ZIP bytes for a selection without staging them on disk.

    A writer thread feeds a pipe so the archive is produced exactly as fast as
    the client consumes it. Duplicate content is downloaded again here, because
    there is no temp file to reuse.
    """
    read_fd, write_fd = os.pipe()
    errors: list[BaseException] = []

    def _pump() -> None:
        try:
            with os.fdopen(write_fd, "wb") as sink:
                _write_stream_archive(sink, records, b2)
        except BaseException as exc:  # noqa: BLE001 - re-raised to the caller
            errors.append(exc)
            try:
                os.close(write_fd)
            except OSError:
                pass

    thread = threading.Thread(target=_pump, name="b2-zip-stream", daemon=True)
    thread.start()
    try:
        with os.fdopen(read_fd, "rb") as source:
            while True:
                chunk = source.read(CHUNK_SIZE)
                if not chunk:
                    break
                yield chunk
    finally:
        thread.join(timeout=30)
    if errors:
        raise errors[0]


# --------------------------------------------------------------------------
# Prepared exports


def export_path(job_id: int) -> Path:
    return EXPORTS_DIR / f"{job_id}.zip"


def export_name(job_id: int, created_at: str | None = None) -> str:
    stamp = ""
    if created_at:
        try:
            stamp = datetime.fromisoformat(created_at).strftime("-%Y%m%d")
        except ValueError:
            stamp = ""
    return f"storageinsights{stamp}-{job_id}.zip"


def _mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except OSError:
        return 0.0


def cleanup_exports() -> None:
    """Drop expired prepared archives and trim the rest to a handful."""
    if not EXPORTS_DIR.is_dir():
        return
    expiry = get_download_expiry_hours() * 3600
    now = time.time()
    archives = sorted(
        (p for p in EXPORTS_DIR.glob("*.zip") if p.is_file()),
        key=_mtime,
        reverse=True,
    )
    for index, path in enumerate(archives):
        try:
            if index >= EXPORT_KEEP or (now - _mtime(path)) > expiry:
                path.unlink()
        except OSError:
            pass

    tmp_dir = EXPORTS_DIR / "tmp"
    if tmp_dir.is_dir():
        for leftover in tmp_dir.glob(f"*{EXPORT_TMP_SUFFIX}"):
            try:
                if (now - _mtime(leftover)) > 3600:
                    leftover.unlink()
            except OSError:
                pass


def estimated_export_need(total_bytes: int) -> int:
    """Bytes of disk a prepared archive is likely to take."""
    return int(total_bytes * 1.1) + 128 * 1024 * 1024


def free_disk_bytes() -> int:
    try:
        EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
        return shutil.disk_usage(EXPORTS_DIR).free
    except OSError:
        return 0


def run_zip_export_job(job: dict) -> None:
    """Background job: build ``data/exports/<job id>.zip`` for a selection."""
    from core.catalog import get_files_by_ids
    from core.jobs import finish_job, update_progress

    job_id = int(job["id"])
    payload = job["payload"]
    ids = [int(i) for i in payload.get("ids") or []]
    bucket = (payload.get("bucket") or get_bucket_name() or "").strip()
    dest = export_path(job_id)

    if not ids:
        finish_job(job_id, "error", "Nothing to download.")
        return
    if not bucket:
        finish_job(job_id, "error", "No B2 bucket configured.")
        return

    try:
        cleanup_exports()
    except Exception:  # noqa: BLE001 - cleanup must never block an export
        pass

    records = get_files_by_ids(ids)
    if not records:
        finish_job(job_id, "error", "Those files are no longer in the catalog.")
        return

    total_bytes = sum(int(r.get("size") or 0) for r in records)
    problem = selection_error(len(records), total_bytes)
    if problem:
        finish_job(job_id, "error", problem)
        return

    need = estimated_export_need(total_bytes)
    free = free_disk_bytes()
    if free and free < need:
        finish_job(
            job_id,
            "error",
            f"Not enough free disk space for a {format_size(total_bytes)} archive: "
            f"{format_size(free)} free, about {format_size(need)} needed.",
        )
        return

    try:
        b2 = shared_manager(bucket)
    except Exception as exc:  # noqa: BLE001
        finish_job(job_id, "error", f"Could not connect to B2: {exc}")
        return

    def progress(done, total, line=None):
        update_progress(job_id, done, total, line)

    update_progress(
        job_id, 0, len(records), f"Building a ZIP of {len(records):,} file(s)…"
    )
    try:
        problems = build_zip(records, b2, dest, progress=progress)
    except Exception as exc:  # noqa: BLE001
        try:
            dest.unlink()
        except OSError:
            pass
        finish_job(job_id, "error", f"Building the ZIP failed: {exc}")
        return

    if not dest.is_file():
        finish_job(job_id, "error", "The ZIP was not written.")
        return

    update_progress(
        job_id,
        len(records),
        len(records),
        f"Done. {len(records) - len(problems):,} file(s), "
        f"{format_size(dest.stat().st_size)} ZIP.",
    )
    if problems:
        for line in problems[:20]:
            update_progress(job_id, len(records), len(records), f"  skipped {line}")
    finish_job(job_id, "done")


def export_status(job: dict | None) -> dict | None:
    """Template-friendly info about a prepared archive, if it still exists."""
    if not job or job.get("type") != "zip_export":
        return None
    path = export_path(int(job["id"]))
    info = {
        "name": export_name(int(job["id"]), job.get("created_at")),
        "url": f"/exports/{int(job['id'])}.zip",
        "exists": path.is_file(),
        "size_fmt": "",
    }
    if info["exists"]:
        try:
            info["size_fmt"] = format_size(path.stat().st_size)
        except OSError:
            info["exists"] = False
    return info
