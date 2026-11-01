"""Image thumbnails for the library. One JPEG per content hash.

Stored under ``data/thumbs/{hash[:2]}/{hash}.jpg``. Status lives on the
original file row in ``file_features`` (``thumb_v1``). CR2/NEF and other
RAW files use the embedded JPEG preview so we do not demosaic.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

from core.db import connect
from core.paths import THUMBS_DIR
from core.time import (
    SOURCE_EXIF,
    choose_taken_at,
    exif_datetime_from_bytes,
    extract_jpeg_preview,
    should_replace,
    to_iso,
)

FEATURE = "thumb_v1"
THUMB_MAX = 400
THUMB_QUALITY = 82
RAW_EXTS = frozenset({"cr2", "cr3", "nef", "arw", "dng", "raf", "orf", "rw2", "srw"})
DIRECT_EXTS = frozenset({"jpg", "jpeg", "png", "gif", "webp", "bmp", "tif", "tiff"})
IMAGE_EXTS = RAW_EXTS | DIRECT_EXTS | {"heic", "heif"}
HEADER_BYTES = 2 * 1024 * 1024
RAW_PROBE_BYTES = 8 * 1024 * 1024
MAX_DOWNLOAD = 20 * 1024 * 1024
HASH_RE = re.compile(r"^[0-9a-f]{64}$")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def file_ext(file_path: str) -> str:
    name = str(file_path).replace("\\", "/").rsplit("/", 1)[-1]
    if "." not in name:
        return ""
    return name.rsplit(".", 1)[-1].lower()


def is_library_image(file_path: str, file_type: str | None = None) -> bool:
    if (file_type or "") == "Image":
        return True
    return file_ext(file_path) in IMAGE_EXTS


def thumb_relpath(file_hash: str) -> str:
    return f"{file_hash[:2]}/{file_hash}.jpg"


def thumb_path(file_hash: str) -> Path:
    return THUMBS_DIR / file_hash[:2] / f"{file_hash}.jpg"


def thumb_exists(file_hash: str) -> bool:
    return bool(file_hash) and thumb_path(file_hash).is_file()


def thumb_url(file_hash: str) -> str | None:
    if file_hash and HASH_RE.fullmatch(file_hash) and thumb_exists(file_hash):
        return f"/thumbs/{file_hash}"
    return None


def _rgb(im):
    from PIL import Image

    if im.mode == "RGB":
        return im
    if im.mode in ("RGBA", "LA") or (im.mode == "P" and "transparency" in im.info):
        bg = Image.new("RGB", im.size, (255, 255, 255))
        rgba = im.convert("RGBA")
        bg.paste(rgba, mask=rgba.split()[-1])
        return bg
    return im.convert("RGB")


def render_thumb(data: bytes) -> tuple[bytes, int, int]:
    """Resize image bytes to a JPEG thumbnail. Raises on failure."""
    from PIL import Image, ImageOps

    with Image.open(BytesIO(data)) as im:
        im = ImageOps.exif_transpose(im)
        im = _rgb(im)
        im.thumbnail((THUMB_MAX, THUMB_MAX), Image.Resampling.LANCZOS)
        width, height = im.size
        out = BytesIO()
        im.save(out, format="JPEG", quality=THUMB_QUALITY, optimize=True)
    return out.getvalue(), width, height


def write_thumb(file_hash: str, jpeg_bytes: bytes) -> Path:
    dest = thumb_path(file_hash)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".jpg.tmp")
    tmp.write_bytes(jpeg_bytes)
    tmp.replace(dest)
    return dest


def _record_feature(file_id: int, status: str, data: dict | None = None, error: str | None = None) -> None:
    conn = connect()
    try:
        conn.execute(
            """
            INSERT INTO file_features (file_id, feature, status, data_json, error, processed_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(file_id, feature) DO UPDATE SET
                status = excluded.status,
                data_json = excluded.data_json,
                error = excluded.error,
                processed_at = excluded.processed_at
            """,
            (
                file_id,
                FEATURE,
                status,
                json.dumps(data) if data is not None else None,
                error,
                _now(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _maybe_upgrade_exif(file_id: int, file_path: str, file_mtime: str | None, header: bytes) -> None:
    exif_dt = exif_datetime_from_bytes(header)
    if not exif_dt:
        return
    conn = connect()
    try:
        row = conn.execute(
            "SELECT taken_at_source FROM files WHERE id = ?", (file_id,)
        ).fetchone()
        current = row["taken_at_source"] if row else None
        if not should_replace(current, SOURCE_EXIF) and current == SOURCE_EXIF:
            iso, source = to_iso(exif_dt), SOURCE_EXIF
        else:
            iso, source = choose_taken_at(file_path, file_mtime, exif_dt=exif_dt)
        if not iso:
            return
        # Apply EXIF to every occurrence of this content? Date can differ by
        # folder, so only upgrade this row (and same-hash rows still on mtime).
        conn.execute(
            "UPDATE files SET best_taken_at = ?, taken_at_source = ? WHERE id = ?",
            (iso, source, file_id),
        )
        conn.execute(
            "UPDATE files SET best_taken_at = ?, taken_at_source = ? "
            "WHERE hash = (SELECT hash FROM files WHERE id = ?) "
            "AND (taken_at_source IS NULL OR taken_at_source = 'mtime')",
            (iso, SOURCE_EXIF, file_id),
        )
        conn.commit()
    finally:
        conn.close()


def _fetch_bytes(b2, rec: dict) -> bytes:
    remote = rec.get("upload_path") or ""
    size = int(rec.get("size") or 0)
    ext = file_ext(rec.get("file_path") or "")
    if ext in RAW_EXTS:
        probe = min(size or RAW_PROBE_BYTES, RAW_PROBE_BYTES)
        header = b2.download_file_range(remote, 0, max(probe - 1, 0))
        preview = extract_jpeg_preview(header)
        if preview:
            return preview
        if size and size > probe:
            header = b2.download_file_range(remote, 0, min(size, MAX_DOWNLOAD) - 1)
            preview = extract_jpeg_preview(header)
            if preview:
                return preview
        raise ValueError("no JPEG preview in RAW header")
    if size and size <= MAX_DOWNLOAD:
        return b2.download_file_content(remote)
    header = b2.download_file_range(remote, 0, HEADER_BYTES - 1)
    if header[:2] == b"\xff\xd8":
        return header
    preview = extract_jpeg_preview(header)
    if preview:
        return preview
    raise ValueError(f"file too large to thumbnail ({size} bytes)")


def generate_thumb_from_bytes(file_hash: str, data: bytes) -> dict:
    preview = data
    if data[:2] != b"\xff\xd8":
        extracted = extract_jpeg_preview(data)
        if extracted:
            preview = extracted
    jpeg, width, height = render_thumb(preview)
    write_thumb(file_hash, jpeg)
    return {"rel": thumb_relpath(file_hash), "w": width, "h": height}


def generate_thumb_from_path(file_hash: str, local_path: Path) -> dict:
    data = Path(local_path).read_bytes()
    return generate_thumb_from_bytes(file_hash, data)


def process_one(b2, rec: dict) -> str:
    """Create a thumb for one original. Returns a status label."""
    file_hash = rec["hash"]
    file_id = int(rec["id"])
    if thumb_exists(file_hash):
        _record_feature(
            file_id,
            "done",
            {"rel": thumb_relpath(file_hash), "cached": True},
        )
        return "exists"
    if not rec.get("upload_path"):
        _record_feature(file_id, "error", error="no upload_path")
        return "skipped"
    try:
        data = _fetch_bytes(b2, rec)
        info = generate_thumb_from_bytes(file_hash, data)
        _maybe_upgrade_exif(file_id, rec.get("file_path") or "", rec.get("file_mtime"), data[: 256 * 1024])
        _record_feature(file_id, "done", info)
        return "created"
    except Exception as e:
        _record_feature(file_id, "error", error=str(e)[:500])
        return "error"


def thumbs_pending(limit: int | None = None) -> list[dict]:
    sql = (
        "SELECT f.id, f.hash, f.upload_path, f.drive_name, f.file_path, "
        "f.size, f.mime_type, f.file_mtime "
        "FROM files f "
        "WHERE f.is_original = 1 AND f.searchable = 1 "
        "AND f.upload_path IS NOT NULL AND f.upload_path != '' "
        "AND (f.file_type = 'Image' OR lower(f.file_path) LIKE '%.dng') "
        "AND NOT EXISTS ("
        "  SELECT 1 FROM file_features ff "
        "  WHERE ff.file_id = f.id AND ff.feature = ? "
        "  AND ff.status IN ('done', 'error')"
        ") "
        "ORDER BY f.size ASC"
    )
    params: list = [FEATURE]
    if limit:
        sql += " LIMIT ?"
        params.append(int(limit))
    conn = connect(readonly=True)
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()
    return [
        {
            "id": int(r["id"]),
            "hash": r["hash"],
            "upload_path": r["upload_path"],
            "drive_name": r["drive_name"],
            "file_path": r["file_path"],
            "size": int(r["size"] or 0),
            "mime_type": r["mime_type"],
            "file_mtime": r["file_mtime"],
        }
        for r in rows
        if r["hash"] and HASH_RE.fullmatch(r["hash"]) and not thumb_exists(r["hash"])
    ]


def thumbs_status() -> dict:
    conn = connect(readonly=True)
    try:
        images = conn.execute(
            "SELECT COUNT(*) FROM files "
            "WHERE is_original = 1 AND searchable = 1 AND file_type = 'Image'"
        ).fetchone()[0]
        done = conn.execute(
            "SELECT COUNT(*) FROM file_features WHERE feature = ? AND status = 'done'",
            (FEATURE,),
        ).fetchone()[0]
        errors = conn.execute(
            "SELECT COUNT(*) FROM file_features WHERE feature = ? AND status = 'error'",
            (FEATURE,),
        ).fetchone()[0]
        dated = conn.execute(
            "SELECT COUNT(*) FROM files "
            "WHERE searchable = 1 AND file_type = 'Image' AND best_taken_at IS NOT NULL"
        ).fetchone()[0]
        images_all = conn.execute(
            "SELECT COUNT(*) FROM files WHERE searchable = 1 AND file_type = 'Image'"
        ).fetchone()[0]
    finally:
        conn.close()
    on_disk = 0
    if THUMBS_DIR.is_dir():
        on_disk = sum(1 for _ in THUMBS_DIR.glob("*/*.jpg"))
    return {
        "original_images": int(images or 0),
        "searchable_images": int(images_all or 0),
        "dated_images": int(dated or 0),
        "thumbs_done": int(done or 0),
        "thumbs_error": int(errors or 0),
        "thumbs_on_disk": on_disk,
    }


def run_thumbs(
    bucket: str | None = None,
    *,
    limit: int | None = 500,
    b2=None,
    progress=None,
) -> dict:
    from core.b2 import B2Manager
    from core.config import get_bucket_name

    stats = {"created": 0, "exists": 0, "error": 0, "skipped": 0, "checked": 0}
    pending = thumbs_pending(limit)
    stats["queued"] = len(pending)
    if progress:
        progress(0, len(pending) or 1, f"{len(pending):,} images queued for thumbnails.")
    if not pending:
        return stats

    if b2 is None:
        bucket = (bucket or get_bucket_name() or "").strip()
        if not bucket:
            raise ValueError("No B2 bucket configured.")
        if progress:
            progress(0, len(pending), f"Connecting to B2 bucket {bucket}…")
        b2 = B2Manager(bucket)

    for i, rec in enumerate(pending, 1):
        label = process_one(b2, rec)
        stats[label] = stats.get(label, 0) + 1
        stats["checked"] = i
        if progress and (i == 1 or i == len(pending) or i % 10 == 0):
            progress(i, len(pending), f"{i}/{len(pending)} {rec['file_path']} → {label}")
    return stats


def run_thumbs_job(job: dict) -> None:
    from core.config import get_bucket_name
    from core.jobs import finish_job, update_progress

    job_id = job["id"]
    payload = job["payload"]
    try:
        limit = payload.get("limit")
        limit = int(limit) if limit not in (None, "", 0, "0") else None
    except (TypeError, ValueError):
        limit = 500
    bucket = (payload.get("bucket") or get_bucket_name() or "").strip()

    def progress(done, total, line=None):
        update_progress(job_id, done, total, line)

    try:
        stats = run_thumbs(bucket, limit=limit, progress=progress)
    except Exception as e:
        finish_job(job_id, "error", str(e))
        return

    update_progress(
        job_id,
        stats["checked"],
        stats.get("queued") or stats["checked"] or 1,
        (
            f"Created {stats['created']:,}; already had {stats['exists']:,}; "
            f"{stats['error']:,} errors; {stats['skipped']:,} skipped."
        ),
    )
    finish_job(job_id, "done")
