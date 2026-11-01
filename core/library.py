"""Date backfill, library grid, and timeline moments."""
from __future__ import annotations

from pathlib import Path

from core.catalog import ROWS_PER_PAGE, format_size
from core.db import connect
from core.thumbs import thumb_url
from core.time import (
    choose_taken_at,
    is_moment_noise,
    moment_folder,
    moment_title,
    should_replace,
)

LIBRARY_PAGE = 60
MOMENT_PAGE = 80
_MONTH_NAMES = (
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
)


def _file_card(row, *, copies: int = 1) -> dict:
    path = row["file_path"] or ""
    taken = row["best_taken_at"] if "best_taken_at" in row.keys() else None
    file_hash = row["hash"] if "hash" in row.keys() else None
    return {
        "id": int(row["id"]),
        "drive_name": row["drive_name"],
        "file_path": path,
        "name": Path(path.replace("\\", "/")).name,
        "hash": file_hash,
        "size": int(row["size"] or 0),
        "size_fmt": format_size(row["size"]),
        "best_taken_at": taken,
        "taken_fmt": (str(taken)[:10] if taken else ""),
        "taken_at_source": row["taken_at_source"] if "taken_at_source" in row.keys() else None,
        "file_type": (row["file_type"] if "file_type" in row.keys() else "") or "",
        "thumb_url": thumb_url(file_hash) if file_hash else None,
        "copies": copies,
        "is_original": bool(row["is_original"]) if "is_original" in row.keys() else True,
    }


def item_noun(file_type: str, count: int) -> str:
    if file_type == "Image":
        return "photo" if count == 1 else "photos"
    if file_type == "Video":
        return "video" if count == 1 else "videos"
    if file_type == "Audio":
        return "track" if count == 1 else "tracks"
    return "file" if count == 1 else "files"


def dates_status() -> dict:
    conn = connect(readonly=True)
    try:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS images,
                SUM(CASE WHEN best_taken_at IS NOT NULL THEN 1 ELSE 0 END) AS dated,
                SUM(CASE WHEN taken_at_source = 'exif' THEN 1 ELSE 0 END) AS exif,
                SUM(CASE WHEN taken_at_source = 'folder' THEN 1 ELSE 0 END) AS folder,
                SUM(CASE WHEN taken_at_source = 'mtime' THEN 1 ELSE 0 END) AS mtime
            FROM files
            WHERE searchable = 1 AND file_type = 'Image'
            """
        ).fetchone()
        years = conn.execute(
            """
            SELECT COUNT(DISTINCT substr(best_taken_at, 1, 4))
            FROM files
            WHERE searchable = 1 AND file_type = 'Image'
              AND best_taken_at IS NOT NULL
            """
        ).fetchone()[0]
    finally:
        conn.close()
    images = int(row["images"] or 0)
    dated = int(row["dated"] or 0)
    return {
        "images": images,
        "images_fmt": f"{images:,}",
        "dated": dated,
        "dated_fmt": f"{dated:,}",
        "undated": max(images - dated, 0),
        "exif": int(row["exif"] or 0),
        "folder": int(row["folder"] or 0),
        "mtime": int(row["mtime"] or 0),
        "years": int(years or 0),
    }


def backfill_dates(*, limit: int | None = None, progress=None) -> dict:
    """Fill best_taken_at from folder names and mtime. Does not overwrite EXIF."""
    stats = {"updated": 0, "skipped": 0, "unchanged": 0, "checked": 0}
    last_id = 0
    batch = 2000
    remaining = None if limit is None else max(0, int(limit))

    while True:
        take = batch if remaining is None else min(batch, remaining)
        if take == 0:
            break
        conn = connect(readonly=True)
        try:
            rows = conn.execute(
                "SELECT id, file_path, file_mtime, best_taken_at, taken_at_source "
                "FROM files WHERE id > ? ORDER BY id LIMIT ?",
                (last_id, take),
            ).fetchall()
        finally:
            conn.close()
        if not rows:
            break

        changes = []
        for row in rows:
            last_id = int(row["id"])
            stats["checked"] += 1
            if remaining is not None:
                remaining -= 1
            existing_source = row["taken_at_source"]
            iso, source = choose_taken_at(row["file_path"] or "", row["file_mtime"])
            if not iso:
                stats["skipped"] += 1
                continue
            if existing_source == source and row["best_taken_at"] == iso:
                stats["unchanged"] += 1
                continue
            if existing_source and not should_replace(existing_source, source):
                if existing_source == source:
                    changes.append((iso, source, last_id))
                else:
                    stats["unchanged"] += 1
                continue
            changes.append((iso, source, last_id))

        if changes:
            conn = connect()
            try:
                conn.executemany(
                    "UPDATE files SET best_taken_at = ?, taken_at_source = ? WHERE id = ?",
                    changes,
                )
                conn.commit()
            finally:
                conn.close()
            stats["updated"] += len(changes)

        if progress:
            progress(
                stats["checked"],
                stats["checked"],
                f"Dated {stats['updated']:,} files ({stats['checked']:,} scanned).",
            )
        if remaining is not None and remaining <= 0:
            break
        if len(rows) < take:
            break
    return stats


def run_dates_job(job: dict) -> None:
    from core.jobs import finish_job, update_progress

    job_id = job["id"]
    payload = job["payload"] or {}
    try:
        limit = payload.get("limit")
        limit = int(limit) if limit not in (None, "", 0, "0") else None
    except (TypeError, ValueError):
        limit = None

    def progress(done, total, line=None):
        update_progress(job_id, done, total, line)

    try:
        stats = backfill_dates(limit=limit, progress=progress)
    except Exception as e:
        finish_job(job_id, "error", str(e))
        return

    update_progress(
        job_id,
        stats["checked"],
        stats["checked"] or 1,
        (
            f"Updated {stats['updated']:,}; left {stats['unchanged']:,} in place; "
            f"{stats['skipped']:,} had no usable date."
        ),
    )
    finish_job(job_id, "done")


def _type_clause(file_type: str, *, alias: str = "f") -> tuple[str | None, list]:
    prefix = f"{alias}." if alias else ""
    if not file_type:
        return None, []
    if file_type == "Image":
        return (
            f"({prefix}file_type = 'Image' OR lower({prefix}file_path) LIKE '%.dng')",
            [],
        )
    return f"{prefix}file_type = ?", [file_type]


def _library_where(
    *,
    file_type: str = "",
    year: str = "",
    month: str = "",
    drive: str = "",
    hide_hidden: bool = True,
    originals_only: bool = True,
    dated_only: bool = False,
) -> tuple[str, list]:
    conditions: list[str] = []
    params: list = []
    type_sql, type_params = _type_clause(file_type)
    if type_sql:
        conditions.append(type_sql)
        params.extend(type_params)
    if hide_hidden:
        conditions.append("f.searchable = 1")
    if originals_only:
        conditions.append("f.is_original = 1")
    if drive:
        conditions.append("f.drive_name = ?")
        params.append(drive)
    if year and re_year(year):
        conditions.append("substr(f.best_taken_at, 1, 4) = ?")
        params.append(year)
    if month and re_month(month):
        conditions.append("substr(f.best_taken_at, 6, 2) = ?")
        params.append(month.zfill(2))
    if dated_only or year or month:
        conditions.append("f.best_taken_at IS NOT NULL")
    if not conditions:
        return "", []
    return " WHERE " + " AND ".join(conditions), params


def re_year(value: str) -> bool:
    return bool(value) and value.isdigit() and len(value) == 4


def re_month(value: str) -> bool:
    if not value:
        return False
    try:
        n = int(value)
    except ValueError:
        return False
    return 1 <= n <= 12


def list_years(*, file_type: str = "", hide_hidden: bool = True) -> list[dict]:
    conditions = ["best_taken_at IS NOT NULL"]
    params: list = []
    type_sql, type_params = _type_clause(file_type, alias="")
    if type_sql:
        conditions.append(type_sql)
        params.extend(type_params)
    if hide_hidden:
        conditions.append("searchable = 1")
    where = " WHERE " + " AND ".join(conditions)
    conn = connect(readonly=True)
    try:
        rows = conn.execute(
            f"SELECT substr(best_taken_at, 1, 4) AS year, COUNT(*) AS n "
            f"FROM files{where} GROUP BY 1 ORDER BY 1 DESC",
            params,
        ).fetchall()
    finally:
        conn.close()
    return [{"year": r["year"], "count": int(r["n"] or 0), "count_fmt": f"{int(r['n'] or 0):,}"} for r in rows]


def list_months(year: str, *, file_type: str = "", hide_hidden: bool = True) -> list[dict]:
    if not re_year(year):
        return []
    conditions = ["substr(best_taken_at, 1, 4) = ?"]
    params: list = [year]
    type_sql, type_params = _type_clause(file_type, alias="")
    if type_sql:
        conditions.append(type_sql)
        params.extend(type_params)
    if hide_hidden:
        conditions.append("searchable = 1")
    where = " WHERE " + " AND ".join(conditions)
    conn = connect(readonly=True)
    try:
        rows = conn.execute(
            f"SELECT substr(best_taken_at, 6, 2) AS month, COUNT(*) AS n "
            f"FROM files{where} GROUP BY 1 ORDER BY 1",
            params,
        ).fetchall()
    finally:
        conn.close()
    out = []
    for r in rows:
        try:
            idx = int(r["month"]) - 1
        except (TypeError, ValueError):
            continue
        if 0 <= idx < 12:
            out.append({
                "month": r["month"],
                "name": _MONTH_NAMES[idx],
                "count": int(r["n"] or 0),
                "count_fmt": f"{int(r['n'] or 0):,}",
            })
    return out


def format_when(stamp: str) -> str:
    if not stamp:
        return ""
    try:
        year = stamp[:4]
        month_i = int(stamp[5:7]) if len(stamp) >= 7 else 0
    except ValueError:
        return stamp
    if not (1 <= month_i <= 12):
        return stamp
    name = _MONTH_NAMES[month_i - 1]
    if len(stamp) >= 10 and stamp[8:10].isdigit() and stamp[8:10] != "01":
        return f"{name} {int(stamp[8:10])}, {year}"
    if len(stamp) == 7:
        return f"{name} {year}"
    return f"{name} {int(stamp[8:10])}, {year}" if len(stamp) >= 10 else f"{name} {year}"


def list_library(
    *,
    file_type: str = "",
    year: str = "",
    month: str = "",
    drive: str = "",
    hide_hidden: bool = True,
    page: int = 1,
    per_page: int = LIBRARY_PAGE,
) -> dict:
    page = max(1, int(page or 1))
    where, params = _library_where(
        file_type=file_type,
        year=year,
        month=month,
        drive=drive,
        hide_hidden=hide_hidden,
        originals_only=True,
    )
    offset = (page - 1) * per_page
    conn = connect(readonly=True)
    try:
        rows = conn.execute(
            "SELECT f.id, f.hash, f.drive_name, f.file_path, f.size, f.is_original, "
            "f.best_taken_at, f.taken_at_source, f.file_type "
            f"FROM files f{where} "
            "ORDER BY (f.best_taken_at IS NULL), f.best_taken_at DESC, f.id DESC "
            "LIMIT ? OFFSET ?",
            [*params, per_page + 1, offset],
        ).fetchall()
    finally:
        conn.close()

    has_more = len(rows) > per_page
    rows = rows[:per_page]
    photos = [_file_card(r) for r in rows]
    shown = offset + len(photos)
    return {
        "total": shown,
        "total_fmt": f"{shown:,}+" if has_more else f"{shown:,}",
        "page": page,
        "pages": page + 1 if has_more else page,
        "has_more": has_more,
        "per_page": per_page,
        "files": photos,
        "noun": item_noun(file_type, len(photos)),
    }


def list_moments(
    *,
    file_type: str = "",
    year: str = "",
    month: str = "",
    hide_hidden: bool = True,
    limit: int = 80,
) -> list[dict]:
    """Group files by meaningful parent folder + day."""
    where, params = _library_where(
        file_type=file_type,
        year=year,
        month=month,
        hide_hidden=hide_hidden,
        originals_only=False,
        dated_only=True,
    )
    # Cap the raw pull so a huge month still renders. Group in Python so
    # we can walk past DCIM / 140CANON / .picasaoriginals.
    conn = connect(readonly=True)
    try:
        rows = conn.execute(
            "SELECT f.id, f.hash, f.drive_name, f.file_path, f.size, f.is_original, "
            "f.best_taken_at, f.taken_at_source, f.file_type "
            f"FROM files f{where} "
            "ORDER BY f.best_taken_at ASC, f.id ASC "
            "LIMIT 8000",
            params,
        ).fetchall()
    finally:
        conn.close()

    groups: dict[tuple[str, str], dict] = {}
    month_bucket = bool(year and month)
    for row in rows:
        path = row["file_path"] or ""
        if is_moment_noise(path):
            continue
        folder = moment_folder(path)
        title = moment_title(folder)
        taken = row["best_taken_at"] or ""
        # A month page is one card per titled folder; otherwise group by day.
        day = taken[:7] if month_bucket else taken[:10]
        key = (title.lower(), day)
        item = groups.get(key)
        if item is None:
            item = {
                "folder": folder,
                "title": title,
                "day": day,
                "hashes": set(),
                "copies": 0,
                "cover": None,
                "drives": set(),
            }
            groups[key] = item
        item["copies"] += 1
        item["drives"].add(row["drive_name"])
        if row["hash"] not in item["hashes"]:
            item["hashes"].add(row["hash"])
            card = _file_card(row)
            if _better_cover(item["cover"], card):
                item["cover"] = card

    moments = []
    for item in groups.values():
        cover = item["cover"] or {}
        moments.append(
            {
                "folder": item["folder"],
                "title": item["title"],
                "day": item["day"],
                "when": format_when(item["day"]),
                "photos": len(item["hashes"]),
                "photos_fmt": f"{len(item['hashes']):,}",
                "noun": item_noun(file_type, len(item["hashes"])),
                "copies": item["copies"],
                "drives": sorted(item["drives"]),
                "thumb_url": cover.get("thumb_url"),
                "cover_id": cover.get("id"),
            }
        )
    moments.sort(key=lambda m: (m["day"] or "", m["title"].lower()))
    return moments[:limit]


def _better_cover(current: dict | None, new: dict) -> bool:
    if current is None:
        return True
    new_image = new.get("file_type") == "Image"
    old_image = current.get("file_type") == "Image"
    if new_image and not old_image:
        return True
    if old_image and not new_image:
        return False
    if new.get("thumb_url") and not current.get("thumb_url"):
        return True
    if new.get("is_original") and not current.get("is_original"):
        return True
    return False


def list_moment_photos(
    folder: str,
    day: str,
    *,
    title: str = "",
    file_type: str = "",
    hide_hidden: bool = True,
    page: int = 1,
    per_page: int = MOMENT_PAGE,
) -> dict:
    page = max(1, int(page or 1))
    year = day[:4] if day and len(day) >= 4 else ""
    month = day[5:7] if day and len(day) >= 7 else ""
    where, params = _library_where(
        file_type=file_type,
        year=year,
        month=month,
        hide_hidden=hide_hidden,
        originals_only=True,
        dated_only=True,
    )
    if day and len(day) >= 10:
        where += " AND substr(f.best_taken_at, 1, 10) = ?"
        params.append(day[:10])
    elif day and len(day) >= 7:
        where += " AND substr(f.best_taken_at, 1, 7) = ?"
        params.append(day[:7])

    conn = connect(readonly=True)
    try:
        rows = conn.execute(
            "SELECT f.id, f.hash, f.drive_name, f.file_path, f.size, f.is_original, "
            "f.best_taken_at, f.taken_at_source, f.file_type "
            f"FROM files f{where} "
            "ORDER BY f.best_taken_at ASC, f.file_path ASC",
            params,
        ).fetchall()
    finally:
        conn.close()

    want_title = (title or moment_title(folder)).strip().lower()
    photos = []
    for row in rows:
        path = row["file_path"] or ""
        if is_moment_noise(path):
            continue
        if moment_title(moment_folder(path)).strip().lower() != want_title:
            continue
        photos.append(_file_card(row))

    total = len(photos)
    offset = (page - 1) * per_page
    page_rows = photos[offset:offset + per_page]
    pages = max(1, (total + per_page - 1) // per_page) if total else 1
    return {
        "total": total,
        "total_fmt": f"{total:,}",
        "page": page,
        "pages": pages,
        "has_more": page < pages,
        "per_page": per_page,
        "files": page_rows,
        "title": title or moment_title(folder),
        "folder": folder,
        "day": day,
        "when": format_when(day),
        "noun": item_noun(file_type, total),
    }
