"""Catalog queries for Home, Search, Browse, Inspector, and restore."""
from __future__ import annotations

from pathlib import Path

from core.db import connect
from core.junk import INGEST_SKIP_DIR_NAMES, SEARCH_HIDE_DIR_NAMES

ROWS_PER_PAGE = 50
DIR_PAGE_SIZE = 200

_SORT_SQL = {
    "path": "f.file_path ASC",
    "mtime_desc": "f.file_mtime DESC",
    "mtime_asc": "f.file_mtime ASC",
    "taken_desc": "f.best_taken_at DESC",
    "taken_asc": "f.best_taken_at ASC",
    "size_desc": "f.size DESC",
    "size_asc": "f.size ASC",
    "type": "f.file_type ASC",
}


def format_size(size_bytes: int | float | None) -> str:
    if not size_bytes:
        return "0 B"
    size = float(size_bytes)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


def _fmt_mtime(value) -> str:
    if not value:
        return ""
    text = str(value)
    return text[:16].replace("T", " ")


def _file_row(row) -> dict:
    path = row["file_path"] or ""
    keys = row.keys()
    taken = row["best_taken_at"] if "best_taken_at" in keys else None
    return {
        "id": int(row["id"]),
        "drive_name": row["drive_name"],
        "file_path": path,
        "name": Path(path.replace("\\", "/")).name,
        "size": int(row["size"] or 0),
        "size_fmt": format_size(row["size"]),
        "is_original": bool(row["is_original"]),
        "file_type": row["file_type"] or "",
        "mime_type": row["mime_type"] or "",
        "file_mtime": row["file_mtime"],
        "mtime_fmt": _fmt_mtime(row["file_mtime"]),
        "best_taken_at": taken,
        "taken_fmt": _fmt_mtime(taken) if taken else "",
        "taken_at_source": row["taken_at_source"] if "taken_at_source" in keys else None,
        "created_at": row["created_at"] if "created_at" in keys else None,
        "hash": row["hash"] if "hash" in keys else None,
        "upload_path": row["upload_path"] if "upload_path" in keys else None,
    }


def get_vault_overview() -> dict:
    """Totals plus per-drive cards for the Home page."""
    conn = connect(readonly=True)
    try:
        totals = conn.execute(
            """
            SELECT
                COUNT(*) AS files,
                COUNT(DISTINCT drive_name) AS drives,
                SUM(CASE WHEN is_original = 1 THEN 1 ELSE 0 END) AS originals,
                SUM(CASE WHEN is_original = 0 THEN 1 ELSE 0 END) AS copies,
                COALESCE(SUM(size), 0) AS logical_bytes,
                COALESCE(SUM(CASE WHEN is_original = 1 THEN size ELSE 0 END), 0) AS unique_bytes
            FROM files
            """
        ).fetchone()

        drive_rows = conn.execute(
            """
            SELECT
                drive_name,
                COUNT(*) AS files,
                SUM(CASE WHEN is_original = 1 THEN 1 ELSE 0 END) AS originals,
                COALESCE(SUM(size), 0) AS logical_bytes,
                COALESCE(SUM(CASE WHEN is_original = 1 THEN size ELSE 0 END), 0) AS unique_bytes
            FROM files
            GROUP BY drive_name
            ORDER BY files DESC
            """
        ).fetchall()
    finally:
        conn.close()

    logical = int(totals["logical_bytes"] or 0)
    unique = int(totals["unique_bytes"] or 0)
    files = int(totals["files"] or 0)
    drives = int(totals["drives"] or 0)
    return {
        "files": files,
        "files_fmt": f"{files:,}",
        "drives": drives,
        "drives_fmt": f"{drives:,}",
        "originals": int(totals["originals"] or 0),
        "copies": int(totals["copies"] or 0),
        "logical_bytes": logical,
        "unique_bytes": unique,
        "saved_bytes": max(logical - unique, 0),
        "logical_fmt": format_size(logical),
        "unique_fmt": format_size(unique),
        "saved_fmt": format_size(max(logical - unique, 0)),
        "drive_cards": [
            {
                "name": row["drive_name"],
                "files": int(row["files"] or 0),
                "files_fmt": f"{int(row['files'] or 0):,}",
                "originals": int(row["originals"] or 0),
                "logical_bytes": int(row["logical_bytes"] or 0),
                "unique_bytes": int(row["unique_bytes"] or 0),
                "logical_fmt": format_size(row["logical_bytes"]),
                "unique_fmt": format_size(row["unique_bytes"]),
                "saved_fmt": format_size(
                    max(int(row["logical_bytes"] or 0) - int(row["unique_bytes"] or 0), 0)
                ),
            }
            for row in drive_rows
        ],
    }


def get_drive_names() -> list[str]:
    conn = connect(readonly=True)
    try:
        rows = conn.execute(
            "SELECT DISTINCT drive_name FROM files ORDER BY drive_name"
        ).fetchall()
    finally:
        conn.close()
    return [r["drive_name"] for r in rows]


def get_file_types() -> list[str]:
    conn = connect(readonly=True)
    try:
        rows = conn.execute(
            "SELECT DISTINCT file_type FROM files "
            "WHERE file_type IS NOT NULL AND file_type != '' "
            "ORDER BY file_type"
        ).fetchall()
    finally:
        conn.close()
    return [r["file_type"] for r in rows]


def _fts_query(raw: str) -> str:
    raw = raw.strip()
    if not raw:
        return ""
    special = set('*"')
    if any(c in raw for c in special):
        return raw
    # Token prefix, not a quoted phrase prefix — `"foo" *` is far too wide
    # on a million-row FTS index and makes COUNT(*) crawl.
    return f"{raw}*"


def search_files(
    q: str,
    *,
    drive: str = "",
    file_type: str = "",
    origin: str = "",
    ext: str = "",
    year: str = "",
    hide_hidden: bool = True,
    sort: str = "path",
    page: int = 1,
    per_page: int = ROWS_PER_PAGE,
) -> dict:
    """Global FTS search. Drive is an optional filter, never required."""
    q = (q or "").strip()
    page = max(1, int(page or 1))
    order = _SORT_SQL.get(sort, _SORT_SQL["path"])

    conditions = []
    params: list = []

    if hide_hidden:
        conditions.append("f.searchable = 1")

    if drive:
        conditions.append("f.drive_name = ?")
        params.append(drive)

    if file_type:
        conditions.append("f.file_type = ?")
        params.append(file_type)

    if origin == "original":
        conditions.append("f.is_original = 1")
    elif origin == "copy":
        conditions.append("f.is_original = 0")

    if ext:
        exts = [e.strip().lower().lstrip(".") for e in ext.split(",") if e.strip()]
        if exts:
            conditions.append("(" + " OR ".join("f.file_path LIKE ?" for _ in exts) + ")")
            params.extend(f"%.{e}" for e in exts)

    if year and str(year).isdigit() and len(str(year)) == 4:
        conditions.append("substr(f.best_taken_at, 1, 4) = ?")
        params.append(str(year))

    if q:
        conditions.append(
            "f.id IN (SELECT rowid FROM files_fts WHERE files_fts MATCH ?)"
        )
        params.append(_fts_query(q))

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

    # Fetch one extra row instead of COUNT(*) — FTS + flag filters over
    # this catalog is too large for an exact count on every page view.
    offset = (page - 1) * per_page
    select_sql = (
        "SELECT f.id, f.drive_name, f.file_path, f.size, f.is_original, "
        "f.file_type, f.mime_type, f.file_mtime, f.created_at, "
        "f.best_taken_at, f.taken_at_source "
        f"FROM files f{where} "
        f"ORDER BY {order} LIMIT ? OFFSET ?"
    )

    conn = connect(readonly=True)
    try:
        rows = conn.execute(select_sql, [*params, per_page + 1, offset]).fetchall()
    finally:
        conn.close()

    has_more = len(rows) > per_page
    rows = rows[:per_page]
    shown = offset + len(rows)
    return {
        "total": shown,
        "total_fmt": f"{shown:,}+" if has_more else f"{shown:,}",
        "page": page,
        "pages": page + 1 if has_more else page,
        "has_more": has_more,
        "per_page": per_page,
        "files": [_file_row(r) for r in rows],
    }


def list_subdirs(
    drive: str,
    prefix: str,
    *,
    hide_hidden: bool = True,
    seek: str = "",
    limit: int = DIR_PAGE_SIZE,
) -> dict:
    """Skip-scan child directories. `seek` continues a previous page."""
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    seek_path = seek if seek else prefix
    dirs: list[dict] = []
    next_seek = ""

    conn = connect(readonly=True)
    try:
        cur = conn.cursor()
        # Bound the loop so a huge folder cannot hang the request.
        for _ in range(limit * 8):
            if hide_hidden:
                row = cur.execute(
                    "SELECT file_path FROM files "
                    "WHERE drive_name = ? AND file_path > ? AND file_path LIKE ? "
                    "AND searchable = 1 "
                    "ORDER BY file_path ASC LIMIT 1",
                    (drive, seek_path, f"{prefix}%"),
                ).fetchone()
            else:
                row = cur.execute(
                    "SELECT file_path FROM files "
                    "WHERE drive_name = ? AND file_path > ? AND file_path LIKE ? "
                    "ORDER BY file_path ASC LIMIT 1",
                    (drive, seek_path, f"{prefix}%"),
                ).fetchone()
            if not row:
                break
            full_path = row[0]
            rel = full_path[len(prefix):]
            if "/" in rel:
                name = rel.split("/")[0]
                if hide_hidden and (
                    name in INGEST_SKIP_DIR_NAMES or name in SEARCH_HIDE_DIR_NAMES
                ):
                    seek_path = f"{prefix}{name}\uffff"
                    continue
                dirs.append({"name": name, "path": f"{prefix}{name}"})
                seek_path = f"{prefix}{name}\uffff"
                if len(dirs) >= limit:
                    next_seek = seek_path
                    break
            else:
                seek_path = full_path
    finally:
        conn.close()

    return {"dirs": dirs, "next_seek": next_seek}


def list_folder_files(
    drive: str,
    prefix: str,
    *,
    hide_hidden: bool = True,
    page: int = 1,
    per_page: int = ROWS_PER_PAGE,
) -> dict:
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    page = max(1, int(page or 1))
    conditions = ["f.drive_name = ?", "f.file_path LIKE ?", "f.file_path NOT LIKE ?"]
    params: list = [drive, f"{prefix}%", f"{prefix}%/%"]
    if hide_hidden:
        conditions.append("f.searchable = 1")
    where = " WHERE " + " AND ".join(conditions)

    conn = connect(readonly=True)
    try:
        total = conn.execute(f"SELECT COUNT(*) FROM files f{where}", params).fetchone()[0]
        offset = (page - 1) * per_page
        rows = conn.execute(
            "SELECT f.id, f.drive_name, f.file_path, f.size, f.is_original, "
            "f.file_type, f.mime_type, f.file_mtime, f.created_at, "
            "f.best_taken_at, f.taken_at_source "
            f"FROM files f{where} ORDER BY f.file_path LIMIT ? OFFSET ?",
            [*params, per_page, offset],
        ).fetchall()
    finally:
        conn.close()

    files = []
    for r in rows:
        item = _file_row(r)
        item["name"] = item["file_path"][len(prefix):]
        files.append(item)

    pages = max(1, (total + per_page - 1) // per_page) if total else 1
    return {
        "total": int(total or 0),
        "total_fmt": f"{int(total or 0):,}",
        "page": page,
        "pages": pages,
        "has_more": page < pages,
        "files": files,
    }


def get_file(file_id: int) -> dict | None:
    conn = connect(readonly=True)
    try:
        row = conn.execute(
            "SELECT id, hash, size, drive_name, file_path, upload_path, is_original, "
            "created_at, file_mtime, file_ctime, file_atime, mime_type, file_type, "
            "best_taken_at, taken_at_source "
            "FROM files WHERE id = ?",
            (file_id,),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return None
    item = _file_row(row)
    item["hash"] = row["hash"]
    item["upload_path"] = row["upload_path"]
    item["file_ctime"] = row["file_ctime"]
    item["file_atime"] = row["file_atime"]
    parent = str(Path(item["file_path"].replace("\\", "/")).parent)
    item["parent_path"] = "" if parent == "." else parent
    return item


def get_copies(file_hash: str, exclude_id: int | None = None, limit: int = 50) -> dict:
    conn = connect(readonly=True)
    try:
        total = conn.execute(
            "SELECT COUNT(*) FROM files WHERE hash = ?", (file_hash,)
        ).fetchone()[0]
        rows = conn.execute(
            "SELECT id, drive_name, file_path, size, is_original, file_mtime, file_type, mime_type "
            "FROM files WHERE hash = ? ORDER BY is_original DESC, drive_name, file_path LIMIT ?",
            (file_hash, limit),
        ).fetchall()
    finally:
        conn.close()
    copies = [_file_row(r) for r in rows]
    if exclude_id is not None:
        copies = [c for c in copies if c["id"] != exclude_id]
    return {"total": int(total or 0), "copies": copies}


def resolve_folder_ids(drive: str, folder_path: str) -> list[int]:
    """Every file under a folder, including VCS (needed for a complete restore)."""
    path = folder_path if folder_path.endswith("/") else folder_path + "/"
    if folder_path == "":
        like = "%"
        params: tuple = (drive,)
        sql = "SELECT id FROM files WHERE drive_name = ?"
    else:
        params = (drive, f"{path}%")
        sql = "SELECT id FROM files WHERE drive_name = ? AND file_path LIKE ?"
    conn = connect(readonly=True)
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()
    return [int(r["id"]) for r in rows]


def get_files_by_ids(ids: list[int]) -> list[dict]:
    if not ids:
        return []
    conn = connect(readonly=True)
    try:
        placeholders = ",".join("?" * len(ids))
        rows = conn.execute(
            f"SELECT id, hash, size, drive_name, file_path, upload_path, is_original, "
            f"file_mtime, file_type, mime_type "
            f"FROM files WHERE id IN ({placeholders}) ORDER BY drive_name, file_path",
            ids,
        ).fetchall()
    finally:
        conn.close()
    out = []
    for r in rows:
        item = _file_row(r)
        item["hash"] = r["hash"]
        item["upload_path"] = r["upload_path"]
        out.append(item)
    return out


def count_originals() -> dict:
    """Originals that should exist on B2 vs scan-only rows with no remote path."""
    conn = connect(readonly=True)
    try:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN upload_path IS NOT NULL AND upload_path != '' THEN 1 ELSE 0 END)
                    AS with_remote,
                SUM(CASE WHEN upload_path IS NULL OR upload_path = '' THEN 1 ELSE 0 END)
                    AS no_remote
            FROM files
            WHERE is_original = 1
            """
        ).fetchone()
    finally:
        conn.close()
    return {
        "total": int(row["total"] or 0),
        "with_remote": int(row["with_remote"] or 0),
        "no_remote": int(row["no_remote"] or 0),
    }


def load_originals(*, sample: int | None = None) -> list[dict]:
    """Originals that have a B2 upload_path. Sample is a random subset."""
    conn = connect(readonly=True)
    try:
        sql = (
            "SELECT id, drive_name, file_path, upload_path, size, hash "
            "FROM files WHERE is_original = 1 "
            "AND upload_path IS NOT NULL AND upload_path != ''"
        )
        if sample:
            rows = conn.execute(
                sql + " ORDER BY RANDOM() LIMIT ?", (int(sample),)
            ).fetchall()
        else:
            rows = conn.execute(sql).fetchall()
    finally:
        conn.close()
    return [
        {
            "id": int(r["id"]),
            "drive_name": r["drive_name"],
            "file_path": r["file_path"],
            "upload_path": r["upload_path"],
            "size": int(r["size"] or 0),
            "hash": r["hash"],
        }
        for r in rows
    ]


def delete_drive(drive_name: str) -> int:
    """Remove every catalog row for a drive. B2 objects are left alone."""
    conn = connect()
    try:
        cur = conn.execute("DELETE FROM files WHERE drive_name = ?", (drive_name,))
        deleted = int(cur.rowcount or 0)
        conn.execute(
            "DELETE FROM restore_set WHERE file_id NOT IN (SELECT id FROM files)"
        )
        conn.commit()
        return deleted
    finally:
        conn.close()


def get_selection_size(ids: list[int]) -> int:
    if not ids:
        return 0
    conn = connect(readonly=True)
    try:
        placeholders = ",".join("?" * len(ids))
        row = conn.execute(
            f"SELECT COALESCE(SUM(size), 0) FROM files WHERE id IN ({placeholders})",
            ids,
        ).fetchone()
    finally:
        conn.close()
    return int(row[0] or 0)
