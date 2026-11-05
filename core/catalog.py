"""Catalog queries for Home, Search, Browse, Inspector, and restore."""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

from core.db import connect
from core.junk import INGEST_SKIP_DIR_NAMES, SEARCH_HIDE_DIR_NAMES

ROWS_PER_PAGE = 50
FOLDER_PAGE_SIZE = 40
DIR_PAGE_SIZE = 200
# Other paths disclosed on a grouped search row, besides the one shown.
PLACE_PREVIEW = 12

_DESC_SORTS = {"size_desc", "mtime_desc", "taken_desc"}


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


# unicode61 indexes letters, numbers, and underscore. The query parser rejects
# other characters as syntax ("." in dice1.png, "-" in file-name), even though
# the index stores them as word breaks.
_FTS_TOKEN_RE = re.compile(r'"[^"]*"\*?|\w+\*?|\*')
_NAME_TOKEN_RE = re.compile(r"\w+")
_FTS_OPERATORS = {"AND", "OR", "NOT", "NEAR"}
# Errors raised by the FTS5 query parser (not by SQLite itself).
_FTS_QUERY_ERRORS = (
    "fts5: syntax error",
    "unknown special query",
    "unterminated string",
)


def fts_query(raw: str) -> str:
    """Turn free-text input into a valid FTS5 MATCH expression.

    unicode61 stores letters, numbers, and underscore as tokens; every other
    character is a word break. Rebuild the expression from those tokens so
    punctuation (the "." in ``dice*.png``, the "-" in ``file-name``) is never
    parsed as FTS syntax. Quoted phrases and explicit ``*`` prefixes survive,
    boolean operators are kept only between terms, and the last bare term gets
    an implicit prefix ``*``.
    """
    raw = (raw or "").strip()
    if not raw:
        return ""
    tokens: list[str] = []
    for token in _FTS_TOKEN_RE.findall(raw):
        # A lone "*" is not a valid FTS expression.
        if token == "*":
            continue
        if not token.startswith('"'):
            # "dice*" is a prefix term, "AND*" is still a boolean operator.
            if token.endswith("*") and token[:-1] in _FTS_OPERATORS:
                token = token[:-1]
            if token in _FTS_OPERATORS:
                # FTS5 has no leading operator; a dangling trailing one is a
                # syntax error and gets dropped below.
                if not tokens:
                    continue
                tokens.append(token)
                continue
        tokens.append(token)
    while tokens and tokens[-1] in _FTS_OPERATORS:
        tokens.pop()
    if not tokens:
        return ""
    # Prefix the last token only, and never a quoted phrase: a phrase plus a
    # bare "*" matches every row and makes the search crawl.
    last = tokens[-1]
    if not last.startswith('"') and not last.endswith("*"):
        tokens[-1] += "*"
    return " ".join(tokens)


def _term_needle(term: str) -> str:
    """A substring that has to appear before this term can match a name."""
    body = term[:-1] if term.endswith("*") else term
    if body.startswith('"') and body.endswith('"') and len(body) >= 2:
        body = body[1:-1]
    words = _NAME_TOKEN_RE.findall(body.lower())
    return words[0] if words else ""


def _term_in_tokens(term: str, tokens: list[str]) -> bool:
    prefix = term.endswith("*")
    body = term[:-1] if prefix else term
    if body.startswith('"'):
        phrase = body[1:-1] if body.endswith('"') else body[1:]
        words = _NAME_TOKEN_RE.findall(phrase.lower())
        if not words:
            return False
        if prefix:
            head, last = words[:-1], words[-1]
            span = len(words)
            for i in range(len(tokens) - span + 1):
                if tokens[i:i + len(head)] == head and tokens[i + len(head)].startswith(last):
                    return True
            return False
        span = len(words)
        return any(tokens[i:i + span] == words for i in range(len(tokens) - span + 1))
    word = body.lower()
    if not word:
        return False
    if prefix:
        return any(tok.startswith(word) for tok in tokens)
    return word in tokens


def _name_matcher(expression: str):
    """Match one path segment the way the FTS expression matches that name.

    Terms are AND within a clause and OR across clauses, same as the
    expression ``fts_query`` builds. Matching is against the segment's own
    tokens, so a folder named Backup matches ``backup`` and a file that
    merely lives inside that folder does not.
    """
    parts = _FTS_TOKEN_RE.findall(expression or "")
    clauses: list[list[tuple[str, bool]]] = [[]]
    i = 0
    while i < len(parts):
        part = parts[i]
        if part == "OR":
            clauses.append([])
            i += 1
            continue
        if part == "AND":
            i += 1
            continue
        negated = False
        if part == "NOT":
            negated = True
            i += 1
            if i >= len(parts):
                break
            part = parts[i]
        if part in _FTS_OPERATORS or part == "*":
            i += 1
            continue
        clauses[-1].append((part, negated))
        i += 1
    clauses = [clause for clause in clauses if clause]

    needle_groups: list[list[str]] = []
    for clause in clauses:
        positives = [term for term, negated in clause if not negated]
        if not positives:
            # A NOT-only clause can match a name that lacks every needle.
            needle_groups.append([])
            continue
        needles = []
        for term in positives:
            needle = _term_needle(term)
            if needle:
                needles.append(needle)
        needle_groups.append(needles)

    def matches(name: str) -> bool:
        if not name or not clauses:
            return False
        lowered = name.lower()
        if not any(all(needle in lowered for needle in group) for group in needle_groups):
            return False
        tokens = _NAME_TOKEN_RE.findall(lowered)
        if not tokens:
            return False
        for clause in clauses:
            if all(_term_in_tokens(term, tokens) != negated for term, negated in clause):
                return True
        return False

    return matches


def _noun_label(count: int, singular: str, plural: str) -> str:
    return f"{count:,} {singular if count == 1 else plural}"


def _also_label(count: int, noun: str) -> str:
    if count <= 0:
        return ""
    word = noun if count == 1 else noun + "s"
    if noun == "drive":
        return f"Also on {count:,} other {word}"
    return f"Also in {count:,} other {word}"


def _empty_search(per_page: int, folder_per_page: int) -> dict:
    return {
        "total": 0,
        "total_fmt": "0",
        "file_label": _noun_label(0, "unique file", "unique files"),
        "page": 1,
        "pages": 1,
        "has_more": False,
        "per_page": per_page,
        "files": [],
        "folder_total": 0,
        "folder_total_fmt": "0",
        "folder_label": _noun_label(0, "folder", "folders"),
        "folder_page": 1,
        "folder_pages": 1,
        "folder_has_more": False,
        "folder_per_page": folder_per_page,
        "folders": [],
    }


def _prefer_rep(new: tuple, old: tuple) -> bool:
    """Prefer the original upload, then a stable path, as the row we show."""
    if new[0] != old[0]:
        return new[0] > old[0]
    return new[1] < old[1]


def _collect_hits(rows, name_matches) -> tuple[dict, dict]:
    reps: dict = {}
    folders: dict[str, set[str]] = {}
    # Directory names repeat on every file beneath them. Remember both the
    # segment verdict and which prefixes of a parent path are folder hits.
    seen: dict[str, bool] = {}
    parent_hits: dict[str, tuple[str, ...]] = {}

    def matches(name: str) -> bool:
        hit = seen.get(name)
        if hit is None:
            hit = name_matches(name)
            seen[name] = hit
        return hit

    def folders_in(parent: str) -> tuple[str, ...]:
        found = parent_hits.get(parent)
        if found is not None:
            return found
        matched: list[str] = []
        if parent:
            start = 0
            while True:
                slash = parent.find("/", start)
                if slash < 0:
                    segment = parent[start:]
                    if segment and matches(segment):
                        matched.append(parent)
                    break
                segment = parent[start:slash]
                if segment and matches(segment):
                    matched.append(parent[:slash])
                start = slash + 1
        found = tuple(matched)
        parent_hits[parent] = found
        return found

    for row in rows:
        raw_path = row["file_path"] or ""
        if "\\" in raw_path:
            raw_path = raw_path.replace("\\", "/")
        slash = raw_path.rfind("/")
        if slash < 0:
            name, parent = raw_path, ""
        else:
            name, parent = raw_path[slash + 1:], raw_path[:slash]
        if name and matches(name):
            key = row["hash"] or f"id:{row['id']}"
            candidate = (
                1 if row["is_original"] else 0,
                raw_path,
                int(row["id"]),
                int(row["size"] or 0),
                row["file_mtime"],
                row["best_taken_at"],
                row["drive_name"] or "",
                row["file_type"] or "",
                key,
            )
            current = reps.get(key)
            if current is None or _prefer_rep(candidate, current):
                reps[key] = candidate
        if not parent:
            continue
        drive = row["drive_name"] or ""
        for folder_path in folders_in(parent):
            known = folders.get(folder_path)
            if known is None:
                folders[folder_path] = {drive}
            else:
                known.add(drive)
    return reps, folders


def _group_sort_key(rep: tuple, sort: str):
    _is_orig, path, _fid, size, mtime, taken, _drive, _ftype, _key = rep
    path_key = path.lower()
    if sort == "size_desc":
        return (size is not None, size or 0, path_key)
    if sort == "size_asc":
        return (size is None, size or 0, path_key)
    if sort == "mtime_desc":
        return (bool(mtime), mtime or "", path_key)
    if sort == "mtime_asc":
        return (not mtime, mtime or "", path_key)
    if sort == "taken_desc":
        return (bool(taken), taken or "", path_key)
    if sort == "taken_asc":
        return (not taken, taken or "", path_key)
    return (path_key,)


def _page_window(items: list, page: int, per_page: int) -> tuple[list, int, int, int, bool]:
    total = len(items)
    pages = max(1, (total + per_page - 1) // per_page) if total else 1
    page = min(max(1, int(page or 1)), pages)
    start = (page - 1) * per_page
    return items[start:start + per_page], total, page, pages, page < pages


def _rep_to_file(rep: tuple) -> dict:
    is_orig, path, fid, size, mtime, taken, drive, ftype, key = rep
    norm = path.replace("\\", "/")
    parent, _, name = norm.rpartition("/")
    return {
        "id": fid,
        "hash": key,
        "drive_name": drive,
        "file_path": path,
        "name": name or norm,
        "parent": parent,
        "size": size,
        "size_fmt": format_size(size),
        "is_original": bool(is_orig),
        "file_type": ftype or "",
        "mime_type": "",
        "file_mtime": mtime,
        "mtime_fmt": _fmt_mtime(mtime),
        "best_taken_at": taken,
        "taken_fmt": _fmt_mtime(taken) if taken else "",
        "taken_at_source": None,
        "created_at": None,
        "upload_path": None,
        "places": [],
        "place_count": 1,
        "other_count": 0,
        "places_label": "",
        "places_more": 0,
        "places_more_fmt": "",
    }


def _folders_from(folders: dict[str, set[str]]) -> list[dict]:
    items = []
    for path, drives in folders.items():
        drive_list = sorted(drive for drive in drives if drive)
        if not drive_list:
            continue
        parent, _, name = path.rpartition("/")
        others = drive_list[1:]
        items.append({
            "name": name or path,
            "path": path,
            "parent": parent,
            "drive_name": drive_list[0],
            "others": others,
            "others_label": _also_label(len(others), "drive"),
        })
    items.sort(key=lambda folder: (folder["path"].lower(), folder["drive_name"].lower()))
    return items


def _attach_places(conn, files: list[dict], *, hide_hidden: bool) -> None:
    hashed = [
        item for item in files
        if item.get("hash") and not str(item["hash"]).startswith("id:")
    ]
    if not hashed:
        return
    hashes = [item["hash"] for item in hashed]
    marks = ",".join("?" * len(hashes))
    # Filter hidden rows in Python. Putting searchable in the SQL makes
    # SQLite scan idx_files_searchable instead of the hash index.
    rows = conn.execute(
        f"SELECT id, hash, drive_name, file_path, searchable FROM files WHERE hash IN ({marks})",
        hashes,
    ).fetchall()
    by_hash: dict[str, list] = {}
    counts: dict[str, int] = {}
    for row in rows:
        if hide_hidden and not row["searchable"]:
            continue
        by_hash.setdefault(row["hash"], []).append(row)
        counts[row["hash"]] = counts.get(row["hash"], 0) + 1
    for item in hashed:
        group = by_hash.get(item["hash"], [])
        group.sort(key=lambda row: ((row["drive_name"] or ""), row["file_path"] or ""))
        others = [row for row in group if int(row["id"]) != item["id"]][:PLACE_PREVIEW]
        total = counts.get(item["hash"], 1)
        other_count = max(total - 1, 0)
        places = []
        for row in others:
            path = row["file_path"] or ""
            norm = path.replace("\\", "/")
            parent, _, name = norm.rpartition("/")
            places.append({
                "id": int(row["id"]),
                "drive_name": row["drive_name"],
                "file_path": path,
                "name": name or norm,
                "parent": parent,
            })
        more = max(other_count - len(places), 0)
        item["places"] = places
        item["place_count"] = total
        item["other_count"] = other_count
        item["places_label"] = _also_label(other_count, "place")
        item["places_more"] = more
        item["places_more_fmt"] = f"{more:,}"


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
    folder_page: int = 1,
    folder_per_page: int = FOLDER_PAGE_SIZE,
) -> dict:
    """Search file names and folder names across every drive.

    Drive is an optional filter, never required. A file matches when its
    own name matches. A folder matches when that directory's name matches,
    so searching for a folder does not list every file inside it. Bytes
    that were backed up more than once come back as one file, and the
    other paths are attached to that row.
    """
    q = (q or "").strip()
    fts = fts_query(q) if q else ""
    if not fts:
        return _empty_search(per_page, folder_per_page)

    conditions = ["f.id IN (SELECT rowid FROM files_fts WHERE files_fts MATCH ?)"]
    params: list = [fts]

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
        exts = [piece.strip().lower().lstrip(".") for piece in ext.split(",") if piece.strip()]
        if exts:
            conditions.append("(" + " OR ".join("f.file_path LIKE ?" for _ in exts) + ")")
            params.extend(f"%.{ext_name}" for ext_name in exts)
    if year and str(year).isdigit() and len(str(year)) == 4:
        conditions.append("substr(f.best_taken_at, 1, 4) = ?")
        params.append(str(year))

    where = " WHERE " + " AND ".join(conditions)
    select_sql = (
        "SELECT f.id, f.hash, f.drive_name, f.file_path, f.size, f.is_original, "
        "f.file_type, f.file_mtime, f.best_taken_at "
        f"FROM files f{where}"
    )
    name_matches = _name_matcher(fts)

    conn = connect(readonly=True)
    try:
        try:
            rows = conn.execute(select_sql, params)
            reps, folder_map = _collect_hits(rows, name_matches)
        except sqlite3.OperationalError as exc:
            # Malformed user input should show no results, not a 500 page.
            if not str(exc).startswith(_FTS_QUERY_ERRORS):
                raise
            reps, folder_map = {}, {}

        ordered = sorted(
            reps.values(),
            key=lambda rep: _group_sort_key(rep, sort),
            reverse=sort in _DESC_SORTS,
        )
        file_slice, total, page, pages, has_more = _page_window(ordered, page, per_page)
        files = [_rep_to_file(rep) for rep in file_slice]
        _attach_places(conn, files, hide_hidden=hide_hidden)

        folder_items = _folders_from(folder_map)
        folder_slice, folder_total, folder_page, folder_pages, folder_has_more = _page_window(
            folder_items, folder_page, folder_per_page
        )
    finally:
        conn.close()

    return {
        "total": total,
        "total_fmt": f"{total:,}",
        "file_label": _noun_label(total, "unique file", "unique files"),
        "page": page,
        "pages": pages,
        "has_more": has_more,
        "per_page": per_page,
        "files": files,
        "folder_total": folder_total,
        "folder_total_fmt": f"{folder_total:,}",
        "folder_label": _noun_label(folder_total, "folder", "folders"),
        "folder_page": folder_page,
        "folder_pages": folder_pages,
        "folder_has_more": folder_has_more,
        "folder_per_page": folder_per_page,
        "folders": folder_slice,
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
