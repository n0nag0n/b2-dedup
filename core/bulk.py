"""Checked rows on the search page: download them, or remember them for a move."""
from __future__ import annotations

import json
from datetime import datetime, timezone

from core.catalog import get_files_by_ids, resolve_folder_ids
from core.config import normalize_drive_name
from core.db import connect
from core.download import forced_to_stream, selection_error
from core.relocate import RelocateError, clean_catalog_path

MAX_CHECKED_FILES = 20000
MAX_CHECKED_FOLDERS = 2000


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_file_ids(values) -> list[int]:
    ids: list[int] = []
    seen: set[int] = set()
    for raw in values or []:
        text = str(raw).strip()
        if not text.isdigit():
            continue
        number = int(text)
        if number <= 0 or number in seen:
            continue
        seen.add(number)
        ids.append(number)
    return ids


def parse_folders(values) -> list[tuple[str, str]] | None:
    """Folder checkboxes are ``drive\\tpath``. None means one of them is invalid."""
    folders: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for raw in values or []:
        drive, sep, path = str(raw).partition("\t")
        if not sep:
            return None
        name = normalize_drive_name(drive)
        if not name:
            return None
        try:
            cleaned = clean_catalog_path(path)
        except RelocateError:
            return None
        if not cleaned or (name, cleaned) in seen:
            if not cleaned:
                return None
            continue
        seen.add((name, cleaned))
        folders.append((name, cleaned))
    return folders


def selection_label(file_count: int, folder_count: int) -> str:
    parts = []
    if file_count:
        parts.append(f"{file_count:,} file" + ("s" if file_count != 1 else ""))
    if folder_count:
        parts.append(f"{folder_count:,} folder" + ("s" if folder_count != 1 else ""))
    return " and ".join(parts)


def collect_records(file_ids: list[int], folders: list[tuple[str, str]]) -> list[dict]:
    """Catalog rows for the checkboxes, with folders expanded and duplicates dropped."""
    ordered: list[int] = []
    seen: set[int] = set()
    for drive, path in folders:
        for file_id in resolve_folder_ids(drive, path):
            if file_id in seen:
                continue
            seen.add(file_id)
            ordered.append(file_id)
    for file_id in file_ids:
        if file_id in seen:
            continue
        seen.add(file_id)
        ordered.append(file_id)
    if not ordered:
        return []
    by_id = {int(row["id"]): row for row in get_files_by_ids(ordered)}
    return [by_id[file_id] for file_id in ordered if file_id in by_id]


def download_mode(records: list[dict]) -> dict:
    """How Download should hand the selection to the browser.

    One file uses the single-file download. A modest selection streams as a
    ZIP. A large one is added to the restore set so the restore page can
    stream it with the size warning already on that page.
    """
    ids = [int(row["id"]) for row in records]
    if len(ids) == 1:
        return {"mode": "file", "ids": ids, "size": int(records[0].get("size") or 0)}
    size = sum(int(row.get("size") or 0) for row in records)
    problem = selection_error(len(ids), size)
    if problem:
        return {"mode": "rejected", "ids": ids, "size": size, "error": problem}
    if forced_to_stream(size):
        return {"mode": "restore", "ids": ids, "size": size}
    return {"mode": "zip", "ids": ids, "size": size}


def save_pick(session_key: str, file_ids: list[int], folders: list[tuple[str, str]]) -> None:
    payload = json.dumps({
        "file_ids": file_ids,
        "folders": [[drive, path] for drive, path in folders],
    })
    conn = connect()
    try:
        conn.execute(
            "INSERT INTO bulk_pick (session_key, payload, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(session_key) DO UPDATE SET "
            "payload = excluded.payload, updated_at = excluded.updated_at",
            (session_key, payload, _now()),
        )
        conn.commit()
    finally:
        conn.close()


def load_pick(session_key: str) -> dict | None:
    conn = connect(readonly=True)
    try:
        row = conn.execute(
            "SELECT payload FROM bulk_pick WHERE session_key = ?",
            (session_key,),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    try:
        data = json.loads(row["payload"])
    except json.JSONDecodeError:
        return None
    file_ids: list[int] = []
    for raw in data.get("file_ids") or []:
        try:
            number = int(raw)
        except (TypeError, ValueError):
            continue
        if number > 0:
            file_ids.append(number)
    folders: list[tuple[str, str]] = []
    for item in data.get("folders") or []:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            folders.append((str(item[0]), str(item[1])))
    return {"file_ids": file_ids, "folders": folders}
