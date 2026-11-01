"""Per-browser restore set stored in SQLite (cookies cannot hold folder-sized ID lists)."""
from __future__ import annotations

from datetime import datetime, timezone

from core.catalog import format_size, get_files_by_ids, get_selection_size, resolve_folder_ids
from core.db import connect


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def list_ids(session_key: str) -> list[int]:
    conn = connect(readonly=True)
    try:
        rows = conn.execute(
            "SELECT file_id FROM restore_set WHERE session_key = ? ORDER BY file_id",
            (session_key,),
        ).fetchall()
    finally:
        conn.close()
    return [int(r["file_id"]) for r in rows]


def count(session_key: str) -> int:
    conn = connect(readonly=True)
    try:
        n = conn.execute(
            "SELECT COUNT(*) FROM restore_set WHERE session_key = ?", (session_key,)
        ).fetchone()[0]
    finally:
        conn.close()
    return int(n or 0)


def add_ids(session_key: str, ids: list[int]) -> int:
    if not ids:
        return 0
    added_at = _now()
    conn = connect()
    try:
        before = conn.execute(
            "SELECT COUNT(*) FROM restore_set WHERE session_key = ?", (session_key,)
        ).fetchone()[0]
        conn.executemany(
            "INSERT OR IGNORE INTO restore_set (session_key, file_id, added_at) VALUES (?, ?, ?)",
            [(session_key, int(i), added_at) for i in ids],
        )
        conn.commit()
        after = conn.execute(
            "SELECT COUNT(*) FROM restore_set WHERE session_key = ?", (session_key,)
        ).fetchone()[0]
    finally:
        conn.close()
    return int(after) - int(before)


def add_folder(session_key: str, drive: str, folder_path: str) -> int:
    return add_ids(session_key, resolve_folder_ids(drive, folder_path))


def remove_id(session_key: str, file_id: int) -> None:
    conn = connect()
    try:
        conn.execute(
            "DELETE FROM restore_set WHERE session_key = ? AND file_id = ?",
            (session_key, int(file_id)),
        )
        conn.commit()
    finally:
        conn.close()


def clear(session_key: str) -> None:
    conn = connect()
    try:
        conn.execute("DELETE FROM restore_set WHERE session_key = ?", (session_key,))
        conn.commit()
    finally:
        conn.close()


def contains(session_key: str, file_id: int) -> bool:
    conn = connect(readonly=True)
    try:
        row = conn.execute(
            "SELECT 1 FROM restore_set WHERE session_key = ? AND file_id = ?",
            (session_key, int(file_id)),
        ).fetchone()
    finally:
        conn.close()
    return row is not None


def summary(session_key: str, preview_limit: int = 40) -> dict:
    ids = list_ids(session_key)
    size = get_selection_size(ids)
    preview_ids = ids[:preview_limit]
    return {
        "count": len(ids),
        "count_fmt": f"{len(ids):,}",
        "size": size,
        "size_fmt": format_size(size),
        "ids": ids,
        "preview": get_files_by_ids(preview_ids),
        "preview_truncated": len(ids) > preview_limit,
    }
