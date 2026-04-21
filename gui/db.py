"""Database access helpers for the GUI layer.

All raw SQL that is shared across multiple tabs/components lives here.
Tab-specific one-off queries can stay inline, but anything reused should
graduate to this module.
"""
import sqlite3
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import b2_dedup


def get_db_connection() -> sqlite3.Connection:
    """Open and return a new SQLite connection. Caller must call conn.close()."""
    return sqlite3.connect(b2_dedup.DB_PATH)


def format_size(size_bytes: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


def get_drives() -> list[str]:
    conn = get_db_connection()
    try:
        cur = conn.execute("SELECT DISTINCT drive_name FROM files ORDER BY drive_name")
        return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def get_groups() -> dict[str, int]:
    """Return {group_name: group_id} for all groups."""
    conn = get_db_connection()
    try:
        rows = conn.execute("SELECT id, name FROM groups ORDER BY name").fetchall()
        return {name: gid for gid, name in rows}
    finally:
        conn.close()


def get_file_types() -> list[str]:
    conn = get_db_connection()
    try:
        rows = conn.execute(
            "SELECT DISTINCT file_type FROM files WHERE file_type IS NOT NULL ORDER BY file_type"
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


def get_basket_file_ids(basket_file_ids: set, basket_folder_paths: set) -> list[int]:
    """Resolve basket (file IDs + folder paths) to a flat list of file IDs."""
    ids = set(basket_file_ids)
    if basket_folder_paths:
        conn = get_db_connection()
        try:
            for drive_name, folder_path in basket_folder_paths:
                path = folder_path if folder_path.endswith('/') else folder_path + '/'
                found = conn.execute(
                    "SELECT id FROM files WHERE drive_name = ? AND file_path LIKE ?",
                    (drive_name, f"{path}%")
                ).fetchall()
                ids.update(f[0] for f in found)
        finally:
            conn.close()
    return list(ids)


def get_selection_size(all_ids: list[int]) -> int:
    """Return total uncompressed bytes for a list of file IDs."""
    if not all_ids:
        return 0
    conn = get_db_connection()
    try:
        placeholders = ",".join("?" * len(all_ids))
        row = conn.execute(
            f"SELECT SUM(size) FROM files WHERE id IN ({placeholders})", all_ids
        ).fetchone()
        return row[0] or 0
    finally:
        conn.close()


def resolve_folder_to_ids(drive_name: str, folder_path: str) -> list[int]:
    """Return all file IDs under a folder path for a given drive."""
    path = folder_path if folder_path.endswith('/') else folder_path + '/'
    conn = get_db_connection()
    try:
        found = conn.execute(
            "SELECT id FROM files WHERE drive_name = ? AND file_path LIKE ?",
            (drive_name, f"{path}%")
        ).fetchall()
        return [f[0] for f in found]
    finally:
        conn.close()


def delete_drive(drive_name: str) -> None:
    """Delete all records for a given drive from the local database."""
    conn = get_db_connection()
    try:
        conn.execute("DELETE FROM files WHERE drive_name = ?", (drive_name,))
        conn.commit()
    finally:
        conn.close()

