"""Browse Folders tab — directory tree navigation with basket integration."""
from pathlib import Path

import pandas as pd
import streamlit as st

from gui.db import get_db_connection, format_size
from gui.components.group_actions import render_group_actions
from gui.components.selection import render_folder_row, render_file_table

ROWS_PER_PAGE = 50


def render_browse_tab(selected_drive: str, selected_group_name: str, group_map: dict[str, int]):
    if selected_drive == "All Drives":
        st.warning("Please select a specific Drive from the sidebar to browse.")
        return

    if 'browse_path' not in st.session_state:
        st.session_state.browse_path = ""
    if 'browse_page' not in st.session_state:
        st.session_state.browse_page = 1

    _render_nav_bar()

    current_prefix = st.session_state.browse_path
    if current_prefix and not current_prefix.endswith('/'):
        current_prefix += '/'

    dirs = _list_subdirs(selected_drive, current_prefix)
    f_df, total_browse_files = _query_files(selected_drive, current_prefix)

    _render_dirs(dirs, current_prefix, selected_drive)
    _render_files(f_df, current_prefix, total_browse_files, group_map)


# ── private helpers ──────────────────────────────────────────────────────────

def _render_nav_bar():
    nav_cols = st.columns([1, 8])
    if st.session_state.browse_path:
        with nav_cols[0]:
            if st.button("⬆ Up", key="btn_up"):
                parent = str(Path(st.session_state.browse_path).parent)
                st.session_state.browse_path = "" if parent == "." else parent
                st.session_state.browse_page = 1
                st.rerun()
    with nav_cols[1]:
        st.code(f"/{st.session_state.browse_path}", language="text")


def _list_subdirs(drive: str, prefix: str) -> list[str]:
    dirs = []
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        seek_path = prefix
        for _ in range(500):
            row = cur.execute(
                "SELECT file_path FROM files WHERE drive_name = ? AND file_path > ? AND file_path LIKE ? ORDER BY file_path ASC LIMIT 1",
                (drive, seek_path, f"{prefix}%")
            ).fetchone()
            if not row:
                break
            full_path = row[0]
            rel_path = full_path[len(prefix):]
            if '/' in rel_path:
                subdir_name = rel_path.split('/')[0]
                dirs.append(subdir_name)
                seek_path = f"{prefix}{subdir_name}\uffff"
            else:
                seek_path = full_path
    except Exception as e:
        st.error(f"Error listing directories: {e}")
    finally:
        conn.close()
    return dirs


def _query_files(drive: str, prefix: str) -> tuple[pd.DataFrame, int]:
    params = [drive, f"{prefix}%", f"{prefix}%/%"]
    query = """
        SELECT f.id, f.drive_name, f.file_path, f.size, f.created_at, f.is_original,
               f.file_type, f.mime_type, f.file_mtime, f.file_atime, f.file_ctime
        FROM files f
        WHERE f.drive_name = ? AND f.file_path LIKE ? AND f.file_path NOT LIKE ?
    """
    conn = get_db_connection()
    try:
        total = conn.execute(
            "SELECT COUNT(*) FROM files f WHERE f.drive_name = ? AND f.file_path LIKE ? AND f.file_path NOT LIKE ?",
            params
        ).fetchone()[0]
        offset = (st.session_state.browse_page - 1) * ROWS_PER_PAGE
        df = pd.read_sql(
            query + f" ORDER BY f.file_path LIMIT {ROWS_PER_PAGE} OFFSET {offset}",
            conn, params=params
        )
    finally:
        conn.close()
    return df, total


def _render_dirs(dirs: list[str], prefix: str, drive: str):
    if st.session_state.browse_page != 1 or not dirs:
        return
    st.divider()
    st.caption("Folders — click ＋ to add every file inside to the basket; click the name to open.")

    def _navigate(folder_path: str):
        st.session_state.browse_path = folder_path
        st.session_state.browse_page = 1
        st.rerun()

    for d in dirs:
        folder_path = f"{prefix}{d}"
        render_folder_row(drive, folder_path, d, _navigate)


def _render_files(
    f_df: pd.DataFrame,
    prefix: str,
    total: int,
    group_map: dict[str, int],
):
    if f_df.empty:
        st.info("No files in this folder")
        _render_pagination(total)
        return

    f_df = f_df.copy()
    f_df['size_fmt'] = f_df['size'].apply(format_size)
    f_df['name'] = f_df['file_path'].apply(lambda x: x[len(prefix):])
    for col in ['file_mtime', 'created_at', 'file_atime', 'file_ctime']:
        if col in f_df.columns:
            f_df[col] = pd.to_datetime(f_df[col], errors='coerce')

    st.divider()
    st.caption("Files — check a box to add it to the basket.")
    selected = render_file_table(f_df, name_col="name", show_drive=False, key_prefix="browse")

    if not selected.empty:
        st.divider()
        render_group_actions(selected, group_map, "All Files", "browse")

    _render_pagination(total)


def _render_pagination(total: int):
    if total <= 0:
        return
    col_bp1, col_bp2, col_bp3 = st.columns([1, 2, 1])
    with col_bp1:
        if st.session_state.browse_page > 1:
            if st.button("Prev"):
                st.session_state.browse_page -= 1
                st.rerun()
    with col_bp2:
        total_pages = max(1, (total + ROWS_PER_PAGE - 1) // ROWS_PER_PAGE)
        st.write(f"Page {st.session_state.browse_page} of {total_pages} ({total:,} files)")
    with col_bp3:
        if total > st.session_state.browse_page * ROWS_PER_PAGE:
            if st.button("Next"):
                st.session_state.browse_page += 1
                st.rerun()
