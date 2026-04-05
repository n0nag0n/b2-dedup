"""Browse Folders tab — directory tree navigation with basket integration."""
from pathlib import Path

import pandas as pd
import streamlit as st

from gui.db import get_db_connection, format_size
from gui.components.group_actions import render_group_actions

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

    dirs_df = _build_dirs_df(dirs, current_prefix, selected_drive)
    _render_dirs(dirs_df)
    _render_files(f_df, current_prefix, total_browse_files, selected_group_name, group_map)


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


def _build_dirs_df(dirs: list[str], prefix: str, drive: str) -> pd.DataFrame:
    if st.session_state.browse_page == 1 and dirs:
        return pd.DataFrame([{
            'is_dir': True,
            'name': d,
            'file_path': f"{prefix}{d}",
            'drive_name': drive,
            'id': -1,
        } for d in dirs])
    return pd.DataFrame()


def _render_dirs(dirs_df: pd.DataFrame):
    if dirs_df.empty:
        return
    st.divider()
    for _, row in dirs_df.iterrows():
        folder_key = (row['drive_name'], row['file_path'])
        widget_key = f"dir_sel_{row['drive_name']}_{row['file_path']}"
        nav_key = f"nav_{row['drive_name']}_{row['file_path']}"

        if widget_key not in st.session_state:
            st.session_state[widget_key] = folder_key in st.session_state.basket_folder_paths

        dcols = st.columns([0.5, 8])
        checked = dcols[0].checkbox("select dir", key=widget_key, label_visibility="collapsed")
        if checked:
            st.session_state.basket_folder_paths.add(folder_key)
        else:
            st.session_state.basket_folder_paths.discard(folder_key)

        if dcols[1].button(f"📁 {row['name']}", key=nav_key):
            st.session_state.browse_path = row['file_path']
            st.session_state.browse_page = 1
            st.rerun()


def _render_files(
    f_df: pd.DataFrame,
    prefix: str,
    total: int,
    selected_group_name: str,
    group_map: dict[str, int],
):
    if f_df.empty:
        if not st.session_state.get('_dirs_rendered'):
            st.info("No files in this folder")
        return

    f_df['size_fmt'] = f_df['size'].apply(format_size)
    f_df['name'] = f_df['file_path'].apply(lambda x: x[len(prefix):])
    for col in ['file_mtime', 'created_at', 'file_atime', 'file_ctime']:
        if col in f_df.columns:
            f_df[col] = pd.to_datetime(f_df[col], errors='coerce')

    f_df['selected'] = f_df['id'].isin(st.session_state.basket_file_ids)

    browse_cols = ['selected', 'name', 'size_fmt', 'file_mtime', 'created_at',
                   'file_type', 'is_original', 'id', 'file_path', 'drive_name']
    browse_cols = [c for c in browse_cols if c in f_df.columns]

    st.divider()
    browse_edited = st.data_editor(
        f_df[browse_cols],
        column_config={
            "selected": st.column_config.CheckboxColumn("Select", default=False),
            "name": "Name",
            "size_fmt": "Size",
            "file_mtime": st.column_config.DatetimeColumn("Modified", format="YYYY-MM-DD HH:mm"),
            "created_at": st.column_config.DatetimeColumn("Added", format="YYYY-MM-DD HH:mm"),
            "file_type": "Type",
            "is_original": "Orig?",
            "id": None,
            "file_path": None,
            "drive_name": None,
            "size": None,
            "mime_type": None,
        },
        hide_index=True,
        disabled=[c for c in browse_cols if c != "selected"],
        key=f"browse_editor_{st.session_state.browse_page}"
    )

    for _, row in browse_edited.iterrows():
        fid = int(row['id'])
        if row['selected']:
            st.session_state.basket_file_ids.add(fid)
        else:
            st.session_state.basket_file_ids.discard(fid)

    browse_files_selected = browse_edited[browse_edited['selected']]
    if not browse_files_selected.empty:
        st.divider()
        render_group_actions(browse_files_selected, group_map, "All Files", "browse")

    # Pagination
    col_bp1, col_bp2, col_bp3 = st.columns([1, 2, 1])
    with col_bp1:
        if st.session_state.browse_page > 1:
            if st.button("Prev"):
                st.session_state.browse_page -= 1
                st.rerun()
    with col_bp2:
        total_pages = (total // ROWS_PER_PAGE) + 1
        st.write(f"Page {st.session_state.browse_page} of {total_pages}")
    with col_bp3:
        if total > st.session_state.browse_page * ROWS_PER_PAGE:
            if st.button("Next"):
                st.session_state.browse_page += 1
                st.rerun()
