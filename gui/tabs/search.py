"""Search Files tab — full-text + filter search with basket integration."""
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

from gui.db import get_db_connection, get_file_types, format_size
from gui.components.group_actions import render_group_actions

ROWS_PER_PAGE = 50

_SORT_MAP = {
    "Date Modified (Newest)": "f.file_mtime DESC",
    "Date Modified (Oldest)": "f.file_mtime ASC",
    "Date Added (Newest)": "f.created_at DESC",
    "Date Added (Oldest)": "f.created_at ASC",
    "Size (Largest)": "f.size DESC",
    "Size (Smallest)": "f.size ASC",
    "Type (A-Z)": "f.file_type ASC",
    "Type (Z-A)": "f.file_type DESC",
}


def render_search_tab(selected_drive: str, selected_group_name: str, group_map: dict[str, int]):
    col1, col2 = st.columns([3, 1])
    with col1:
        search_query = st.text_input("Search files (Path/Name)", placeholder="e.g. DCIM or .jpg")
    with col2:
        sort_order = st.selectbox("Sort by", list(_SORT_MAP.keys()))

    filters = _render_advanced_filters()

    if 'page' not in st.session_state:
        st.session_state.page = 1

    query, params = _build_query(selected_drive, selected_group_name, group_map, search_query, filters)

    conn = get_db_connection()
    total_rows = conn.execute(f"SELECT COUNT(*) FROM ({query})", params).fetchone()[0]
    conn.close()

    offset = (st.session_state.page - 1) * ROWS_PER_PAGE
    full_query = query + f" ORDER BY {_SORT_MAP[sort_order]} LIMIT {ROWS_PER_PAGE} OFFSET {offset}"

    conn = get_db_connection()
    df = pd.read_sql(full_query, conn, params=params)
    conn.close()

    st.markdown(f"**Found {total_rows:,} files**")

    selected_rows = _render_results(df)
    _render_pagination(total_rows)

    if not selected_rows.empty:
        st.divider()
        render_group_actions(selected_rows, group_map, selected_group_name, "search")


# ── private helpers ──────────────────────────────────────────────────────────

def _render_advanced_filters() -> dict:
    types = get_file_types()
    with st.expander("Advanced Filters"):
        af_col1, af_col2, af_col3, af_col4 = st.columns(4)
        with af_col1:
            filter_origin = st.selectbox("Original / Duplicate", ["All", "Originals Only", "Duplicates Only"])
        with af_col2:
            filter_date_col = st.selectbox("Filter Date By", ["Date Added", "File Modified"])
        with af_col3:
            filter_period = st.selectbox(
                "Time Period", ["All Time", "Last 24 Hours", "Last 7 Days", "Last 30 Days", "Custom Range"]
            )
        with af_col4:
            filter_file_type = st.selectbox("File Category", ["All Types"] + types)

        filter_date_range = []
        if filter_period == "Custom Range":
            filter_date_range = st.date_input("Select Date Range", [])

        st.caption("Extensions Filter")
        filter_ext = st.text_input("File Extensions (comma separated)", placeholder="e.g. jpg, png, pdf")

    return {
        "origin": filter_origin,
        "date_col": filter_date_col,
        "period": filter_period,
        "file_type": filter_file_type,
        "date_range": filter_date_range,
        "ext": filter_ext,
    }


def _build_query(
    selected_drive: str,
    selected_group_name: str,
    group_map: dict[str, int],
    search_query: str,
    filters: dict,
) -> tuple[str, list]:
    base = """
        SELECT f.id, f.drive_name, f.file_path, f.size, f.created_at, f.is_original,
               f.file_type, f.mime_type, f.file_mtime, f.file_atime, f.file_ctime,
               EXISTS(SELECT 1 FROM group_members gm WHERE gm.file_id = f.id) as has_group
        FROM files f
    """
    params = []
    conditions = []

    if selected_drive != "All Drives":
        conditions.append("f.drive_name = ?")
        params.append(selected_drive)

    if filters["file_type"] != "All Types":
        conditions.append("f.file_type = ?")
        params.append(filters["file_type"])

    if selected_group_name != "All Files":
        group_id = group_map[selected_group_name]
        base += " JOIN group_members gm_filter ON f.id = gm_filter.file_id"
        conditions.append("gm_filter.group_id = ?")
        params.append(group_id)

    if search_query:
        conn = get_db_connection()
        fts_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='files_fts'"
        ).fetchone()
        conn.close()
        if fts_exists:
            base += " JOIN files_fts fts ON f.id = fts.rowid"
            conditions.append("files_fts MATCH ?")
            special = set('*"')
            fts_q = search_query if any(c in search_query for c in special) else f'"{search_query}" *'
            params.append(fts_q)
        else:
            conditions.append("f.file_path LIKE ?")
            params.append(f"%{search_query}%")

    if filters["origin"] == "Originals Only":
        conditions.append("f.is_original = 1")
    elif filters["origin"] == "Duplicates Only":
        conditions.append("f.is_original = 0")

    date_col = "f.created_at" if filters["date_col"] == "Date Added" else "f.file_mtime"
    period = filters["period"]
    if period != "All Time":
        now = datetime.now()
        if period == "Last 24 Hours":
            conditions.append(f"{date_col} >= ?")
            params.append((now - timedelta(days=1)).isoformat())
        elif period == "Last 7 Days":
            conditions.append(f"{date_col} >= ?")
            params.append((now - timedelta(days=7)).isoformat())
        elif period == "Last 30 Days":
            conditions.append(f"{date_col} >= ?")
            params.append((now - timedelta(days=30)).isoformat())
        elif period == "Custom Range" and len(filters["date_range"]) == 2:
            start_date, end_date = filters["date_range"]
            conditions.append(f"{date_col} >= ? AND {date_col} <= ?")
            params.append(start_date.isoformat())
            params.append(end_date.isoformat() + "T23:59:59")

    if filters["ext"]:
        exts = [e.strip().lower().lstrip('.') for e in filters["ext"].split(',') if e.strip()]
        if exts:
            ext_conditions = [f"f.file_path LIKE ?" for _ in exts]
            for e in exts:
                params.append(f"%.{e}")
            conditions.append(f"({' OR '.join(ext_conditions)})")

    if conditions:
        base += " WHERE " + " AND ".join(conditions)

    return base, params


def _render_results(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    df['size_fmt'] = df['size'].apply(format_size)
    for col in ['file_mtime', 'created_at', 'file_atime', 'file_ctime']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    df['selected'] = df['id'].isin(st.session_state.basket_file_ids)

    cols_order = ['selected', 'id', 'drive_name', 'file_path', 'size_fmt', 'file_mtime',
                  'created_at', 'file_atime', 'file_type', 'is_original', 'mime_type',
                  'file_ctime', 'size', 'has_group']
    cols_order = [c for c in cols_order if c in df.columns]

    edited_df = st.data_editor(
        df[cols_order],
        column_config={
            "selected": st.column_config.CheckboxColumn("Select", default=False),
            "drive_name": "Drive",
            "file_path": "Path",
            "size_fmt": "Size",
            "file_mtime": st.column_config.DatetimeColumn("Modified", format="YYYY-MM-DD HH:mm"),
            "created_at": st.column_config.DatetimeColumn("Added", format="YYYY-MM-DD HH:mm"),
            "file_atime": st.column_config.DatetimeColumn("Accessed", format="YYYY-MM-DD HH:mm"),
            "file_ctime": st.column_config.DatetimeColumn("Created", format="YYYY-MM-DD HH:mm"),
            "is_original": "Orig?",
            "file_type": "Type",
            "mime_type": "MIME",
            "id": None,
            "size": None,
            "has_group": None,
        },
        hide_index=True,
        disabled=[c for c in cols_order if c != "selected"],
        key=f"editor_{st.session_state.page}"
    )

    for _, row in edited_df.iterrows():
        fid = int(row['id'])
        if row['selected']:
            st.session_state.basket_file_ids.add(fid)
        else:
            st.session_state.basket_file_ids.discard(fid)

    return edited_df[edited_df['selected']]


def _render_pagination(total_rows: int):
    st.divider()
    col_p1, col_p2, col_p3 = st.columns([1, 2, 1])
    with col_p1:
        if st.session_state.page > 1:
            if st.button("Previous Page"):
                st.session_state.page -= 1
                st.rerun()
    with col_p2:
        total_pages = (total_rows // ROWS_PER_PAGE) + 1
        st.markdown(
            f"<p style='text-align: center'>Page {st.session_state.page} of {total_pages}</p>",
            unsafe_allow_html=True
        )
    with col_p3:
        if total_rows > st.session_state.page * ROWS_PER_PAGE:
            if st.button("Next Page"):
                st.session_state.page += 1
                st.rerun()
