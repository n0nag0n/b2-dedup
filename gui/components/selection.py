"""Simple checkbox selection bound to the download basket.

Uses native st.checkbox (not data_editor). Basket membership is updated only
via on_change callbacks — never by "every unchecked row means remove" — so
Streamlit's multi-tab re-execution cannot silently empty the basket.

Keys include basket_widget_gen so Clear remounts every checkbox unchecked.
"""
from __future__ import annotations

import pandas as pd
import streamlit as st

from gui.state import (
    add_file_to_basket,
    remove_file_from_basket,
    add_folder_to_basket,
    is_file_in_basket,
)


def render_folder_row(drive_name: str, folder_path: str, name: str, nav_callback) -> None:
    """One folder line: add-all button + navigate button."""
    cols = st.columns([0.7, 8])

    if cols[0].button(
        "＋",
        key=f"add_dir_{drive_name}_{folder_path}",
        help="Add every file in this folder to the basket",
    ):
        added = add_folder_to_basket(drive_name, folder_path)
        # Persist in session_state (not st.toast). Basket bar shows st.success after rerun.
        if added:
            st.session_state.basket_notice = f"Added {added} file(s) from 📁 {name}"
        else:
            st.session_state.basket_notice = (
                f"📁 {name} — all files already in the basket"
            )
        st.rerun()

    if cols[1].button(f"📁 {name}", key=f"nav_{drive_name}_{folder_path}"):
        nav_callback(folder_path)


def _on_file_checkbox(fid: int, key: str) -> None:
    """Streamlit callback: widget value is already updated in session_state."""
    if st.session_state.get(key):
        add_file_to_basket(fid)
    else:
        remove_file_from_basket(fid)


def render_file_table(
    df: pd.DataFrame,
    *,
    name_col: str = "name",
    show_drive: bool = False,
    key_prefix: str = "file",
) -> pd.DataFrame:
    """Render a simple file list with a checkbox per row.

    Check → add to basket. Uncheck → remove from basket.
    Returns the subset of *df* currently in the basket (for group actions).
    """
    if df.empty:
        return df.iloc[0:0]

    gen = st.session_state.basket_widget_gen
    selected_indices: list[int] = []

    # Header
    if show_drive:
        h = st.columns([0.4, 1.4, 4.5, 1.1, 1.6, 1.0])
        h[1].caption("Drive")
        h[2].caption("Path")
    else:
        h = st.columns([0.4, 5.5, 1.1, 1.6, 1.0])
        h[1].caption("Name")
    h[-3].caption("Size")
    h[-2].caption("Modified")
    h[-1].caption("Type")

    for i, (_, row) in enumerate(df.iterrows()):
        fid = int(row["id"])
        key = f"bfile_{key_prefix}_{gen}_{fid}"
        desired = is_file_in_basket(fid)

        # Keep widget state aligned with the basket (handles folder-add,
        # Clear, remove-from-list, and the other tab showing the same file).
        if key not in st.session_state or st.session_state[key] != desired:
            st.session_state[key] = desired

        if show_drive:
            cols = st.columns([0.4, 1.4, 4.5, 1.1, 1.6, 1.0])
            path_col = cols[2]
            drive_col = cols[1]
        else:
            cols = st.columns([0.4, 5.5, 1.1, 1.6, 1.0])
            path_col = cols[1]
            drive_col = None

        cols[0].checkbox(
            "select",
            key=key,
            label_visibility="collapsed",
            on_change=_on_file_checkbox,
            args=(fid, key),
        )
        if is_file_in_basket(fid):
            selected_indices.append(i)

        label = str(row[name_col]) if name_col in row.index else str(row.get("file_path", ""))
        path_col.markdown(label)
        if drive_col is not None:
            drive_col.caption(str(row.get("drive_name", "")))

        size_txt = row["size_fmt"] if "size_fmt" in row.index else ""
        cols[-3].caption(str(size_txt))

        mtime = row["file_mtime"] if "file_mtime" in row.index else None
        if mtime is not None and pd.notna(mtime):
            try:
                cols[-2].caption(pd.Timestamp(mtime).strftime("%Y-%m-%d %H:%M"))
            except Exception:
                cols[-2].caption("")
        else:
            cols[-2].caption("")

        ftype = row["file_type"] if "file_type" in row.index else ""
        cols[-1].caption(str(ftype) if pd.notna(ftype) else "")

    if not selected_indices:
        return df.iloc[0:0]
    return df.iloc[selected_indices]
