"""Group add/remove actions rendered below file listings."""
import sqlite3
import pandas as pd
import streamlit as st
from datetime import datetime

from gui.db import get_db_connection, resolve_folder_to_ids


def resolve_selection_to_ids(selected_rows: pd.DataFrame) -> list[int]:
    """Resolve a DataFrame selection (may include directory rows) to file IDs."""
    ids = set()
    conn = get_db_connection()
    try:
        if 'is_dir' in selected_rows.columns:
            is_dir_mask = selected_rows['is_dir'].fillna(False).astype(bool)
            files = selected_rows[~is_dir_mask]
            dirs = selected_rows[is_dir_mask]
        else:
            files = selected_rows
            dirs = pd.DataFrame()

        if not files.empty and 'id' in files.columns:
            valid_ids = files['id'].dropna()
            valid_ids = valid_ids[valid_ids != -1]
            ids.update(valid_ids.astype(int).tolist())

        if not dirs.empty:
            for _, row in dirs.iterrows():
                d_drive = row.get('drive_name')
                d_path = row.get('file_path')
                if not d_drive or not d_path:
                    continue
                ids.update(resolve_folder_to_ids(d_drive, d_path))
    except Exception as e:
        st.error(f"Error resolving selection: {e}")
    finally:
        conn.close()
    return list(ids)


def render_group_actions(
    selected_rows: pd.DataFrame,
    group_map: dict[str, int],
    current_group_name: str,
    key_suffix: str,
):
    """Render Group Add/Remove actions for the current-view selection."""
    if selected_rows.empty:
        return
    col_a, col_b = st.columns(2)
    with col_a:
        if group_map:
            cols = st.columns([2, 1])
            target_group = cols[0].selectbox(
                "Add to Group", list(group_map.keys()), key=f"add_grp_{key_suffix}"
            )
            if cols[1].button("Add", key=f"btn_add_{key_suffix}"):
                all_ids = resolve_selection_to_ids(selected_rows)
                if not all_ids:
                    st.warning("No files found in selection.")
                else:
                    conn = get_db_connection()
                    gid = group_map[target_group]
                    count = 0
                    for fid in all_ids:
                        try:
                            conn.execute(
                                "INSERT INTO group_members (group_id, file_id, added_at) VALUES (?, ?, ?)",
                                (gid, fid, datetime.now().isoformat())
                            )
                            count += 1
                        except sqlite3.IntegrityError:
                            pass
                    conn.commit()
                    conn.close()
                    st.success(f"Added {count} files to '{target_group}'")
    with col_b:
        if current_group_name != "All Files":
            if st.button(f"Remove from '{current_group_name}'", key=f"btn_rem_{key_suffix}"):
                all_ids = resolve_selection_to_ids(selected_rows)
                if all_ids:
                    conn = get_db_connection()
                    gid = group_map[current_group_name]
                    ids = tuple(all_ids)
                    if len(ids) == 1:
                        conn.execute(
                            "DELETE FROM group_members WHERE group_id = ? AND file_id = ?",
                            (gid, ids[0])
                        )
                    else:
                        conn.execute(
                            f"DELETE FROM group_members WHERE group_id = ? AND file_id IN {ids}",
                            (gid,)
                        )
                    conn.commit()
                    conn.close()
                    st.success("Removed files from group")
                    st.rerun()
