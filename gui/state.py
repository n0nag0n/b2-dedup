"""Streamlit session state initialization and basket helpers."""
import streamlit as st
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import b2_dedup
from gui.db import get_selection_size, resolve_folder_to_ids


def init_session_state():
    """Must be called once at app startup (before any tab renders)."""
    if 'db_init_done' not in st.session_state:
        b2_dedup.init_db()
        st.session_state.db_init_done = True

    if 'basket_file_ids' not in st.session_state:
        st.session_state.basket_file_ids = set()

    if 'basket_widget_gen' not in st.session_state:
        # Bumped on Clear so checkbox widgets remount unchecked
        st.session_state.basket_widget_gen = 0

    if 'basket_zip_bytes' not in st.session_state:
        st.session_state.basket_zip_bytes = None

    # Drop legacy folder-set if present (membership is file IDs only now)
    st.session_state.pop('basket_folder_paths', None)


def get_basket_all_ids() -> list[int]:
    """Flat list of file IDs currently in the basket."""
    return list(st.session_state.basket_file_ids)


def get_basket_size() -> int:
    return get_selection_size(get_basket_all_ids())


def clear_basket():
    """Empty the basket and force all selection checkboxes to remount unchecked."""
    st.session_state.basket_file_ids = set()
    _invalidate_zip()
    _remount_selection_widgets()


def _invalidate_zip() -> None:
    st.session_state.basket_zip_bytes = None
    st.session_state.pop('basket_zip_status', None)


def _remount_selection_widgets() -> None:
    """Force file checkboxes to re-init from basket_file_ids on next render.

    Call this when the basket changes outside of a checkbox click (folder add,
    remove-from-list, clear). Checkbox-driven add/remove must NOT call this.
    """
    st.session_state.basket_widget_gen = st.session_state.get('basket_widget_gen', 0) + 1


def add_file_to_basket(file_id: int) -> None:
    """Add one file (called from checkbox sync — does not remount widgets)."""
    fid = int(file_id)
    if fid not in st.session_state.basket_file_ids:
        st.session_state.basket_file_ids.add(fid)
        _invalidate_zip()


def remove_file_from_basket(file_id: int, *, remount: bool = False) -> None:
    """Remove one file. Set remount=True when removing from the basket list UI."""
    fid = int(file_id)
    if fid in st.session_state.basket_file_ids:
        st.session_state.basket_file_ids.discard(fid)
        _invalidate_zip()
        if remount:
            _remount_selection_widgets()


def add_folder_to_basket(drive_name: str, folder_path: str) -> int:
    """Expand folder to file IDs and add them. Returns count of newly added files."""
    ids = [int(i) for i in resolve_folder_to_ids(drive_name, folder_path)]
    before = len(st.session_state.basket_file_ids)
    st.session_state.basket_file_ids.update(ids)
    added = len(st.session_state.basket_file_ids) - before
    if added:
        _invalidate_zip()
        # Remount so visible file checkboxes pick up the new membership
        _remount_selection_widgets()
    return added


def is_file_in_basket(file_id: int) -> bool:
    return int(file_id) in st.session_state.basket_file_ids
