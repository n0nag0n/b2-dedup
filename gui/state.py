"""Streamlit session state initialization and basket helpers."""
import streamlit as st
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import b2_dedup
from gui.db import get_basket_file_ids, get_selection_size


def init_session_state():
    """Must be called once at app startup (before any tab renders)."""
    if 'db_init_done' not in st.session_state:
        b2_dedup.init_db()
        st.session_state.db_init_done = True

    if 'basket_file_ids' not in st.session_state:
        st.session_state.basket_file_ids = set()

    if 'basket_folder_paths' not in st.session_state:
        st.session_state.basket_folder_paths = set()


def get_basket_all_ids() -> list[int]:
    return get_basket_file_ids(
        st.session_state.basket_file_ids,
        st.session_state.basket_folder_paths,
    )


def get_basket_size() -> int:
    return get_selection_size(get_basket_all_ids())


def clear_basket():
    st.session_state.basket_file_ids = set()
    st.session_state.basket_folder_paths = set()
