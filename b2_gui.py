"""B2 Dedup Explorer — Streamlit web UI entry point.

Assembled from gui/ modules. To add a new tab, create gui/tabs/my_tab.py and
add a render call below. To add sidebar panels, extend gui/components/sidebar.py.
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import streamlit as st

from gui.state import init_session_state
from gui.components.sidebar import render_sidebar
from gui.components.basket import render_basket_bar
from gui.tabs.browse import render_browse_tab
from gui.tabs.search import render_search_tab
from gui.tabs.upload import render_upload_tab

st.set_page_config(
    page_title="B2 Dedup Explorer",
    page_icon="🔎",
    layout="wide",
    initial_sidebar_state="expanded",
)

init_session_state()

selected_drive, selected_group_name, group_map = render_sidebar()

render_basket_bar()

tab_browse, tab_search, tab_upload = st.tabs(["📂 Browse Folders", "🔍 Search Files", "☁️ Upload Drive"])

with tab_browse:
    render_browse_tab(selected_drive, selected_group_name, group_map)

with tab_search:
    render_search_tab(selected_drive, selected_group_name, group_map)

with tab_upload:
    render_upload_tab(selected_drive, selected_group_name, group_map)
