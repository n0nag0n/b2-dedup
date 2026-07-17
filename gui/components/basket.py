"""Basket bar (above tabs) and ZIP download logic."""
import json
import os
import tempfile
import zipfile

import streamlit as st

import b2_dedup
from gui.config import load_gui_config
from gui.db import get_db_connection, format_size
from gui.state import (
    get_basket_all_ids,
    get_basket_size,
    clear_basket,
    remove_file_from_basket,
)


def render_basket_bar():
    """Compact bar shown above the tabs: count, size, clear, download, optional list."""
    basket_ids = get_basket_all_ids()

    if not basket_ids:
        st.caption("Basket empty — check files below (or ＋ on a folder) to add them.")
        _show_basket_notice()
        st.divider()
        return

    basket_size = get_basket_size()
    bar_cols = st.columns([4, 1, 1])
    bar_cols[0].markdown(
        f"**Basket:** {len(basket_ids)} file(s) &nbsp;·&nbsp; "
        f"**{format_size(basket_size)}** uncompressed"
    )
    if bar_cols[1].button("Clear", key="bar_clear_basket"):
        clear_basket()
        st.session_state.pop("basket_notice", None)
        st.rerun()
    if bar_cols[2].button("⬇ Download ZIP", key="bar_download", type="primary"):
        _build_and_store_zip(basket_ids)

    _show_basket_notice()
    _render_basket_contents(basket_ids)
    _render_ready_download()
    st.divider()


def _show_basket_notice() -> None:
    """Durable status line (not toast). Stays until Clear or the next folder add overwrites it.

    Do not pop() on read — Streamlit can run the script more than once per interaction,
    which would swallow a one-shot message before it appears in the UI.
    """
    notice = st.session_state.get("basket_notice")
    if notice:
        st.success(notice)


def _render_basket_contents(basket_ids: list[int]) -> None:
    """Expandable list so the user can see / remove items before download."""
    with st.expander(f"View basket ({len(basket_ids)} files)", expanded=False):
        conn = get_db_connection()
        try:
            placeholders = ",".join("?" * len(basket_ids))
            rows = conn.execute(
                f"SELECT id, drive_name, file_path, size FROM files "
                f"WHERE id IN ({placeholders}) ORDER BY drive_name, file_path",
                basket_ids,
            ).fetchall()
        finally:
            conn.close()

        if not rows:
            st.caption("No matching files in the database.")
            return

        # Cap display for huge baskets
        show = rows[:100]
        for fid, drive, path, size in show:
            c1, c2 = st.columns([8, 1])
            c1.caption(f"`{drive}/{path}` · {format_size(size or 0)}")
            if c2.button("✕", key=f"basket_rm_{fid}", help="Remove from basket"):
                remove_file_from_basket(fid, remount=True)
                st.rerun()
        if len(rows) > 100:
            st.caption(f"…and {len(rows) - 100} more")


def _build_and_store_zip(all_ids: list[int]) -> None:
    """Fetch basket files from B2, build a ZIP, store bytes in session_state."""
    config = load_gui_config()
    bucket_name = config.get("bucket_name", "")
    if not bucket_name:
        st.error("No B2 bucket configured. Set it in the sidebar.")
        return

    progress = st.progress(0, text="Connecting to B2...")
    try:
        b2 = b2_dedup.B2Manager(bucket_name)
    except Exception as e:
        st.error(f"Could not connect to B2: {e}")
        progress.empty()
        return

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
            tmp_path = tmp.name

        skipped = 0
        conn = get_db_connection()
        try:
            placeholders = ",".join("?" * len(all_ids))
            rows = conn.execute(
                f"SELECT id, hash, is_original, upload_path, drive_name, file_path "
                f"FROM files WHERE id IN ({placeholders})",
                all_ids,
            ).fetchall()
        finally:
            conn.close()
        rec_by_id = {r[0]: r[1:] for r in rows}

        with zipfile.ZipFile(tmp_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for i, fid in enumerate(all_ids):
                rec = rec_by_id.get(fid)
                if not rec:
                    skipped += 1
                    continue

                _, is_orig, up_path, d_name, f_path = rec
                remote_path = b2_dedup.sanitize_b2_path(f"{d_name}/{f_path}")
                # Prefix drive so paths from different drives never collide
                zip_name = f"{d_name}/{f_path}"

                try:
                    if is_orig:
                        final_path = up_path if up_path else remote_path
                        content = b2.download_file_content(final_path)
                    else:
                        ptr_path = remote_path + b2_dedup.POINTER_EXTENSION
                        ptr_content = b2.download_file_content(ptr_path)
                        pointer = json.loads(ptr_content)
                        content = b2.download_file_content(pointer["original_path"])
                    zf.writestr(zip_name, content)
                except Exception as e:
                    st.warning(f"Skipped {zip_name}: {e}")
                    skipped += 1

                progress.progress(
                    (i + 1) / len(all_ids),
                    text=f"Fetching {i + 1}/{len(all_ids)}...",
                )

        with open(tmp_path, "rb") as f:
            st.session_state.basket_zip_bytes = f.read()

        progress.empty()
        fetched = len(all_ids) - skipped
        st.session_state.basket_zip_status = (
            f"Ready: {fetched} file(s) packaged"
            + (f", {skipped} skipped" if skipped else "")
            + ". Click **Save ZIP** below."
        )
    except Exception as e:
        st.error(f"Error building ZIP: {e}")
        st.session_state.basket_zip_bytes = None
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


def _render_ready_download() -> None:
    """If a ZIP was prepared, offer the browser download button."""
    zip_bytes = st.session_state.get("basket_zip_bytes")
    if not zip_bytes:
        return
    status = st.session_state.get("basket_zip_status", "ZIP ready.")
    st.success(status)
    st.download_button(
        label="⬇ Save ZIP",
        data=zip_bytes,
        file_name="b2_files.zip",
        mime="application/zip",
        key="basket_save_zip",
        type="primary",
    )
