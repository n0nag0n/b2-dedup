"""Basket bar (above tabs) and ZIP download logic."""
import json
import os
import tempfile
import zipfile

import streamlit as st

import b2_dedup
from gui.config import load_gui_config
from gui.db import get_db_connection, format_size
from gui.state import get_basket_all_ids, get_basket_size, clear_basket


def render_basket_bar():
    """Compact one-line bar shown above the tabs: count, size, clear, download."""
    basket_ids = get_basket_all_ids()
    if basket_ids:
        basket_size = get_basket_size()
        bar_cols = st.columns([4, 1, 1])
        bar_cols[0].markdown(
            f"**Basket:** {len(basket_ids)} file(s) &nbsp;·&nbsp; "
            f"**{format_size(basket_size)}** uncompressed"
        )
        if bar_cols[1].button("Clear", key="bar_clear_basket"):
            clear_basket()
            st.rerun()
        if bar_cols[2].button("⬇ Download ZIP", key="bar_download", type="primary"):
            _render_basket_download(basket_ids)
    else:
        st.caption("Basket empty — check files or folders below to add them.")
    st.divider()


def _render_basket_download(all_ids: list[int]):
    """Build a ZIP from the basket and serve it as a browser download."""
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
        with zipfile.ZipFile(tmp_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for i, fid in enumerate(all_ids):
                conn = get_db_connection()
                rec = conn.execute(
                    "SELECT hash, is_original, upload_path, drive_name, file_path FROM files WHERE id = ?",
                    (fid,)
                ).fetchone()
                conn.close()
                if not rec:
                    skipped += 1
                    continue

                f_hash, is_orig, up_path, d_name, f_path = rec
                remote_path = b2_dedup.sanitize_b2_path(f"{d_name}/{f_path}")

                try:
                    if is_orig:
                        final_path = up_path if up_path else remote_path
                        content = b2.download_file_content(final_path)
                    else:
                        ptr_path = remote_path + b2_dedup.POINTER_EXTENSION
                        ptr_content = b2.download_file_content(ptr_path)
                        pointer = json.loads(ptr_content)
                        content = b2.download_file_content(pointer['original_path'])
                    zf.writestr(f_path, content)
                except Exception as e:
                    st.warning(f"Skipped {f_path}: {e}")
                    skipped += 1

                progress.progress((i + 1) / len(all_ids), text=f"Fetching {i + 1}/{len(all_ids)}...")

        with open(tmp_path, 'rb') as f:
            zip_bytes = f.read()

        progress.empty()
        fetched = len(all_ids) - skipped
        st.success(f"Ready: {fetched} file(s) packaged{f', {skipped} skipped' if skipped else ''}.")
        st.download_button(
            label="⬇ Save ZIP",
            data=zip_bytes,
            file_name="b2_files.zip",
            mime="application/zip",
            key="basket_save_zip"
        )
    except Exception as e:
        st.error(f"Error building ZIP: {e}")
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)
