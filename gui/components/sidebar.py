"""Sidebar: drive selector, group management, bucket config, DB backup, basket summary."""
import os
import sqlite3
import threading
from datetime import datetime

import streamlit as st

import b2_dedup
from gui.config import load_gui_config, save_gui_config
from gui.db import get_db_connection, get_drives, get_groups, format_size
from gui.state import get_basket_all_ids, get_basket_size, clear_basket

DB_REMOTE_PATH = "__b2_dedup_metadata__/b2_dedup.db"


def render_sidebar() -> tuple[str, str, dict[str, int]]:
    """Render the full sidebar.

    Returns:
        selected_drive: "All Drives" or a drive name
        selected_group_name: "All Files" or a group name
        group_map: {name: id} for all groups
    """
    with st.sidebar:
        st.title("🗂 File Explorer")

        selected_drive = _render_drive_selector()
        selected_group_name, group_map = _render_group_selector()

        st.divider()
        _render_bucket_config()

        st.divider()
        _render_db_backup()

        st.divider()
        _render_group_creation()

        st.divider()
        _render_basket_summary()

    return selected_drive, selected_group_name, group_map


# ── private helpers ──────────────────────────────────────────────────────────

def _render_drive_selector() -> str:
    drives = get_drives()
    return st.selectbox("Drive", ["All Drives"] + drives)


def _render_group_selector() -> tuple[str, dict[str, int]]:
    group_map = get_groups()
    selected = st.selectbox("Filter by Group", ["All Files"] + list(group_map.keys()))
    return selected, group_map


def _render_bucket_config():
    cfg = load_gui_config()
    bucket = cfg.get("bucket_name", "")
    if bucket:
        st.caption(f"B2 Bucket: `{bucket}`")
        with st.expander("Change bucket"):
            new_bucket = st.text_input("New bucket name", value=bucket, key="sidebar_bucket_edit")
            if st.button("Save bucket", key="sidebar_bucket_save") and new_bucket:
                cfg["bucket_name"] = new_bucket
                save_gui_config(cfg)
                st.rerun()
    else:
        new_bucket = st.text_input(
            "B2 Bucket Name", key="sidebar_bucket_new", placeholder="my-bucket-name"
        )
        if st.button("Save bucket", key="sidebar_bucket_save_new") and new_bucket:
            cfg["bucket_name"] = new_bucket
            save_gui_config(cfg)
            st.rerun()


def _render_db_backup():
    with st.expander("Database Backup"):
        db_bucket = load_gui_config().get("bucket_name", "")
        if not db_bucket:
            st.warning("Configure a B2 bucket above first.")
            return

        def _check_db_status():
            try:
                bm = b2_dedup.B2Manager(db_bucket)
                b2_info = bm.get_file_info(DB_REMOTE_PATH)
                local_mtime = os.path.getmtime(b2_dedup.DB_PATH)
                local_size = os.path.getsize(b2_dedup.DB_PATH)
                st.session_state.db_backup_status = {
                    "b2_info": b2_info,
                    "local_mtime": local_mtime,
                    "local_size": local_size,
                }
            except Exception as e:
                st.error(f"Error checking status: {e}")

        if "db_backup_status" not in st.session_state:
            _check_db_status()

        status = st.session_state.get("db_backup_status")
        if not status:
            return

        b2_info = status["b2_info"]
        local_mtime = status["local_mtime"]
        local_size = status["local_size"]
        local_dt = datetime.fromtimestamp(local_mtime).strftime("%Y-%m-%d %H:%M:%S")
        st.caption(f"Local DB: {format_size(local_size)}, modified {local_dt}")

        cfg = load_gui_config()
        backed_mtime = cfg.get("db_backup_local_mtime")
        backed_size = cfg.get("db_backup_local_size")

        show_backup_btn = False
        if b2_info is None or backed_mtime is None:
            st.warning("Not backed up to B2.")
            show_backup_btn = True
        else:
            b2_dt = datetime.fromtimestamp(
                b2_info["upload_timestamp_ms"] / 1000.0
            ).strftime("%Y-%m-%d %H:%M:%S")
            st.caption(f"B2 backup: {format_size(b2_info['size'])}, uploaded {b2_dt}")
            if local_mtime > backed_mtime + 1 or local_size != backed_size:
                st.warning("Local has changed since last backup.")
            else:
                st.success("Up to date.")
            show_backup_btn = True

        col1, col2, col3 = st.columns(3)
        if col1.button("Sync To", key="db_sync_to"):
            _confirm_sync_dialog("Sync To", db_bucket)

        if col2.button("Sync From", key="db_sync_from"):
            _confirm_sync_dialog("Sync From", db_bucket)

        if col3.button("Refresh", key="db_backup_refresh"):
            del st.session_state["db_backup_status"]
            st.rerun()


@st.dialog("Confirm Database Sync")
def _confirm_sync_dialog(action_type: str, db_bucket: str):
    st.warning(f"Are you sure you want to {action_type}?")
    st.write("This may overwrite your existing backup. A `.prev` version will be kept automatically.")
    if st.button("Confirm"):
        if action_type == "Sync To":
            _do_db_sync_to(db_bucket)
        elif action_type == "Sync From":
            _do_db_sync_from(db_bucket)
        st.rerun()


def _do_db_sync_to(db_bucket: str):
    try:
        from b2sdk.v2 import AbstractProgressListener
        from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx

        db_size = os.path.getsize(b2_dedup.DB_PATH)
        progress_bar = st.progress(0, text="Uploading…")
        status_text = st.empty()
        _ctx = get_script_run_ctx()

        class _StreamlitListener(AbstractProgressListener):
            def set_total_bytes(self, total):
                pass

            def bytes_completed(self, byte_count):
                add_script_run_ctx(threading.current_thread(), _ctx)
                pct = min(byte_count / db_size, 1.0) if db_size else 1.0
                done_mb = byte_count / (1024 * 1024)
                total_mb = db_size / (1024 * 1024)
                progress_bar.progress(pct, text=f"Uploading… {done_mb:.0f} / {total_mb:.0f} MB")

            def close(self):
                add_script_run_ctx(threading.current_thread(), _ctx)
                progress_bar.progress(1.0, text="Upload complete!")
                status_text.success("Database backed up successfully.")
                super().close()

        bm = b2_dedup.B2Manager(db_bucket)
        
        # Keep previous backup remotely in B2
        try:
            prev_remote_path = DB_REMOTE_PATH.replace(".db", ".prev.db")
            bm.copy_file(DB_REMOTE_PATH, prev_remote_path)
        except Exception:
            pass

        snapshot_mtime = os.path.getmtime(b2_dedup.DB_PATH)
        snapshot_size = os.path.getsize(b2_dedup.DB_PATH)
        bm.upload_file(b2_dedup.DB_PATH, DB_REMOTE_PATH, progress_listener=_StreamlitListener())
        cfg = load_gui_config()
        cfg["db_backup_local_mtime"] = snapshot_mtime
        cfg["db_backup_local_size"] = snapshot_size
        save_gui_config(cfg)
        del st.session_state["db_backup_status"]
    except Exception as e:
        st.error(f"Sync To failed: {e}")


def _do_db_sync_from(db_bucket: str):
    try:
        status_text = st.empty()
        status_text.info("Downloading database from B2...")
        bm = b2_dedup.B2Manager(db_bucket)

        # Backup local just in case
        if os.path.exists(b2_dedup.DB_PATH):
            import shutil
            shutil.copy2(b2_dedup.DB_PATH, str(b2_dedup.DB_PATH) + ".prev")

        bm.download_file_to_path(DB_REMOTE_PATH, b2_dedup.DB_PATH)

        snapshot_mtime = os.path.getmtime(b2_dedup.DB_PATH)
        snapshot_size = os.path.getsize(b2_dedup.DB_PATH)

        cfg = load_gui_config()
        cfg["db_backup_local_mtime"] = snapshot_mtime
        cfg["db_backup_local_size"] = snapshot_size
        save_gui_config(cfg)
        
        if "db_backup_status" in st.session_state:
            del st.session_state["db_backup_status"]
            
        status_text.success("Database synced from B2 successfully.")
        import time; time.sleep(1)
    except Exception as e:
        st.error(f"Sync From failed: {e}")


def _render_group_creation():
    new_group = st.text_input("New Group Name")
    if st.button("Create Group") and new_group:
        conn = get_db_connection()
        try:
            conn.execute(
                "INSERT INTO groups (name, created_at) VALUES (?, ?)",
                (new_group, datetime.now().isoformat())
            )
            conn.commit()
            st.success(f"Group '{new_group}' created!")
            st.rerun()
        except sqlite3.IntegrityError:
            st.error("Group already exists")
        finally:
            conn.close()


def _render_basket_summary():
    basket_ids = get_basket_all_ids()
    if basket_ids:
        basket_size = get_basket_size()
        st.markdown(f"**Basket:** {len(basket_ids)} file(s)")
        st.caption(f"{format_size(basket_size)} uncompressed")
        if st.button("Clear basket", key="sidebar_clear_basket"):
            clear_basket()
            st.rerun()
    else:
        st.caption("Basket is empty")
