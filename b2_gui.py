import streamlit as st
import sqlite3
import pandas as pd
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
import json
import subprocess
import urllib.parse
import zipfile
import tempfile

# Import config from main script
# Ensure current directory is in path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import b2_dedup

# ================= CONFIG =================
ROWS_PER_PAGE = 50
DB_REMOTE_PATH = "__b2_dedup_metadata__/b2_dedup.db"

# Set page config
st.set_page_config(
    page_title="B2 Dedup Explorer",
    page_icon="🔎",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize DB and basket state on startup
if 'db_init_done' not in st.session_state:
    b2_dedup.init_db()
    st.session_state.db_init_done = True

if 'basket_file_ids' not in st.session_state:
    st.session_state.basket_file_ids = set()   # set of int file IDs

if 'basket_folder_paths' not in st.session_state:
    st.session_state.basket_folder_paths = set()  # set of (drive_name, path) tuples


# ================= HELPERS =================

def get_db_connection():
    return sqlite3.connect(b2_dedup.DB_PATH)

GUI_CONFIG_PATH = b2_dedup._DATA_DIR / "b2_gui_config.json"

def load_gui_config():
    if GUI_CONFIG_PATH.exists():
        try:
            with open(GUI_CONFIG_PATH, 'r') as f:
                return json.load(f)
        except:
            pass
    return {}

def save_gui_config(config):
    try:
        with open(GUI_CONFIG_PATH, 'w') as f:
            json.dump(config, f)
    except:
        pass

def format_size(size_bytes):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"

def get_basket_all_ids():
    """Return flat list of file IDs from basket (files + expanded folders)."""
    ids = set(st.session_state.basket_file_ids)
    if st.session_state.basket_folder_paths:
        conn = get_db_connection()
        for drive_name, folder_path in st.session_state.basket_folder_paths:
            path = folder_path if folder_path.endswith('/') else folder_path + '/'
            found = conn.execute(
                "SELECT id FROM files WHERE drive_name = ? AND file_path LIKE ?",
                (drive_name, f"{path}%")
            ).fetchall()
            ids.update(f[0] for f in found)
        conn.close()
    return list(ids)

def get_selection_size(all_ids):
    """Return total uncompressed bytes for a list of file IDs."""
    if not all_ids:
        return 0
    conn = get_db_connection()
    placeholders = ",".join("?" * len(all_ids))
    row = conn.execute(f"SELECT SUM(size) FROM files WHERE id IN ({placeholders})", all_ids).fetchone()
    conn.close()
    return row[0] or 0

def resolve_selection_to_ids(selected_rows):
    """Resolve a DataFrame selection to file IDs, expanding directory rows."""
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
                if not d_path.endswith('/'):
                    d_path += '/'
                found = conn.execute(
                    "SELECT id FROM files WHERE drive_name = ? AND file_path LIKE ?",
                    (d_drive, f"{d_path}%")
                ).fetchall()
                if found:
                    ids.update(f[0] for f in found)
    except Exception as e:
        st.error(f"Error resolving selection: {e}")
    conn.close()
    return list(ids)


# ================= GROUP ACTIONS =================

def render_group_actions(selected_rows, group_map, current_group_name, key_suffix):
    """Render Group Add/Remove actions for current-view selection."""
    if selected_rows.empty:
        return
    col_a, col_b = st.columns(2)
    with col_a:
        if group_map:
            cols = st.columns([2, 1])
            target_group = cols[0].selectbox("Add to Group", list(group_map.keys()), key=f"add_grp_{key_suffix}")
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


# ================= BASKET DOWNLOAD =================

def render_basket_download():
    """Build a ZIP from the basket and serve it as a browser download."""
    config = load_gui_config()
    bucket_name = config.get("bucket_name", "")
    if not bucket_name:
        st.error("No B2 bucket configured. Set it in the sidebar.")
        return

    all_ids = get_basket_all_ids()
    if not all_ids:
        st.warning("Basket is empty.")
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


# ================= SIDEBAR =================

with st.sidebar:
    st.title("🗂 File Explorer")

    # Drive Filter
    conn = get_db_connection()
    drives = pd.read_sql("SELECT DISTINCT drive_name FROM files ORDER BY drive_name", conn)['drive_name'].tolist()
    conn.close()
    selected_drive = st.selectbox("Drive", ["All Drives"] + drives)

    # Groups Filter
    conn = get_db_connection()
    groups_df = pd.read_sql("SELECT id, name FROM groups ORDER BY name", conn)
    conn.close()
    group_map = {row['name']: row['id'] for _, row in groups_df.iterrows()}
    selected_group_name = st.selectbox("Filter by Group", ["All Files"] + list(group_map.keys()))

    # B2 Bucket Config
    st.divider()
    _cfg = load_gui_config()
    _bucket = _cfg.get("bucket_name", "")
    if _bucket:
        st.caption(f"B2 Bucket: `{_bucket}`")
        with st.expander("Change bucket"):
            _new_bucket = st.text_input("New bucket name", value=_bucket, key="sidebar_bucket_edit")
            if st.button("Save bucket", key="sidebar_bucket_save") and _new_bucket:
                _cfg["bucket_name"] = _new_bucket
                save_gui_config(_cfg)
                st.rerun()
    else:
        _new_bucket = st.text_input("B2 Bucket Name", key="sidebar_bucket_new", placeholder="my-bucket-name")
        if st.button("Save bucket", key="sidebar_bucket_save_new") and _new_bucket:
            _cfg["bucket_name"] = _new_bucket
            save_gui_config(_cfg)
            st.rerun()

    # Database Backup
    st.divider()
    with st.expander("Database Backup"):
        _db_bucket = load_gui_config().get("bucket_name", "")
        if not _db_bucket:
            st.warning("Configure a B2 bucket above first.")
        else:
            def _check_db_status():
                try:
                    bm = b2_dedup.B2Manager(_db_bucket)
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
            if status:
                b2_info = status["b2_info"]
                local_mtime = status["local_mtime"]
                local_size = status["local_size"]
                local_dt = datetime.fromtimestamp(local_mtime).strftime("%Y-%m-%d %H:%M:%S")
                st.caption(f"Local DB: {format_size(local_size)}, modified {local_dt}")

                _cfg = load_gui_config()
                backed_mtime = _cfg.get("db_backup_local_mtime")
                backed_size = _cfg.get("db_backup_local_size")

                show_backup_btn = False
                if b2_info is None or backed_mtime is None:
                    st.warning("Not backed up to B2.")
                    show_backup_btn = True
                else:
                    b2_dt = datetime.fromtimestamp(b2_info["upload_timestamp_ms"] / 1000.0).strftime("%Y-%m-%d %H:%M:%S")
                    st.caption(f"B2 backup: {format_size(b2_info['size'])}, uploaded {b2_dt}")
                    if local_mtime > backed_mtime + 1 or local_size != backed_size:
                        st.warning("Local has changed since last backup.")
                    else:
                        st.success("Up to date.")
                    show_backup_btn = True

                col1, col2 = st.columns(2)
                if col2.button("Refresh", key="db_backup_refresh"):
                    del st.session_state["db_backup_status"]
                    st.rerun()
                if show_backup_btn and col1.button("Backup Now", key="db_backup_now"):
                    try:
                        from b2sdk.v2 import AbstractProgressListener
                        from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx
                        import threading
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

                        bm = b2_dedup.B2Manager(_db_bucket)
                        snapshot_mtime = os.path.getmtime(b2_dedup.DB_PATH)
                        snapshot_size = os.path.getsize(b2_dedup.DB_PATH)
                        bm.upload_file(b2_dedup.DB_PATH, DB_REMOTE_PATH, progress_listener=_StreamlitListener())
                        _cfg = load_gui_config()
                        _cfg["db_backup_local_mtime"] = snapshot_mtime
                        _cfg["db_backup_local_size"] = snapshot_size
                        save_gui_config(_cfg)
                        del st.session_state["db_backup_status"]
                        st.rerun()
                    except Exception as e:
                        st.error(f"Backup failed: {e}")

    # New Group Creation
    st.divider()
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
        conn.close()

    # Basket summary
    st.divider()
    _basket_ids = get_basket_all_ids()
    if _basket_ids:
        _basket_size = get_selection_size(_basket_ids)
        st.markdown(f"**Basket:** {len(_basket_ids)} file(s)")
        st.caption(f"{format_size(_basket_size)} uncompressed")
        if st.button("Clear basket", key="sidebar_clear_basket"):
            st.session_state.basket_file_ids = set()
            st.session_state.basket_folder_paths = set()
            st.rerun()
    else:
        st.caption("Basket is empty")


# ================= BASKET BAR (above tabs) =================

basket_ids = get_basket_all_ids()
if basket_ids:
    basket_size = get_selection_size(basket_ids)
    bar_cols = st.columns([4, 1, 1])
    bar_cols[0].markdown(
        f"**Basket:** {len(basket_ids)} file(s) &nbsp;·&nbsp; "
        f"**{format_size(basket_size)}** uncompressed"
    )
    if bar_cols[1].button("Clear", key="bar_clear_basket"):
        st.session_state.basket_file_ids = set()
        st.session_state.basket_folder_paths = set()
        st.rerun()
    if bar_cols[2].button("⬇ Download ZIP", key="bar_download", type="primary"):
        render_basket_download()
else:
    st.caption("Basket empty — check files or folders below to add them.")

st.divider()


# ================= TABS =================

tab_browse, tab_search = st.tabs(["📂 Browse Folders", "🔍 Search Files"])


# =======================
# TAB 1: BROWSE FOLDERS
# =======================
with tab_browse:
    if selected_drive == "All Drives":
        st.warning("Please select a specific Drive from the sidebar to browse.")
    else:
        if 'browse_path' not in st.session_state:
            st.session_state.browse_path = ""
        if 'browse_page' not in st.session_state:
            st.session_state.browse_page = 1

        # Navigation bar
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

        current_prefix = st.session_state.browse_path
        if current_prefix and not current_prefix.endswith('/'):
            current_prefix += '/'

        # Find immediate subdirectories (skip-scan strategy)
        dirs = []
        conn = get_db_connection()
        try:
            cur = conn.cursor()
            seek_path = current_prefix
            for _ in range(500):
                row = cur.execute(
                    "SELECT file_path FROM files WHERE drive_name = ? AND file_path > ? AND file_path LIKE ? ORDER BY file_path ASC LIMIT 1",
                    (selected_drive, seek_path, f"{current_prefix}%")
                ).fetchone()
                if not row:
                    break
                full_path = row[0]
                rel_path = full_path[len(current_prefix):]
                if '/' in rel_path:
                    subdir_name = rel_path.split('/')[0]
                    dirs.append(subdir_name)
                    seek_path = f"{current_prefix}{subdir_name}\uffff"
                else:
                    seek_path = full_path
        except Exception as e:
            st.error(f"Error listing directories: {e}")
        conn.close()

        # Files query (current level only, paginated)
        f_params = [selected_drive, f"{current_prefix}%", f"{current_prefix}%/%"]
        file_query = """
            SELECT f.id, f.drive_name, f.file_path, f.size, f.created_at, f.is_original,
                   f.file_type, f.mime_type, f.file_mtime, f.file_atime, f.file_ctime
            FROM files f
            WHERE f.drive_name = ? AND f.file_path LIKE ? AND f.file_path NOT LIKE ?
        """
        conn = get_db_connection()
        total_browse_files = conn.execute(
            "SELECT COUNT(*) FROM files f WHERE f.drive_name = ? AND f.file_path LIKE ? AND f.file_path NOT LIKE ?",
            f_params
        ).fetchone()[0]
        browse_offset = (st.session_state.browse_page - 1) * ROWS_PER_PAGE
        f_df = pd.read_sql(
            file_query + f" ORDER BY f.file_path LIMIT {ROWS_PER_PAGE} OFFSET {browse_offset}",
            conn, params=f_params
        )
        conn.close()

        # Build dirs DataFrame (page 1 only)
        if st.session_state.browse_page == 1 and dirs:
            dirs_df = pd.DataFrame([{
                'is_dir': True,
                'name': d,
                'file_path': f"{current_prefix}{d}",
                'drive_name': selected_drive,
                'id': -1,
            } for d in dirs])
        else:
            dirs_df = pd.DataFrame()

        # --- Render directories: checkbox (basket) + nav button ---
        if not dirs_df.empty:
            st.divider()
            for _, row in dirs_df.iterrows():
                folder_key = (row['drive_name'], row['file_path'])
                widget_key = f"dir_sel_{row['drive_name']}_{row['file_path']}"
                nav_key = f"nav_{row['drive_name']}_{row['file_path']}"

                # Pre-populate checkbox from basket
                if widget_key not in st.session_state:
                    st.session_state[widget_key] = folder_key in st.session_state.basket_folder_paths

                dcols = st.columns([0.5, 8])
                checked = dcols[0].checkbox(
                    "select dir", key=widget_key, label_visibility="collapsed"
                )
                # Sync to basket
                if checked:
                    st.session_state.basket_folder_paths.add(folder_key)
                else:
                    st.session_state.basket_folder_paths.discard(folder_key)

                if dcols[1].button(f"📁 {row['name']}", key=nav_key):
                    st.session_state.browse_path = row['file_path']
                    st.session_state.browse_page = 1
                    st.rerun()

        # --- Render files as data_editor, pre-populated from basket ---
        if not f_df.empty:
            f_df['size_fmt'] = f_df['size'].apply(format_size)
            f_df['name'] = f_df['file_path'].apply(lambda x: x[len(current_prefix):])
            for col in ['file_mtime', 'created_at', 'file_atime', 'file_ctime']:
                if col in f_df.columns:
                    f_df[col] = pd.to_datetime(f_df[col], errors='coerce')

            # Pre-populate 'selected' from basket
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

            # Sync file selections back to basket
            for _, row in browse_edited.iterrows():
                fid = int(row['id'])
                if row['selected']:
                    st.session_state.basket_file_ids.add(fid)
                else:
                    st.session_state.basket_file_ids.discard(fid)

            # Group actions for current-view file selection only
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
                total_pages = (total_browse_files // ROWS_PER_PAGE) + 1
                st.write(f"Page {st.session_state.browse_page} of {total_pages}")
            with col_bp3:
                if total_browse_files > st.session_state.browse_page * ROWS_PER_PAGE:
                    if st.button("Next"):
                        st.session_state.browse_page += 1
                        st.rerun()

        elif dirs_df.empty:
            st.info("No files in this folder")


# =======================
# TAB 2: SEARCH FILES
# =======================
with tab_search:
    col1, col2 = st.columns([3, 1])
    with col1:
        search_query = st.text_input("Search files (Path/Name)", placeholder="e.g. DCIM or .jpg")
    with col2:
        sort_order = st.selectbox("Sort by", [
            "Date Modified (Newest)", "Date Modified (Oldest)",
            "Date Added (Newest)", "Date Added (Oldest)",
            "Size (Largest)", "Size (Smallest)",
            "Type (A-Z)", "Type (Z-A)"
        ])

    with st.expander("Advanced Filters"):
        try:
            conn = get_db_connection()
            types = pd.read_sql("SELECT DISTINCT file_type FROM files ORDER BY file_type", conn)['file_type'].tolist()
            conn.close()
            types = [t for t in types if t]
        except:
            types = []

        af_col1, af_col2, af_col3, af_col4 = st.columns(4)
        with af_col1:
            filter_origin = st.selectbox("Original / Duplicate", ["All", "Originals Only", "Duplicates Only"])
        with af_col2:
            filter_date_col = st.selectbox("Filter Date By", ["Date Added", "File Modified"])
        with af_col3:
            filter_period = st.selectbox("Time Period", ["All Time", "Last 24 Hours", "Last 7 Days", "Last 30 Days", "Custom Range"])
        with af_col4:
            filter_file_type = st.selectbox("File Category", ["All Types"] + types)

        filter_date_range = []
        if filter_period == "Custom Range":
            filter_date_range = st.date_input("Select Date Range", [])

        st.caption("Extensions Filter")
        filter_ext = st.text_input("File Extensions (comma separated)", placeholder="e.g. jpg, png, pdf")

    if 'page' not in st.session_state:
        st.session_state.page = 1

    # Build query
    base_query = """
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

    if filter_file_type != "All Types":
        conditions.append("f.file_type = ?")
        params.append(filter_file_type)

    if selected_group_name != "All Files":
        group_id = group_map[selected_group_name]
        base_query += " JOIN group_members gm_filter ON f.id = gm_filter.file_id"
        conditions.append("gm_filter.group_id = ?")
        params.append(group_id)

    if search_query:
        conn = get_db_connection()
        fts_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='files_fts'"
        ).fetchone()
        conn.close()
        if fts_exists:
            base_query += " JOIN files_fts fts ON f.id = fts.rowid"
            conditions.append("files_fts MATCH ?")
            chars = set('*"')
            fts_query = search_query if any(c in search_query for c in chars) else f'"{search_query}" *'
            params.append(fts_query)
        else:
            conditions.append("f.file_path LIKE ?")
            params.append(f"%{search_query}%")

    if filter_origin == "Originals Only":
        conditions.append("f.is_original = 1")
    elif filter_origin == "Duplicates Only":
        conditions.append("f.is_original = 0")

    date_col = "f.created_at" if filter_date_col == "Date Added" else "f.file_mtime"
    if filter_period != "All Time":
        now = datetime.now()
        if filter_period == "Last 24 Hours":
            conditions.append(f"{date_col} >= ?")
            params.append((now - timedelta(days=1)).isoformat())
        elif filter_period == "Last 7 Days":
            conditions.append(f"{date_col} >= ?")
            params.append((now - timedelta(days=7)).isoformat())
        elif filter_period == "Last 30 Days":
            conditions.append(f"{date_col} >= ?")
            params.append((now - timedelta(days=30)).isoformat())
        elif filter_period == "Custom Range" and len(filter_date_range) == 2:
            start_date, end_date = filter_date_range
            conditions.append(f"{date_col} >= ? AND {date_col} <= ?")
            params.append(start_date.isoformat())
            params.append(end_date.isoformat() + "T23:59:59")

    if filter_ext:
        exts = [e.strip().lower().lstrip('.') for e in filter_ext.split(',') if e.strip()]
        if exts:
            ext_conditions = [f"f.file_path LIKE ?" for e in exts]
            for e in exts:
                params.append(f"%.{e}")
            conditions.append(f"({' OR '.join(ext_conditions)})")

    if conditions:
        base_query += " WHERE " + " AND ".join(conditions)

    conn = get_db_connection()
    total_rows = conn.execute(f"SELECT COUNT(*) FROM ({base_query})", params).fetchone()[0]
    conn.close()

    sort_map = {
        "Date Modified (Newest)": "f.file_mtime DESC",
        "Date Modified (Oldest)": "f.file_mtime ASC",
        "Date Added (Newest)": "f.created_at DESC",
        "Date Added (Oldest)": "f.created_at ASC",
        "Size (Largest)": "f.size DESC",
        "Size (Smallest)": "f.size ASC",
        "Type (A-Z)": "f.file_type ASC",
        "Type (Z-A)": "f.file_type DESC"
    }
    base_query += f" ORDER BY {sort_map[sort_order]}"
    offset = (st.session_state.page - 1) * ROWS_PER_PAGE
    base_query += f" LIMIT {ROWS_PER_PAGE} OFFSET {offset}"

    conn = get_db_connection()
    df = pd.read_sql(base_query, conn, params=params)
    conn.close()

    st.markdown(f"**Found {total_rows:,} files**")

    if not df.empty:
        df['size_fmt'] = df['size'].apply(format_size)
        for col in ['file_mtime', 'created_at', 'file_atime', 'file_ctime']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Pre-populate 'selected' from basket
        df['selected'] = df['id'].isin(st.session_state.basket_file_ids)

        cols_order = ['selected', 'id', 'drive_name', 'file_path', 'size_fmt', 'file_mtime',
                      'created_at', 'file_atime', 'file_type', 'is_original', 'mime_type', 'file_ctime',
                      'size', 'has_group']
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
                "has_group": None
            },
            hide_index=True,
            disabled=[c for c in cols_order if c != "selected"],
            key=f"editor_{st.session_state.page}"
        )

        # Sync file selections back to basket
        for _, row in edited_df.iterrows():
            fid = int(row['id'])
            if row['selected']:
                st.session_state.basket_file_ids.add(fid)
            else:
                st.session_state.basket_file_ids.discard(fid)

        selected_rows = edited_df[edited_df['selected']]
    else:
        selected_rows = pd.DataFrame()

    # Pagination
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

    # Group actions for current-view selection
    if not selected_rows.empty:
        st.divider()
        render_group_actions(selected_rows, group_map, selected_group_name, "search")
