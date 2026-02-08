import streamlit as st
import sqlite3
import pandas as pd
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import json
import subprocess
import urllib.parse

# Import config from main script
# Ensure current directory is in path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import b2_dedup

# ================= CONFIG =================
ROWS_PER_PAGE = 50    # ← add this  (you can change the number)

# Set page config
st.set_page_config(
    page_title="B2 Dedup Explorer",
    page_icon="🔎",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize DB on startup
if 'db_init_done' not in st.session_state:
    b2_dedup.init_db()
    st.session_state.db_init_done = True

def get_db_connection():
    """Get a connection to the SQLite database."""
    return sqlite3.connect(b2_dedup.DB_PATH)

GUI_CONFIG_PATH = Path.home() / ".b2_gui_config.json"

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
    """Format bytes to human readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"

def resolve_selection_to_ids(selected_rows):
    """Resolve selection to list of file IDs, expanding directories."""
    ids = set()
    conn = get_db_connection()
    
    try:
        # Handle direct files
        # Check if 'is_dir' column exists
        if 'is_dir' in selected_rows.columns:
            # Explicitly cast to boolean to handle 0/1 integers matching boolean logic
            is_dir_mask = selected_rows['is_dir'].fillna(False).astype(bool)
            files = selected_rows[~is_dir_mask]
            dirs = selected_rows[is_dir_mask]
        else:
            files = selected_rows
            dirs = pd.DataFrame()
            
        if not files.empty and 'id' in files.columns:
            # Dropna() because fake rows might have None
            valid_ids = files['id'].dropna()
            # Filter out placeholder ids (-1) just in case
            valid_ids = valid_ids[valid_ids != -1]
            ids.update(valid_ids.astype(int).tolist())
            
        if not dirs.empty:
            for _, row in dirs.iterrows():
                d_drive = row.get('drive_name')
                d_path = row.get('file_path')
                
                if not d_drive or not d_path:
                    continue
                    
                if not d_path.endswith('/'): d_path += '/'
                
                # Find files in dir
                q = "SELECT id FROM files WHERE drive_name = ? AND file_path LIKE ?"
                found = conn.execute(q, (d_drive, f"{d_path}%")).fetchall()
                if found:
                    ids.update(f[0] for f in found)
    except Exception as e:
        st.error(f"Error resolving selection: {e}")
            
    conn.close()
    return list(ids)

def render_group_actions(selected_rows, group_map, current_group_name, key_suffix):
    """Render Group Add/Remove actions."""
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
                            conn.execute("INSERT INTO group_members (group_id, file_id, added_at) VALUES (?, ?, ?)",
                                        (gid, fid, datetime.now().isoformat()))
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
                        conn.execute("DELETE FROM group_members WHERE group_id = ? AND file_id = ?", (gid, ids[0]))
                    else:
                        conn.execute(f"DELETE FROM group_members WHERE group_id = ? AND file_id IN {ids}", (gid,))
                    conn.commit()
                    conn.close()
                    st.success("Removed files from group")
                    st.rerun()

def render_restore_ui(selected_rows, key_suffix):
    """Render Restore/Download UI."""
    st.markdown("### Restore / Download")
    cols = st.columns([3, 2, 1])
    
    # Load config defaults
    config = load_gui_config()
    default_bucket = config.get("bucket_name", "")
    default_dest = config.get("restore_dest", str(Path.home() / "Downloads"))
    
    restore_dest = cols[0].text_input("Restore to (Local Path)", value=default_dest, key=f"dest_{key_suffix}")
    bucket_name = cols[1].text_input("B2 Bucket Name", value=default_bucket, key=f"buck_{key_suffix}")
    
    if cols[2].button("Restore", key=f"btn_rest_{key_suffix}"):
        if not bucket_name:
            st.error("Bucket Name required")
            return

        # Save config
        save_gui_config({
            "bucket_name": bucket_name,
            "restore_dest": restore_dest
        })

        progress_text = "Restoring files..."
        my_bar = st.progress(0, text=progress_text)
        try:
            b2 = b2_dedup.B2Manager(bucket_name)
            dest_path = Path(restore_dest)
            
            all_ids = resolve_selection_to_ids(selected_rows)
            total = len(all_ids)
            
            if total == 0:
                st.warning("No files found to restore. If you selected a folder, ensure it is not empty.")
                my_bar.empty()
                return

            done = 0
            my_bar.progress(0, text=f"Restoring 0/{total}...")
            
            for fid in all_ids:
                conn = get_db_connection()
                file_rec = conn.execute("SELECT hash, is_original, upload_path, drive_name, file_path FROM files WHERE id = ?", (fid,)).fetchone()
                conn.close()
                if not file_rec: continue
                
                f_hash, is_orig, up_path, d_name, f_path = file_rec
                
                # Reconstruct remote path with sanitization for B2
                remote_path = b2_dedup.sanitize_b2_path(f"{d_name}/{f_path}")
                local_path = dest_path / f_path
                
                if is_orig:
                    # Use stored upload_path if available, otherwise sanitized reconstructed path
                    final_remote_path = up_path if up_path else remote_path
                    if hasattr(b2, 'download_file_on_path'):
                        b2.download_file_on_path(final_remote_path, local_path)
                    else:
                        b2.download_file_to_path(final_remote_path, local_path)
                else:
                    # Resolve duplicate from pointer file
                    try:
                        ptr_path = remote_path + b2_dedup.POINTER_EXTENSION
                        ptr_content = b2.download_file_content(ptr_path)
                        pointer = json.loads(ptr_content)
                        # original_path in pointer is already the correct B2 path
                        b2.download_file_to_path(pointer['original_path'], local_path)
                    except Exception as e:
                        # Log error for this file
                        st.error(f"Failed to restore {f_path}: {e}")
                done += 1
                my_bar.progress(done / total, text=f"Restored {done}/{total}")
            st.success(f"Restored {done} files")
        except Exception as e:
            st.error(f"Error: {e}")


# --- Sidebar Filters ---

with st.sidebar:
    st.title("🗂 File Explorer")
    
    # 1. Drive Filter
    conn = get_db_connection()
    drives = pd.read_sql("SELECT DISTINCT drive_name FROM files ORDER BY drive_name", conn)['drive_name'].tolist()
    conn.close()
    
    selected_drive = st.selectbox("Drive", ["All Drives"] + drives)
    

    
    # 2. Groups Filter
    conn = get_db_connection()
    groups_df = pd.read_sql("SELECT id, name FROM groups ORDER BY name", conn)
    conn.close()
    
    group_map = {row['name']: row['id'] for _, row in groups_df.iterrows()}
    selected_group_name = st.selectbox("Filter by Group", ["All Files"] + list(group_map.keys()))
    
    # 3. New Group Creation
    st.divider()
    new_group = st.text_input("New Group Name")
    if st.button("Create Group") and new_group:
        conn = get_db_connection()
        try:
            conn.execute("INSERT INTO groups (name, created_at) VALUES (?, ?)", 
                        (new_group, datetime.now().isoformat()))
            conn.commit()
            st.success(f"Group '{new_group}' created!")
            st.rerun()
        except sqlite3.IntegrityError:
            st.error("Group already exists")
        conn.close()

# --- Tabs: Browse vs Search ---

tab_browse, tab_search = st.tabs(["📂 Browse Folders", "🔍 Search Files"])

# =======================
# TAB 1: BROWSE FOLDERS
# =======================
with tab_browse:
    if selected_drive == "All Drives":
        st.warning("Please select a specific Drive from the sidebar to browse.")
    else:
        # State for current path
        if 'browse_path' not in st.session_state:
            st.session_state.browse_path = "" # Root
            
        # Navigation Bar
        cols = st.columns([1, 8])
        if st.session_state.browse_path:
            with cols[0]:
                if st.button("⬆ Up", key="btn_up"):
                    parent = str(Path(st.session_state.browse_path).parent)
                    st.session_state.browse_path = "" if parent == "." else parent
                    st.rerun()
        with cols[1]:
            st.code(f"/{st.session_state.browse_path}", language="text")

        current_prefix = st.session_state.browse_path
        if current_prefix and not current_prefix.endswith('/'):
            current_prefix += '/'
        
        # 1. Get Directories (Skip Scan Strategy)
        # Efficiently find immediate subdirectories without scanning all files
        dirs = []
        conn = get_db_connection()
        try:
            # We want to find distinct next-level directory components
            # Strategy: Query ordered by path, find first match, extract dir, query > dir + \xff
            
            # Base cursor
            cur = conn.cursor()
            
            # Start search at prefix
            seek_path = current_prefix
            
            # Limit loop to avoid infinite stalls (e.g. max 500 subdirs per folder)
            for _ in range(500):
                # Find first file >= seek_path that matches prefix
                # We need it to match the prefix strict
                query = "SELECT file_path FROM files WHERE drive_name = ? AND file_path > ? AND file_path LIKE ? ORDER BY file_path ASC LIMIT 1"
                row = cur.execute(query, (selected_drive, seek_path, f"{current_prefix}%")).fetchone()
                
                if not row:
                    break
                
                full_path = row[0]
                
                # Extract the relative part after prefix
                rel_path = full_path[len(current_prefix):]
                
                if '/' in rel_path:
                    # It's in a subdir
                    subdir_name = rel_path.split('/')[0]
                    dirs.append(subdir_name)
                    
                    # Advance seek_path to skip this entire subdirectory
                    # prefix + subdir + the highest possible character
                    seek_path = f"{current_prefix}{subdir_name}\uffff"
                else:
                    # It's a file in the current dir.
                    # Since we oredered by path, and files come after dirs in ASCII usually? 
                    # Actually files might interleave.
                    # But we only want DIRS here.
                    # If we hit a file, we just need to skip it? 
                    # No, this loop is optimized for finding DIRS. 
                    # If we found a file, we should skip to next potential dir?
                    # Files don't block DIRS in sorting. 
                    # Wait, if we have "A.txt" and "B/...", "A.txt" comes first.
                    # We just continue searching after this file?
                    seek_path = full_path
                    
        except Exception as e:
            st.error(f"Error listing directories: {e}")
        conn.close()
        
        # Display Directories
        # Display Directories AND Files
        
        # files query
        file_query = """
            SELECT 
                f.id, f.drive_name, f.file_path, f.size, f.created_at, f.is_original, 
                f.file_type, f.mime_type, f.file_mtime, f.file_atime, f.file_ctime
            FROM files f
            WHERE f.drive_name = ? 
            AND f.file_path LIKE ?
            AND f.file_path NOT LIKE ?
        """
        
        f_params = [selected_drive, f"{current_prefix}%", f"{current_prefix}%/%"]
        
        # Pagination for browser files
        if 'browse_page' not in st.session_state: st.session_state.browse_page = 1
        
        browse_offset = (st.session_state.browse_page - 1) * ROWS_PER_PAGE
        
        conn = get_db_connection()
        total_browse_files = conn.execute(f"SELECT COUNT(*) FROM files f WHERE f.drive_name = ? AND f.file_path LIKE ? AND f.file_path NOT LIKE ?", f_params).fetchone()[0]
        
        f_df = pd.read_sql(file_query + f" ORDER BY f.file_path LIMIT {ROWS_PER_PAGE} OFFSET {browse_offset}", conn, params=f_params)
        conn.close()
        
        # Prepare Dataframes
        combined_df = pd.DataFrame()

        # 1. Directories (Show only on Page 1 to avoid confusion or Duplication?)
        # Let's show them on Page 1.
        if st.session_state.browse_page == 1 and dirs:
            dirs_data = []
            for d in dirs:
                dirs_data.append({
                    'selected': False,
                    'is_dir': True,
                    'type_icon': '📁',
                    'name': d,
                    'size_fmt': '-',
                    'created_at': '-',
                    'file_type': 'Folder',
                    'mime_type': '',
                    'id': -1, # placeholder
                    'file_path': f"{current_prefix}{d}",
                    'drive_name': selected_drive
                })
            dirs_df = pd.DataFrame(dirs_data)
        else:
            dirs_df = pd.DataFrame()
            
        # 2. Files
        if not f_df.empty:
            f_df['is_dir'] = False
            f_df['type_icon'] = '📄'
            f_df['size_fmt'] = f_df['size'].apply(format_size)
            f_df['name'] = f_df['file_path'].apply(lambda x: x[len(current_prefix):])
            f_df['selected'] = False
            # Normalize columns
            files_df = f_df
        else:
            files_df = pd.DataFrame()
            
        # Combine
        combined_df = pd.concat([dirs_df, files_df], ignore_index=True)
        
        if not combined_df.empty:
            
            # Reorder columns and render manually
            # Headers
            st.divider()
            h_cols = st.columns([0.5, 0.5, 4, 1.5, 1.5, 2])
            h_cols[0].markdown("**✔**")
            h_cols[2].markdown("**Name**")
            h_cols[3].markdown("**Size**")
            h_cols[4].markdown("**Type**")
            h_cols[5].markdown("**Modified**")
            
            selected_rows_data = []
            
            # Manual Row Rendering
            for idx, row in combined_df.iterrows():
                # Unique key for widgets
                row_key_base = f"row_{row['drive_name']}_{row['file_path']}"
                
                cols = st.columns([0.5, 0.5, 4, 1.5, 1.5, 2])
                
                # 1. Checkbox
                checked = cols[0].checkbox("select", key=f"sel_{row_key_base}", label_visibility="collapsed")
                if checked:
                    selected_rows_data.append(row)
                
                # 2. Icon
                cols[1].write(row['type_icon'])
                
                # 3. Name (Button for Dir, Text for File)
                if row['is_dir']:
                    # Use a button for navigation
                    if cols[2].button(row['name'], key=f"nav_{row_key_base}"):
                        st.session_state.browse_path = row['file_path']
                        st.session_state.browse_page = 1
                        st.rerun()
                else:
                    cols[2].write(row['name'])
                    
                # 4. Size
                cols[3].write(row['size_fmt'])
                
                # 5. Type
                cols[4].write(row['file_type'] if row['file_type'] else "")
                
                # 6. Date (Modified) with Tooltip
                m_date = row.get('file_mtime')
                if pd.isna(m_date): m_date = None
                display_date = m_date.split('T')[0] if m_date else "-"
                
                # Build tooltip
                tt = f"Modified: {m_date if m_date else 'Unknown'}\nAdded: {row.get('created_at', '-')}\nAccessed: {row.get('file_atime', '-')}\nCreated: {row.get('file_ctime', '-')}"
                cols[5].markdown(f'<span title="{tt}">{display_date}</span>', unsafe_allow_html=True)

            # Reconstruct selection DataFrame
            if selected_rows_data:
                browse_selected = pd.DataFrame(selected_rows_data)
                
                st.divider()
                st.info(f"{len(browse_selected)} items selected")
                
                # Render Actions
                render_group_actions(browse_selected, group_map, "All Files", "browse")
                st.divider()
                render_restore_ui(browse_selected, "browse")
            else:
                browse_selected = pd.DataFrame()

            # Pagination
            col_bp1, col_bp2, col_bp3 = st.columns([1, 2, 1])
            with col_bp1:
                if st.session_state.browse_page > 1:
                    if st.button("Prev"):
                        st.session_state.browse_page -= 1
                        st.rerun()
            with col_bp2:
                st.write(f"Page {st.session_state.browse_page} of {(total_browse_files // ROWS_PER_PAGE) + 1}")
            with col_bp3:
                if total_browse_files > st.session_state.browse_page * ROWS_PER_PAGE:
                    if st.button("Next"):
                        st.session_state.browse_page += 1
                        st.rerun()
        else:
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
        # Get dynamic file types
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

    # Pagination state
    if 'page' not in st.session_state:
        st.session_state.page = 1

    # ROWS_PER_PAGE defined globally

    # Construct Query
    base_query = """
        SELECT 
            f.id, 
            f.drive_name, 
            f.file_path, 
            f.size, 
            f.created_at, 
            f.is_original,
            f.file_type,
            f.mime_type,
            f.file_mtime,
            f.file_atime,
            f.file_ctime,
            EXISTS(SELECT 1 FROM group_members gm WHERE gm.file_id = f.id) as has_group
        FROM files f
    """
    params = []
    conditions = []

    # Filter: Drive
    if selected_drive != "All Drives":
        conditions.append("f.drive_name = ?")
        params.append(selected_drive)

    # Filter: File Type
    if filter_file_type != "All Types":
        conditions.append("f.file_type = ?")
        params.append(filter_file_type)

    # Filter: Group
    if selected_group_name != "All Files":
        group_id = group_map[selected_group_name]
        base_query += " JOIN group_members gm_filter ON f.id = gm_filter.file_id"
        conditions.append("gm_filter.group_id = ?")
        params.append(group_id)

    # Filter: Search (FTS)
    if search_query:
        # Use FTS if available, otherwise fallback to LIKE (slow but functional if FTS failed to load)
        # Check if FTS table exists first
        conn = get_db_connection()
        fts_exists = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='files_fts'").fetchone()
        conn.close()
        
        if fts_exists:
            # Join with FTS table
            base_query += " JOIN files_fts fts ON f.id = fts.rowid"
            conditions.append("files_fts MATCH ?")
            
            # Smart Query Logic
            chars = set('*"')
            if any((c in search_query) for c in chars):
                # User provided wildcards or quotes - use as is
                fts_query = search_query
            else:
                # Default to prefix search query
                # "term" * -> matches "term" at start of token
                fts_query = f'"{search_query}" *' 
                
            params.append(fts_query)
        else:
            conditions.append("f.file_path LIKE ?")
            params.append(f"%{search_query}%")

    # Filter: Origin
    if filter_origin == "Originals Only":
        conditions.append("f.is_original = 1")
    elif filter_origin == "Duplicates Only":
        conditions.append("f.is_original = 0")

    # Filter: Date
    date_col = "f.created_at" if filter_date_col == "Date Added" else "f.file_mtime"
    if filter_period != "All Time":
        now = datetime.now()
        if filter_period == "Last 24 Hours":
            cutoff = (now - timedelta(days=1)).isoformat()
            conditions.append(f"{date_col} >= ?")
            params.append(cutoff)
        elif filter_period == "Last 7 Days":
            cutoff = (now - timedelta(days=7)).isoformat()
            conditions.append(f"{date_col} >= ?")
            params.append(cutoff)
        elif filter_period == "Last 30 Days":
            cutoff = (now - timedelta(days=30)).isoformat()
            conditions.append(f"{date_col} >= ?")
            params.append(cutoff)
        elif filter_period == "Custom Range" and len(filter_date_range) == 2:
            start_date, end_date = filter_date_range
            # date_input returns date objects, need to ensure comparison works with text ISO strings
            # We'll use ISO format start and end of day
            s_iso = start_date.isoformat()
            e_iso = end_date.isoformat() + "T23:59:59"
            conditions.append(f"{date_col} >= ? AND {date_col} <= ?")
            params.append(s_iso)
            params.append(e_iso)

    # Filter: Extensions
    if filter_ext:
        # Split by comma
        exts = [e.strip().lower().lstrip('.') for e in filter_ext.split(',') if e.strip()]
        if exts:
            # OR logic for extensions
            ext_conditions = []
            for e in exts:
                ext_conditions.append("f.file_path LIKE ?")
                params.append(f"%.{e}")
            conditions.append(f"({' OR '.join(ext_conditions)})")

    if conditions:
        base_query += " WHERE " + " AND ".join(conditions)

    # Count total results (for pagination)
    count_query = f"SELECT COUNT(*) FROM ({base_query})"
    conn = get_db_connection()
    total_rows = conn.execute(count_query, params).fetchone()[0]
    conn.close()

    # Sorting
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

    # Pagination Limit
    offset = (st.session_state.page - 1) * ROWS_PER_PAGE
    base_query += f" LIMIT {ROWS_PER_PAGE} OFFSET {offset}"

    # Fetch Data
    conn = get_db_connection()
    df = pd.read_sql(base_query, conn, params=params)
    conn.close()

    # Display
    st.markdown(f"**Found {total_rows:,} files**")

    # Custom Data Grid
    if not df.empty:
        # Prepare display data
        df['size_fmt'] = df['size'].apply(format_size)
        df['selected'] = False
        
        # Convert timestamps to proper datetime objects for DatetimeColumn
        for col in ['file_mtime', 'created_at', 'file_atime', 'file_ctime']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Use data editor for selection
        # Reorder columns to put checkbox first
        cols_order = ['selected', 'drive_name', 'file_path', 'size_fmt', 'file_mtime', 'created_at', 'file_atime', 'file_type', 'is_original', 'mime_type', 'file_ctime']
        # Filter to only existing columns
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
                "id": None, # Hide ID
                "size": None, # Hide raw size
                "has_group": None
            },
            hide_index=True,
            width='stretch',
            disabled=[c for c in cols_order if c != "selected"],
            key=f"editor_{st.session_state.page}" # Reset editor on page change
        )
        
        selected_rows = edited_df[edited_df['selected']]
    else:
        selected_rows = pd.DataFrame() # Empty

    # Pagination Controls
    st.divider()
    col_p1, col_p2, col_p3 = st.columns([1, 2, 1])

    with col_p1:
        if st.session_state.page > 1:
            if st.button("Previous Page"):
                st.session_state.page -= 1
                st.rerun()

    with col_p2:
        total_pages = (total_rows // ROWS_PER_PAGE) + 1
        st.markdown(f"<p style='text-align: center'>Page {st.session_state.page} of {total_pages}</p>", unsafe_allow_html=True)

    with col_p3:
        if total_rows > st.session_state.page * ROWS_PER_PAGE:
            if st.button("Next Page"):
                st.session_state.page += 1
                st.rerun()

# --- Shared Action Bar ---
# Handled via functions above

# --- RESTORE ACTIONS FOR SEARCH TAB ---
with tab_search:
    if not selected_rows.empty:
        st.divider()
        st.info(f"{len(selected_rows)} files selected")
        
        render_group_actions(selected_rows, group_map, selected_group_name, "search")
        st.divider()
        render_restore_ui(selected_rows, "search")

