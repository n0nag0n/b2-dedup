import streamlit as st
import sqlite3
import pandas as pd
import os
import sys
from pathlib import Path
from datetime import datetime
import json
import subprocess

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
    
    # Handle direct files
    # Check if 'is_dir' column exists
    if 'is_dir' in selected_rows.columns:
        files = selected_rows[~selected_rows['is_dir']]
        dirs = selected_rows[selected_rows['is_dir']]
    else:
        files = selected_rows
        dirs = pd.DataFrame()
        
    if not files.empty and 'id' in files.columns:
        # Dropna() because fake rows might have None
        ids.update(files['id'].dropna().astype(int).tolist())
        
    if not dirs.empty:
        for _, row in dirs.iterrows():
            d_drive = row['drive_name']
            d_path = row['file_path']
            if not d_path.endswith('/'): d_path += '/'
            
            # Find files in dir
            q = "SELECT id FROM files WHERE drive_name = ? AND file_path LIKE ?"
            found = conn.execute(q, (d_drive, f"{d_path}%")).fetchall()
            ids.update(f[0] for f in found)
            
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
    
    restore_dest = cols[0].text_input("Restore to (Local Path)", value=str(Path.home() / "Downloads"), key=f"dest_{key_suffix}")
    bucket_name = cols[1].text_input("B2 Bucket Name", key=f"buck_{key_suffix}")
    
    if cols[2].button("Restore", key=f"btn_rest_{key_suffix}"):
        if not bucket_name:
            st.error("Bucket Name required")
            return

        progress_text = "Restoring files..."
        my_bar = st.progress(0, text=progress_text)
        try:
            b2 = b2_dedup.B2Manager(bucket_name)
            dest_path = Path(restore_dest)
            
            all_ids = resolve_selection_to_ids(selected_rows)
            total = len(all_ids)
            done = 0
            
            my_bar = st.progress(0, text=f"Restoring 0/{total}...")
            
            for fid in all_ids:
                conn = get_db_connection()
                file_rec = conn.execute("SELECT hash, is_original, upload_path, drive_name, file_path FROM files WHERE id = ?", (fid,)).fetchone()
                conn.close()
                if not file_rec: continue
                
                f_hash, is_orig, up_path, d_name, f_path = file_rec
                remote_path = f"{d_name}/{f_path}"
                local_path = dest_path / f_path
                
                if is_orig:
                    b2.download_file_on_path(remote_path, local_path) if hasattr(b2, 'download_file_on_path') else b2.download_file_to_path(remote_path, local_path)
                else:
                    try:
                        ptr_content = b2.download_file_content(remote_path + ".b2ptr")
                        pointer = json.loads(ptr_content)
                        b2.download_file_to_path(pointer['original_path'], local_path)
                    except:
                        pass
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
    
    # 2. File Type Filter
    try:
        conn = get_db_connection()
        # Some DBs might not have file_type column if old, handle gracefully
        types = pd.read_sql("SELECT DISTINCT file_type FROM files ORDER BY file_type", conn)['file_type'].tolist()
        conn.close()
        # Filter None/Empty
        types = [t for t in types if t]
        selected_file_type = st.selectbox("File Type", ["All Types"] + types)
    except:
        selected_file_type = "All Types"
    
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
                f.file_type, f.mime_type
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
            h_cols[5].markdown("**Date**")
            
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
                
                # 6. Date
                d_str = row['created_at'].split('T')[0] if row['created_at'] else ""
                cols[5].write(d_str)

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
            "Date (Newest)", "Date (Oldest)", 
            "Size (Largest)", "Size (Smallest)",
            "Type (A-Z)", "Type (Z-A)"
        ])

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
    if selected_file_type != "All Types":
        conditions.append("f.file_type = ?")
        params.append(selected_file_type)

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

    if conditions:
        base_query += " WHERE " + " AND ".join(conditions)

    # Count total results (for pagination)
    count_query = f"SELECT COUNT(*) FROM ({base_query})"
    conn = get_db_connection()
    total_rows = conn.execute(count_query, params).fetchone()[0]
    conn.close()

    # Sorting
    sort_map = {
        "Date (Newest)": "f.created_at DESC",
        "Date (Oldest)": "f.created_at ASC",
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
        
        # Use data editor for selection
        edited_df = st.data_editor(
            df,
            column_config={
                "selected": st.column_config.CheckboxColumn("Select", default=False),
                "drive_name": "Drive",
                "file_path": "Path",
                "size_fmt": "Size",
                "created_at": "Datestamp",
                "is_original": "Orig?",
                "file_type": "Type",
                "mime_type": "MIME",
                "id": None, # Hide ID
                "size": None, # Hide raw size
                "has_group": None
            },
            hide_index=True,
            width='stretch',
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

