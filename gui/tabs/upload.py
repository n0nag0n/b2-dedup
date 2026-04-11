import streamlit as st
import subprocess
import os
import sys
from gui.config import load_gui_config, save_gui_config

def render_upload_tab(selected_drive, selected_group_name, group_map):
    st.header("☁️ Upload Local Drive")
    
    st.markdown("""
    Select a directory from your mounted host drives. If you don't see your drives here, 
    ensure `HOST_DRIVES_PATH` is configured correctly in `.env` and `docker-compose.yml`, then restart the container.
    """)
    
    config = load_gui_config()
    default_bucket = config.get("bucket_name", "")
    
    with st.form("upload_form"):
        col1, col2 = st.columns(2)
        with col1:
            upload_path = st.text_input("Local Upload Path", value="/host_drives/", help="Path inside the Docker container")
            drive_name = st.text_input("Drive Name", value="", help="The top-level drive folder in B2")
        
        with col2:
            bucket_name = st.text_input("B2 Bucket", value=default_bucket)
            workers = st.number_input("Workers", min_value=1, max_value=100, value=10)
            
        st.write("Options")
        col_opt1, col_opt2, col_opt3 = st.columns(3)
        with col_opt1:
            scan_only = st.checkbox("Scan Only (No Upload)", value=False)
        with col_opt2:
            dry_run = st.checkbox("Dry Run", value=False)
        with col_opt3:
            verbose = st.checkbox("Verbose Output", value=True)
            
        submit = st.form_submit_button("Start Upload", type="primary")
        
    if submit:
        if not upload_path or not drive_name or not bucket_name:
            st.error("Upload Path, Drive Name, and Bucket are required.")
            return
            
        if not os.path.exists(upload_path):
            st.error(f"Path not found: `{upload_path}`")
            return
            
        if bucket_name != default_bucket:
            config["bucket_name"] = bucket_name
            save_gui_config(config)
            
        cmd = [
            sys.executable, "b2_dedup.py", "upload",
            upload_path,
            "--drive-name", drive_name,
            "--bucket", bucket_name,
            "--workers", str(int(workers))
        ]
        
        if scan_only:
            cmd.append("--scan-only")
        if dry_run:
            cmd.append("--dry-run")
        if verbose:
            cmd.append("--verbose")
            
        st.write("### Upload Log")
        log_container = st.empty()
        
        # Start subprocess
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        log_lines = []
        for line in process.stdout:
            # Clean up terminal control characters emitted by tqdm or other outputs
            clean_line = line.replace('\r', '').rstrip()
            if clean_line:
                log_lines.append(clean_line)
                if len(log_lines) > 500:
                    log_lines.pop(0) # Keep tail to prevent out of memory in browser
                
                log_container.code('\n'.join(log_lines), language='bash')
                
        process.wait()
        if process.returncode == 0:
            st.success("Upload task completed successfully!")
            st.balloons()
        else:
            st.error(f"Upload task failed with exit code: {process.returncode}")
