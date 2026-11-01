import streamlit as st


def render_upload_tab(_selected_drive, _selected_group_name, _group_map):
    st.header("Upload moved")
    st.markdown(
        """
Ingest now runs as a background job in StorageInsights so this page no longer
spawns a blocking `upload` subprocess.

Open **Ingest** in the main app (`python b2_dedup.py serve`, then `/ingest`),
or run the same engine from the command line:

```
python b2_dedup.py ingest /path/to/drive --drive-name MyDrive --bucket my-bucket
```

Use `--dry-run` or `--scan-only` first if you want a preview without uploading.
        """
    )
