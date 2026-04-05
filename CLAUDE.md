# B2 Dedup — Claude Instructions

## What this project is

A Python CLI + web UI for deduplicating backups to Backblaze B2. Files are hashed (SHA-256) and only uploaded once; duplicates get lightweight `.b2ptr` JSON pointer files that reference the original. A local SQLite database tracks everything. The web UI (`serve` command) lets you browse, search, and download files from that database.

## Key files

| File | Purpose |
|---|---|
| `b2_dedup.py` | CLI entry point — `upload`, `download`, `serve` actions; `B2Manager` class; DB init; all backend logic |
| `b2_gui.py` | Streamlit web UI — browse, search, persistent download basket |
| `file_utils.py` | File metadata extraction (mtime, ctime, atime, mime type, file category) |
| `update_db_and_rescan.py` | Utility for re-scanning drives against the existing DB |
| `requirements.txt` | `b2sdk==2.10.2`, `tqdm==4.67.1` — GUI also needs `streamlit` and `pandas` |

## Running the project

```bash
# CLI
python b2_dedup.py upload /path/to/drive --drive-name MyDrive --bucket my-bucket
python b2_dedup.py download MyDrive/ --dest /restore --bucket my-bucket

# Web UI (opens http://localhost:8501)
python b2_dedup.py serve [--port 8501]
```

The `serve` command shells out to `streamlit run b2_gui.py`.

## Important constants / paths

- **Database:** `~/b2_dedup.db` (`b2_dedup.DB_PATH`)
- **File count cache:** `~/.b2_dedup_cache.json`
- **GUI config** (bucket name, etc.): `~/.b2_gui_config.json`
  - `bucket_name` — B2 bucket name
  - `db_backup_local_mtime` — float mtime of local DB at time of last backup (used to detect local changes)
  - `db_backup_local_size` — int size of local DB at time of last backup
- **Pointer file extension:** `.b2ptr` (`b2_dedup.POINTER_EXTENSION`)
- **DB backup remote path:** `__b2_dedup_metadata__/b2_dedup.db` (`b2_gui.DB_REMOTE_PATH`)
- **Default workers:** 10 threads
- **Chunk size:** 4 MB

## B2 credentials

Tried in order: stored B2 CLI credentials (`~/.b2/account_info` via `SqliteAccountInfo`) → env vars `B2_KEY_ID` / `B2_APPLICATION_KEY`. No hardcoded credentials anywhere.

## Database schema (SQLite, FTS5)

**`files`** — one row per file occurrence (originals AND duplicates):
- `id`, `hash` (SHA-256), `size`, `drive_name`, `file_path`, `upload_path` (B2 path, originals only)
- `is_original` (1 = uploaded copy, 0 = pointer), `created_at`
- `file_mtime`, `file_ctime`, `file_atime`, `mime_type`, `file_type` (category string)

**`groups` / `group_members`** — user-defined file grouping (many-to-many)

**`files_fts`** — FTS5 virtual table on `file_path`, kept in sync via triggers

Key indexes: `(drive_name, file_path)` covering index is the primary browse index.

## Pointer file format

```json
{
  "type": "b2_dedup_pointer",
  "version": 1,
  "original_hash": "abc123...",
  "original_path": "MyDrive/path/to/original.ext",
  "pointer_created": "2026-01-21T15:00:00Z"
}
```

Stored in B2 as `original/path.ext.b2ptr`. Downloads resolve the pointer then fetch `original_path`.

## GUI architecture (b2_gui.py)

Pure Streamlit — no custom HTML/JS/CSS. All state lives in `st.session_state`.

**Session state keys:**
- `basket_file_ids` — `set` of int file IDs queued for download
- `basket_folder_paths` — `set` of `(drive_name, path)` tuples queued for download
- `browse_path` — current folder path in Browse tab
- `browse_page`, `page` — pagination for Browse / Search tabs
- `db_init_done` — one-time DB init guard
- `db_backup_status` — cached dict `{b2_info, local_mtime, local_size}` for the DB Backup sidebar panel; delete from session state to force a re-check

**Basket / download flow:**
1. Checking any file or folder row adds it to `basket_file_ids` / `basket_folder_paths` — persists across navigation and search
2. `get_basket_all_ids()` resolves folder paths to file IDs via DB query
3. Basket bar (above tabs) shows count + uncompressed size + Download ZIP button
4. Download: files fetched from B2 → written into a `tempfile` ZIP on disk → served via `st.download_button`
5. Duplicates resolved via their `.b2ptr` pointer file before downloading

**Browse tab directory listing** uses a skip-scan strategy against the `(drive_name, file_path)` index — avoids full table scans for large drives.

## Common patterns

- Always call `conn.close()` after every DB operation — no context managers used here, connections are short-lived per operation.
- `format_size(bytes)` for human-readable sizes.
- `b2_dedup.sanitize_b2_path(path)` before any B2 remote path construction.
- `resolve_selection_to_ids(df)` expands directory rows (where `is_dir=True`) into individual file IDs.
- `get_selection_size(ids)` for a quick `SUM(size)` estimate from the DB.
- `B2Manager.get_file_info(remote_path)` returns `{'upload_timestamp_ms': int, 'size': int}` or `None`.

## b2sdk gotchas

- `bucket.list_file_versions(file_name, fetch_count=None)` — the limit parameter is `fetch_count`, **not** `max_versions`. Passing `max_versions` raises `TypeError` which bare `except` silently swallows, making every existence check return `False`/`None`.
- B2 large-file uploads use an internal `ThreadPoolExecutor`. Progress listener callbacks (`bytes_completed`, `close`) fire on those threads, not the main thread. In Streamlit this causes `NoSessionContext`. Fix: capture `get_script_run_ctx()` on the main thread and call `add_script_run_ctx(threading.current_thread(), ctx)` at the top of each listener method.
- When passing a `st.data_editor` dataframe, any column you read back from the edited result (e.g. `row['id']`) **must** be included in the dataframe slice passed to the widget — hiding it via `column_config: None` is not enough if it was omitted from `cols_order`.

## No test suite

There are no automated tests. Verify changes by running `python b2_dedup.py serve` and exercising the UI, or by running CLI commands with `--dry-run`.
