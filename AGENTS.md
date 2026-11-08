# StorageInsights — Agent Instructions

Guidance for coding agents working in this repository. Prefer small, behavior-preserving changes. Do not invent new product features unless asked.

The product name is **StorageInsights**. The git repo may still be `b2-dedup`.

## Project overview

Home-network vault for a Backblaze B2 backup with content-addressed deduplication (SHA-256). Unique files upload once; duplicates become lightweight `.b2ptr` JSON pointers. Local SQLite (`data/b2_dedup.db`) tracks every file occurrence. The web UI is FastAPI + Jinja + Alpine (no JS build step). Streamlit (`gui/`, `serve --legacy`) remains until leftover sidebar-only extras are gone; ingest, restore, verify, and Settings live in the new app.

## Layout

```
b2_dedup.py          # thin CLI: serve | upload/ingest | download/restore | verify | dates | thumbs
core/                # B2, hash, ingest, scratch staging, restore, verify, catalog backup, junk rules, time, thumbs, downloads
web/                 # FastAPI + Jinja templates + vendored Alpine
file_utils.py        # mtime/ctime/atime, mime, file category
gui/                 # legacy Streamlit UI (serve --legacy)
b2_gui.py            # Streamlit entry
migrations/          # numbered schema migrations (runner.py)
tests/               # pytest suite
```

## Commands

```bash
python b2_dedup.py serve [--port 8501]          # StorageInsights
python b2_dedup.py serve --legacy [--port 8501] # previous Streamlit UI

Web: GET /search (names, one drive's folders, or moments; junk hidden),
GET /file/{id}, old /browse /library /timeline /moment redirect into /search,
restore set at /restore-set, ingest wizard at /ingest (preview then job),
scratch uploads at /scratch (browser files/folders → normal ingest job;
a custom staging folder's name is the default vault prefix, e.g. Scratch/asus-backup/…),
POST /catalog/move relocates a file or folder in the catalog and in B2 (job type relocate),
jobs at /jobs/{id}, Settings at /settings (bucket, catalog backup, verify,
date backfill, thumbnails, browser-download limits, Scratch drive name/retention).
Home shows catalog-backup and last-verify health (no B2 call on load).

Downloads never require a local restore folder:
- `GET /download/{file_id}` streams one file (pointer-resolved) to the browser.
- `GET /restore-set/download.zip` streams the restore set as a ZIP (no staging).
- `POST /restore-set/export` builds a resumable ZIP job under `data/exports/`,
  served at `GET /exports/{job_id}.zip` (Range/resume supported).
- Caps: `download_max_files` (1000), `download_max_bytes` (5 GiB),
  `download_stream_threshold` (500 MiB), `download_expiry_hours` (24) in
  `data/b2_gui_config.json`. Prepared archives are swept on startup and before
  each build; exports live under `data/exports/` (gitignored with `data/`).
`python b2_dedup.py dates` fills `best_taken_at` (folder + mtime; no B2).
`python b2_dedup.py thumbs [--limit N]` builds JPEG previews from B2.
FTS: `files_fts MATCH ?` via `id IN (SELECT rowid FROM files_fts WHERE …)`.
Do not use SQL LIKE `'._%'` for AppleDouble — `_` is a wildcard; use GLOB.
python b2_dedup.py upload /path --drive-name NAME --bucket BUCKET [--scan-only] [--dry-run] [--workers N]
python b2_dedup.py ingest …                     # alias for upload; same core.ingest as the web job
python b2_dedup.py download REMOTE/ --dest /out --bucket BUCKET [--dry-run]
python b2_dedup.py verify [--bucket BUCKET] [--sample 200] [--full]
python b2_dedup.py dates [--limit N]
python b2_dedup.py thumbs [--bucket BUCKET] [--limit N]
```

Run with a venv, not Docker: `python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt && python b2_dedup.py serve`. Ingest lists OS mount points; extra folders live in Settings (`ingest_extra_roots` in `data/b2_gui_config.json`).

## Paths & env

| Item | Location / default |
|---|---|
| Data dir | `B2_DEDUP_DATA_DIR` or `./data` next to `b2_dedup.py` |
| DB | `data/b2_dedup.db` |
| GUI config | `data/b2_gui_config.json` |
| File-count cache | `data/.b2_dedup_cache.json` |
| Extra ingest folders | `ingest_extra_roots` in `data/b2_gui_config.json` |
| Prepared ZIP exports | `data/exports/` |
| Scratch upload staging | `data/scratch/<batch-id>/` |
| Pointer extension | `.b2ptr` |
| Remote DB backup | `__b2_dedup_metadata__/b2_dedup.db` |
| Default workers | 10 threads |
| Chunk size | 4 MB |

Config keys in `data/b2_gui_config.json`: `bucket_name`, `ingest_extra_roots`, `db_backup_local_mtime`, `db_backup_local_size`, `download_max_files`, `download_max_bytes`, `download_stream_threshold`, `download_expiry_hours`.

Credentials (in order): `B2_KEY_ID` + `B2_APPLICATION_KEY` env vars (a project-root `.env` is loaded automatically; `.env.example` documents them), then b2sdk `SqliteAccountInfo` stored login (`~/.b2_account_info`, then `~/.config/b2/account_info`). Never hardcode secrets.

## Public surface to preserve

- CLI subcommands and flags for `upload` / `download` / `serve` / `verify`
- Pointer JSON shape (`type`, `version`, `original_hash`, `original_path`, `pointer_created`)
- Remote naming: originals at `{drive}/{relpath}`, pointers as `{path}.b2ptr`
- Credential resolution order above
- DB columns used by GUI/search (do not break without a migration)

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

Stored in B2 as `original/path.ext.b2ptr`. Downloads resolve the pointer, then fetch `original_path`.

## Schema notes

- Migrations: `migrations/00N_*.py` with `up(conn)`; applied once via `schema_migrations`, run from `b2_dedup.init_db()`
- `files` — one row per occurrence (originals AND duplicates): `id`, `hash`, `size`, `drive_name`, `file_path`, `upload_path` (originals only), `is_original`, `created_at`, `file_mtime`, `file_ctime`, `file_atime`, `mime_type`, `file_type`
- `groups` / `group_members` — user-defined file grouping
- `files_fts` — FTS5 virtual table on `file_path` with insert/update/delete triggers
- `file_features` exists for future use; no app code reads/writes it yet
- Primary browse index: `(drive_name, file_path)`
- **FTS MATCH:** with `JOIN files_fts fts ON …`, write `files_fts MATCH ?` — **not** `fts MATCH ?` (SQLite error: `no such column: fts`)

## Coding conventions

- Short-lived SQLite connections: open → query → `conn.close()`. No connection context managers in this codebase.
- Call `b2_dedup.sanitize_b2_path()` for any remote path.
- File inserts on upload go through `_insert_file_record()` in `b2_dedup.py`.
- GUI DB helpers live in `gui/db.py`; tab-specific SQL may stay local.
- Streamlit state: use existing keys (`basket_file_ids`, `basket_folder_paths`, `browse_path`, `browse_page`, `page`, `db_init_done`, `db_backup_status`, `upload_dir`); clear `db_backup_status` to force backup panel refresh.
- `st.data_editor`: any column you read back (especially `id`) must remain in the dataframe passed to the widget.
- Prefer `except Exception` over bare `except:` (b2sdk TypeErrors get swallowed otherwise).

## b2sdk pitfalls

1. `list_file_versions(..., fetch_count=1)` — the limit parameter is `fetch_count`, not `max_versions`. Passing `max_versions` raises `TypeError`, which a broad `except Exception` silently swallows, making every existence check return `False`/`None`.
2. Large-file uploads use an internal `ThreadPoolExecutor`; progress listener callbacks (`bytes_completed`, `close`) fire on worker threads. In Streamlit this causes `NoSessionContext`. Fix: capture `get_script_run_ctx()` on the main thread and call `add_script_run_ctx(threading.current_thread(), ctx)` at the top of each listener method.

## Legacy Streamlit UI (`gui/`)

Entry: `b2_gui.py` → modules under `gui/`. Roles: `gui/state.py` (session state, basket helpers), `gui/config.py` (`b2_gui_config.json`), `gui/db.py` (shared queries), `gui/components/{sidebar,basket,group_actions}.py`, `gui/tabs/{browse,search,upload}.py`. Browse uses a skip-scan against `(drive_name, file_path)` to avoid full table scans. Upload log uses a small HTML tail panel; all state lives in `st.session_state`.

Basket flow: checked rows add to `basket_file_ids` / `basket_folder_paths`; `get_basket_all_ids()` resolves folders to IDs; duplicates resolve via `.b2ptr` before download.

## Verification

Tests live in `tests/` and run with pytest (`pip install pytest`, then
`venv/bin/python -m pytest -q`). After changes:

- CLI: `python b2_dedup.py upload … --dry-run` / `--scan-only` where appropriate
- UI: `python b2_dedup.py serve` and exercise search (including a drive and moments), restore set, downloads

Do not commit secrets, `data/`, `venv/`, or large local DB backups (`*.prev`, accidental `data (copy)/` trees).
