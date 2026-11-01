# StorageInsights — Agent Instructions

Guidance for coding agents working in this repository. Prefer small, behavior-preserving changes. Do not invent new product features unless asked.

The product name is **StorageInsights**. The git repo may still be `b2-dedup`.

## Project overview

Home-network vault for a Backblaze B2 backup with content-addressed deduplication (SHA-256). Unique files upload once; duplicates become lightweight `.b2ptr` JSON pointers. Local SQLite (`data/b2_dedup.db`) tracks every file occurrence. The web UI is FastAPI + Jinja + Alpine (no JS build step). Streamlit (`gui/`, `serve --legacy`) remains until leftover sidebar-only extras are gone; ingest, restore, verify, and Settings live in the new app.

## Layout

```
b2_dedup.py          # thin CLI: serve | upload/ingest | download/restore | verify | dates | thumbs
core/                # B2, hash, ingest, restore, verify, catalog backup, junk rules, time, thumbs
web/                 # FastAPI + Jinja templates + vendored Alpine
file_utils.py        # mtime/ctime/atime, mime, file category
gui/                 # legacy Streamlit UI (serve --legacy)
b2_gui.py            # Streamlit entry
migrations/          # numbered schema migrations (runner.py)
```

## Commands

```bash
python b2_dedup.py serve [--port 8501]          # StorageInsights
python b2_dedup.py serve --legacy [--port 8501] # previous Streamlit UI

Web: GET /search (all drives, junk hidden), GET /browse, GET /file/{id},
GET /library (file grid + type filter), GET /timeline (moment cards), GET /moment,
restore set at /restore-set, ingest wizard at /ingest (preview then job),
jobs at /jobs/{id}, Settings at /settings (bucket, catalog backup, verify,
date backfill, thumbnails). Home shows catalog-backup and last-verify health
(no B2 call on load).
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
| Pointer extension | `.b2ptr` |
| Remote DB backup | `__b2_dedup_metadata__/b2_dedup.db` |

Credentials (in order): `B2_KEY_ID` + `B2_APPLICATION_KEY` env vars, then b2sdk `SqliteAccountInfo` stored login. Never hardcode secrets.

## Public surface to preserve

- CLI subcommands and flags for `upload` / `download` / `serve` / `verify`
- Pointer JSON shape (`type`, `version`, `original_hash`, `original_path`, `pointer_created`)
- Remote naming: originals at `{drive}/{relpath}`, pointers as `{path}.b2ptr`
- Credential resolution order above
- DB columns used by GUI/search (do not break without a migration)

## Schema notes

- Migrations: `migrations/00N_*.py` with `up(conn)`; applied once via `schema_migrations`
- Primary browse index: `(drive_name, file_path)`
- FTS5 table `files_fts` on `file_path` with insert/update/delete triggers
- **FTS MATCH:** with `JOIN files_fts fts ON …`, write `files_fts MATCH ?` — **not** `fts MATCH ?` (SQLite error: `no such column: fts`)
- `file_features` exists for future use; no app code reads/writes it yet

## Coding conventions

- Short-lived SQLite connections: open → query → `conn.close()`. No connection context managers in this codebase.
- Call `b2_dedup.sanitize_b2_path()` for any remote path.
- File inserts on upload go through `_insert_file_record()` in `b2_dedup.py`.
- GUI DB helpers live in `gui/db.py`; tab-specific SQL may stay local.
- Streamlit state: use existing keys (`basket_file_ids`, `basket_folder_paths`, etc.); clear `db_backup_status` to force backup panel refresh.
- `st.data_editor`: any column you read back (especially `id`) must remain in the dataframe passed to the widget.
- Prefer `except Exception` over bare `except:` (b2sdk TypeErrors get swallowed otherwise).

## b2sdk pitfalls

1. `list_file_versions(..., fetch_count=1)` — parameter is `fetch_count`, not `max_versions`.
2. Large-file upload progress listeners run on worker threads. In Streamlit, reattach context with `add_script_run_ctx` using a main-thread `get_script_run_ctx()` capture.

## Verification

No automated test suite. After changes:

- CLI: `python b2_dedup.py upload … --dry-run` / `--scan-only` where appropriate
- UI: `python b2_dedup.py serve` and exercise browse, search, basket, sidebar

Do not commit secrets, `data/`, `venv/`, or large local DB backups (`*.prev`, accidental `data (copy)/` trees).
