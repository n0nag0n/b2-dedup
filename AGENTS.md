# B2 Dedup — Agent Instructions

Guidance for coding agents working in this repository. Prefer small, behavior-preserving changes. Do not invent new product features unless asked.

## Project overview

Python CLI + Streamlit web UI that backs up files to Backblaze B2 with content-addressed deduplication (SHA-256). Unique files upload once; duplicates become lightweight `.b2ptr` JSON pointers. Local SQLite (`data/b2_dedup.db`) tracks every file occurrence.

## Layout

```
b2_dedup.py          # CLI: upload | download | serve; B2Manager; workers
b2_gui.py            # Streamlit entry (thin assembler)
file_utils.py        # mtime/ctime/atime, mime, file category
gui/                 # Streamlit UI
  state.py           # session_state + basket helpers
  config.py          # data/b2_gui_config.json
  db.py              # shared SQL helpers
  components/        # sidebar, basket bar, group actions
  tabs/              # browse, search, upload
migrations/          # numbered schema migrations (runner.py)
requirements.txt     # b2sdk, tqdm (streamlit+pandas via Dockerfile / manual install)
```

## Commands

```bash
python b2_dedup.py upload /path --drive-name NAME --bucket BUCKET [--scan-only] [--dry-run] [--workers N]
python b2_dedup.py download REMOTE/ --dest /out --bucket BUCKET [--dry-run]
python b2_dedup.py serve [--port 8501]
```

Docker: `cp .env.example .env` then `docker compose up --build`. Host data is `./data` → `/app/data`. Optional `HOST_DRIVES_PATH` mounts into `/host_drives/` for GUI uploads.

## Paths & env

| Item | Location / default |
|---|---|
| Data dir | `B2_DEDUP_DATA_DIR` or `./data` next to `b2_dedup.py` |
| DB | `data/b2_dedup.db` |
| GUI config | `data/b2_gui_config.json` |
| File-count cache | `data/.b2_dedup_cache.json` |
| Pointer extension | `.b2ptr` |
| Remote DB backup | `__b2_dedup_metadata__/b2_dedup.db` |

Credentials (in order): `B2_KEY_ID` + `B2_APPLICATION_KEY` env vars, then b2sdk `SqliteAccountInfo` stored login. Never hardcode secrets.

## Public surface to preserve

- CLI subcommands and flags for `upload` / `download` / `serve`
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
