# Rebuild as StorageInsights, a personal life vault

## Context

The project is a working Backblaze B2 backup of **1.18 million files / 19 drives / ~5.4 TB logical / ~3.3 TB unique**. Dedup, pointers, FTS, Docker, and the CLI ingest engine already work. The Streamlit UI does not match the real job: take a pile of old drives, make the contents searchable, and make a lifetime of files *make sense*.

The product name is **StorageInsights**. The git repo can stay `b2-dedup` until a rename is worth the hassle. UI, docs, Docker service, and page titles use StorageInsights.

It started as a CLI, grew flags, then grew a Streamlit UI beside it. That split is why the repo feels disjointed. StorageInsights is a **home-network web app**. The CLI should shrink to a thin entry point (`serve`, optional `worker` / unattended ingest), not a second product.

**Mission Stories comparison (this is the north star, not phase 1):** `~/projects/mission-stories-php` turns handwritten journals into a searchable story: upload → extract → people/places/events → search / “ask the archive.” This vault should do the same for *files*: ingest old disks → catalog → time + folders + later vision/embeddings → moments and correlations. Dedup is the ingestion plumbing (and the reason 2 TB is not stored twice). The value is “I know I have that photo somewhere” and, later, “what was this year of our life?”

That is a real product niche if it is ever monetized: people with boxes of old hard drives, CDs, and camera cards from the pre-iCloud decade. Do not build multi-tenant SaaS now. Leave seams (jobs, config, optional auth, no hardcoded family) so tenancy can be added later.

**Constraints from you**
- Backend: FastAPI.
- Frontend: server-rendered Jinja + Alpine.js. **No React/Vue/Svelte, no JS bundler, no npm build step.**
- Home / home-network tool, single purpose. SaaS is a possible future, not the current design.
- Search defaults to **all drives**. Drive is an optional filter. You will not remember which disk something lived on.
- Junk: **do not upload** `node_modules`, `venv`, `vendor`, AppleDouble `._*`, `.DS_Store`, Spotlight/Trashes/fseventsd, `Thumbs.db`, `__MACOSX`. **Keep `.git` / `.hg` in the backup** so a restored folder is complete; hide them from default search.
- Embeddings for images/video are desired later; `file_features` already exists for this.
- Web UI, not a TUI.
- Tone: no vulgar language in this project’s conversation or UI copy.

**What we are not doing**
- Rewriting the backup engine in another language.
- Switching off B2 or changing the `.b2ptr` pointer format.
- Replacing this with restic/kopia (opaque repos would kill browse-by-real-path).
- Building users, billing, or sharing in the first ship.
- Keeping Streamlit around as a long-term UI.

---

## Product identity

**Name:** **StorageInsights**. Header, browser title, Docker service comment, README. Backup/dedup stays in the subtitle and Settings, not the home headline. Repo folder may remain `b2-dedup` for now.

**Jobs to be done (in order of daily use)**
1. Find a file or photo you know exists *somewhere*.
2. Restore a file or folder to a local path (not “ZIP in the browser”).
3. Ingest another old disk safely, with a preview of what will be skipped.
4. Trust that B2 still has what the catalog claims.
5. Later: flip through time and see a life, not a spreadsheet.

---

## Recommended approach

One Python process: FastAPI serves Jinja pages and a small JSON/HTML-partial API. The existing hash / B2 / pointer / SQLite logic moves into a `core` package. Streamlit (`b2_gui.py`, `gui/`) is deleted once the new pages cover browse, search, ingest, restore, and settings.

```
b2_dedup.py              # thin CLI: serve | worker | ingest | restore | verify
core/
  b2.py                  # B2Manager (moved)
  hash.py                # sha256 + incremental skip
  pointers.py            # .b2ptr create/resolve
  junk.py                # ingest-exclude vs search-hide rules
  ingest.py              # upload / scan-only / dry-run workers
  restore.py             # stream restore to a dest path
  verify.py              # catalog vs B2
  catalog.py             # shared SQL (today’s gui/db.py + more)
  time.py                # best-effort “when” (EXIF > folder date > mtime)
web/
  app.py                 # FastAPI factory, static, templates
  routes/                # home, search, browse, file, jobs, settings
  templates/             # Jinja layouts + pages
  static/                # vendored alpine.min.js, app.css, app.js
jobs.py                  # SQLite job queue + worker loop
migrations/              # keep numbered runner; add 003+
file_utils.py            # keep; extend for EXIF later
```

Run with a local venv: `python b2_dedup.py serve`. `./data` holds the live catalog. Ingest lists OS mount points (plus extra folders from Settings). No Docker.

### Frontend rules (non-negotiable)

- Pages are HTML from Jinja. Forms are real forms. Lists are real links.
- Alpine.js is vendored in `web/static/` (home network should work with no CDN).
- Alpine is only for: selection/basket, filter chips, lightbox, job-log polling, “add folder” confirmations.
- Progressive enhancement: search and browse must work with JS off (GET query strings + pagination).
- No npm, no Vite, no Tailwind build. A single `app.css` (small custom or vendored Pico-like base).
- HTML partials (`/search/results`) are fine if we want Alpine `fetch` without a SPA.

### Why this stack over a SPA

You already rejected the extra build layer, and Mission Stories already proves the pattern: Slim + Twig pages, search as GET, inspector as a show page. FastAPI + Jinja is the Python equivalent. At 1.18M rows the server paginates; the browser never sees the whole table.

---

## Key decisions

1. **One app, thin CLI.** `serve` is the product. `ingest` / `restore` / `verify` stay as commands that call `core` (cron, SSH, recovery). They are not a parallel UX. All the Streamlit-only flags become Settings + job options.

2. **Search is global.** Default scope is every drive. Sidebar drive picker goes away as a gate. Drive, type, date, and “has duplicates” are filters. Browse-by-folder still needs a drive (or a virtual “all roots” list), but finding something does not.

3. **Two junk policies, not one.**
   - **Ingest skip** (never hash/upload): `node_modules`, `venv`, `.venv`, `vendor`, `__pycache__`, `._*`, `.DS_Store`, `.Spotlight-*`, `.Trashes`, `.fseventsd`, `Thumbs.db`, `__MACOSX`.
   - **Search hide (default on):** those, plus `.git/` and `.hg/` path prefixes. Restoring a folder still includes `.git` / `.hg`.
   - Existing 411k AppleDouble rows already in the DB: mark + hide; do not mass-delete from B2 unless you later run an explicit cleanup job.

4. **Restore to disk, not ZIP-in-RAM.** Settings has `restore_dest` (already in `b2_gui_config.json` and unused). Basket → “Restore to folder” streams files via the worker. Browser download is allowed only for a small selection (size/count cap). Adding `big-bertha/` to a ZIP in session memory is a hard no.

5. **Jobs are first-class.** Ingest, restore, verify, thumbnail, embed, DB backup all become rows in a `jobs` table with status + log tail. The UI never blocks on a subprocess the way Streamlit does. This is also the SaaS seam.

6. **Incremental ingest.** Skip hash when `(drive_name, file_path)` exists and `size` + `mtime` match. Today `already_tracked` skips forever even if the file changed.

7. **Story layer is phase 4+, not a rewrite of ingest.** The Mission Stories analog is real, but it needs EXIF, folder-name moments, thumbnails, then embeddings. File `mtime` alone is a weak narrator (the catalog already has 1985 / 1970 / 2038 timestamps). Do not sell “your life story” until date quality is better than filesystem mtime.

8. **SaaS later, seams now.** No `user_id` on every table yet. Use config + env, a jobs table, optional shared-password auth, and keep `file_features` generic. When tenancy arrives, wrap the catalog rather than rewrite pages.

9. **Optional LAN password, no accounts.** Env `VAULT_PASSWORD`. Empty or unset = open on the LAN. If set, cookie session after a simple login form. Enough for a home NAS; replaceable with real users later. **Confirmed.**

---

## The Mission Stories angle (honest take)

Yes, this project can be a sibling of Mission Stories. They are the same product idea on different artifacts:

| Mission Stories | StorageInsights |
|---|---|
| A box of handwritten journals | A box of old drives and camera cards |
| OCR → cleaned text | Ingest → catalog + EXIF + later vision |
| People / places / events | Moments (folder titles), duplicate clusters, later faces/scenes |
| Search entries | Search across all drives |
| “Ask the archive” | “Ask StorageInsights” (after there is something to retrieve) |

What is *already* story-shaped in your data, without AI:
- Folder names: `July 2nd the triplets are here!`, `They're Both Home!`, `old-collier-family-videos`
- 406k images (56k Canon CR2) and 13k videos
- The same photo existing 18–28 times (that is a “this mattered” signal)
- 19 named physical artifacts as chapters

What is **not** a story yet:
- 411k `._*` files and 34k `node_modules` rows
- `mtime` as the only clock
- Drive-gated browse
- No previews

**Recommended story pipeline (after the vault UI works)**
1. **When:** EXIF `DateTimeOriginal` for photos; parse dates out of folder names; fall back to `mtime`. Store `best_taken_at` on the file or in `file_features`.
2. **Moments:** group by (best date + parent folder title). `July 2nd the triplets are here!` becomes a moment card, not 19 copies of `IMG_8730.JPG`.
3. **Thumbnails / CR2 previews:** library and timeline are dead without them. Store under `data/thumbs/` or a B2 prefix; status in `file_features`.
4. **Embeddings:** CLIP (or similar) on images; later keyframe a video. Query: “birthday cake”, “hospital”, “beach”. Use `file_features` (`embedding_clip_v1`). This is a background job, GPU optional, can run overnight on the home box.
5. **Ask StorageInsights:** same shape as `ArchiveAskService` — retrieve moments/files, then a grounded model answer with citations (links to files). Do not invent a chat UI before retrieval works.

Monetization, if you ever go there: “Ship us (or point us at) the old drives. We return a searchable family archive.” That is not competing with iCloud Photos. It is competing with a closet of disks.

---

## UX to ship (replace Streamlit)

**Home** — StorageInsights overview. Drive cards (count, unique vs duplicate bytes, last ingest). Dedup savings. Catalog backup status. Verify health. Recent jobs. Search box is the first thing on the page.

**Search** — GET `/search?q=`. FTS across all drives. Default hide junk / VCS. Filters: type, extension, date (`best_taken_at` when present), originals vs copies, optional drive. Results show drive as a badge, not as the axis. Click → inspector.

**Browse** — Finder-like folder pages. Breadcrumbs. Folder add-to-restore-set. No 500-folder hard cap (paginate or continue-token the skip-scan). Works per drive once you have opened one.

**Inspector** — one file: preview when we have a thumb, size, hashes, **every other path with the same hash**, download/restore this file, restore containing folder.

**Restore set** (rename of “basket”) — count + size. Restore to `restore_dest` as a job. Optional small browser download with a cap.

**Ingest** — plugged-in disk cards (OS mount points), optional extra folder, drive name, scan-only / dry-run / upload. Wizard step: “this tree is ~N files; we will skip ~X junk paths; estimated new unique bytes.” Then a job with a live log (poll `/jobs/{id}`).

**Settings** — bucket, restore dest, workers, junk rules (view defaults; maybe extra exclude globs), DB backup sync, optional password, manage/delete drive (keep the scary confirm).

**Timeline / Library** (after thumbs + dates) — year/month, moment cards from folder titles, photo grid. This is the Mission Stories “story board.”

---

## Data / migrations

Keep SQLite. Add, incrementally:

- `003_file_flags.py` — `is_junk` (or compute via a view) + maybe `searchable` so `.git` stays restorable but out of default FTS queries.
- `004_jobs.py` — `id, type, status, payload_json, log_path, created_at, started_at, finished_at, error`.
- `005_best_taken_at.py` — `best_taken_at`, `taken_at_source` (`exif` / `folder` / `mtime`).
- Later: use existing `file_features` for `thumb_v1`, `embedding_clip_v1`, `exif_v1`. Do not invent a parallel metadata pile.

FTS: keep `files_fts` on `file_path`. When hiding junk, filter in SQL (`is_junk = 0` and not VCS prefix), do not rebuild FTS unless we add a `search_text` column later (folder title + EXIF camera, etc.).

**Reuse as-is (move, do not rewrite):** `B2Manager`, `sanitize_b2_path`, `_insert_file_record`, `create_pointer_content`, `sha256_file` + EIO retries, skip-scan directory listing idea, `migrations.runner`, credential order (env then b2sdk), pointer JSON shape.

**Fix while moving:** download/restore should use the **local catalog**, not `bucket.ls`. Pointer resolution should stream to disk (today’s download cache holds whole files in RAM). `list_file_versions(..., fetch_count=1)` stays; never `max_versions`.

---

## Implementation phases (build in this order)

Each phase should leave the vault usable. Do not start embeddings until search, restore, and ingest jobs work.

### Phase 0 — Extract core, serve an empty FastAPI shell
- Split `b2_dedup.py` into `core/` + thin CLI.
- FastAPI app, Jinja layout, vendored Alpine, Docker port/CMD.
- `init_db()` on startup. Read-only Home that shows drive counts from the live DB.
- Streamlit still runnable until Phase 1 is enough to daily-drive. Then delete `gui/`, `b2_gui.py`, streamlit/pandas from Docker.

### Phase 1 — Find and inspect (the reason the UI exists)
- Global search + filters + junk/VCS hide.
- Browse folders + inspector + “other copies.”
- Restore set + restore-to-folder job (catalog-driven, pointer-aware, streamed).
- This replaces Browse + Search + Basket.

### Phase 2 — Ingest as a job
- Web ingest wizard + incremental skip + ingest-exclude junk list.
- Job log page.
- CLI `ingest` calls the same `core.ingest`.
- Delete the Streamlit upload subprocess.

### Phase 3 — Trust
- `verify` job (sample or full: DB originals exist on B2).
- DB backup/restore moved from sidebar dialogs into Settings.
- Health on Home.
- **Done:** `core/verify.py`, `core/backup.py`, `core/health.py`, `/settings`, Home health cards, CLI `verify`.

### Phase 4 — Time and library (Mission Stories begins)
- EXIF + folder-date → `best_taken_at`.
- Thumbnail job for images (and JPEG previews of CR2).
- Timeline / moment cards from parent folder names.
- Library grid. Hide junk by default so 400k photos are the surface.
- **Done:** `migrations/006_best_taken_at.py`, `core/time.py`, `core/thumbs.py`, `core/library.py`, `/library`, `/timeline`, `/moment`, Settings date + thumb jobs, CLI `dates` / `thumbs`.

### Phase 5 — Embeddings and “ask”
- CLIP (or successor) over images; store in `file_features`.
- Visual kNN search (“like this” / text query).
- Optional video keyframes.
- Ask-the-vault: retrieve moments + captions/paths, grounded answer with file citations (same idea as `ArchiveAskService`).
- Only then consider SaaS packaging.

---

## Files we expect to touch first (Phase 0–1)

| Path | Role |
|---|---|
| `b2_dedup.py` | Shrink to CLI dispatcher |
| `core/*.py` | New package from existing functions |
| `web/app.py`, `web/routes/*.py`, `web/templates/*` | New UI |
| `jobs.py` | Job queue |
| `migrations/003_*.py`, `004_*.py` | flags + jobs |
| `requirements.txt` | fastapi, jinja2, uvicorn, python-multipart; streamlit/pandas only if someone still uses `--legacy` |
| `README.md`, `AGENTS.md`, `CLAUDE.md` | Product is StorageInsights; CLI is ops |
| `.notes/overhaul-plan.md` | Checked-in copy of this plan (created after approval) |
| Delete after cutover | `b2_gui.py`, `gui/` |

---

## Verification

No test suite today. Add a few focused tests around junk rules, incremental skip, pointer restore, and search SQL (especially `files_fts MATCH` vs alias). Then:

1. `python b2_dedup.py serve` against the real `data/b2_dedup.db` (read-only first).
2. Home shows 19 drives and ~1.18M files.
3. Search `triplets` or `IMG_8730` across all drives; junk/`._*` not in default results.
4. Inspector on a duplicated family photo lists the other paths.
5. Restore a small folder to `restore_dest`; pointers resolve; `.git` included if present; `node_modules` absent on new ingests.
6. Ingest dry-run on a tiny tree: skip list + incremental already-tracked.
7. Verify job reports a sensible missing count (hopefully 0).
8. Confirm no npm, no Streamlit, and JS-off search still works.

---

## Confirmed decisions

- **Name:** StorageInsights (repo may stay `b2-dedup` for now).
- **LAN password:** optional `VAULT_PASSWORD`; unset = open on the LAN.
- **Purge existing AppleDouble from B2:** hide only; explicit cleanup job later.
- **CLI ingest for cron:** yes, same `core` as the web job.

Saved in-repo as `.notes/overhaul-plan.md`.
