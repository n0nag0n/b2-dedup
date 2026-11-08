# StorageInsights

A home-network vault for old drives backed up to Backblaze B2. Files are hashed (SHA-256) and only uploaded once; duplicates become lightweight `.b2ptr` pointer files. A local SQLite catalog makes the archive searchable. Dedup is the plumbing — the product is finding what you already have.

The CLI still handles unattended ingest and restore. `python b2_dedup.py serve` opens the StorageInsights web UI (FastAPI + HTML). Ingest is a background job (`/ingest`); `/scratch` accepts browser uploads and feeds them through the same ingest job. Search covers name queries, walking one drive, and moments. Restore-to-folder is in the same app. The previous Streamlit UI is `serve --legacy`.

## Purpose

When backing up multiple drives or performing incremental backups, you often end up with duplicate files across different directories (e.g., multiple WordPress installations sharing the same core files). This tool uses a local SQLite database to track file hashes (SHA-256), ensuring that:

1. **Deduplication:** The same file content is never uploaded twice to your B2 bucket.
2. **Complete Restoration:** Duplicate files get pointer files (`.b2ptr`) that reference the original, so downloads restore the complete directory structure.
3. **Resumption:** If a backup is interrupted, it can pick up right where it left off.
4. **Speed:** Parallel workers and streaming uploads ensure your bandwidth is fully utilized.

---

## Recommended Workflow

To get the most out of deduplication, it is recommended to:
1. **Upload a "Primary" Drive:** Start by processing your main or most complete data source. This establishes the baseline in your local database.
2. **Add Secondary Sources:** Any subsequent drives or folders you upload will be compared against this baseline. Unique files get uploaded; duplicates get pointer files created.
3. **Download When Needed:** Use the download command to restore any directory. Pointer files are automatically resolved to fetch the original content.

### Creating Missing Pointers
If you previously uploaded drives using an older version of this tool (where duplicates were skipped), you can generate the missing pointer files by simply running the `upload` command again on those drives. The script will detect that the files are duplicates but that no pointer exists, and will create/upload the necessary `.b2ptr` files.

---

## Features

- 🚀 **Parallel Uploads/Downloads:** Multi-threaded architecture for high performance.
- 🔍 **Deduplication:** Tracks SHA-256 hashes in a local database to avoid storing duplicates.
- 📁 **Pointer Files:** Duplicates create lightweight `.b2ptr` files referencing the original.
- ⬇️ **Smart Downloads:** Automatically resolves pointer files to restore complete directories.
- 🛠 **Scan-Only Mode:** Pre-calculate hashes and fill your database without uploading anything.
- 🧪 **Dry-Run Mode:** Simulate the entire process to see what would happen.
- 📊 **Progress Indicators:** Detailed progress bars for file counting, uploading, and downloading.
- 💾 **File Count Caching:** Speeds up restarts by caching directory file counts.
- 🔐 **Flexible Auth:** Supports standard B2 CLI credentials or environment variables.

---

## Installation

### 1. Requirements
- Python 3.8+
- Backblaze B2 Account

### 2. Setup
Clone this repository, create a virtualenv, and install the dependencies:

```bash
git clone https://github.com/yourusername/b2-dedup.git
cd b2-dedup
python3 -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env              # then fill in B2_KEY_ID and B2_APPLICATION_KEY
python b2_dedup.py serve
```

Open `http://localhost:8501`. Ingest (`/ingest`) lists plugged-in disks automatically (Linux: `/media` and `/mnt`, macOS: `/Volumes`, Windows: drive letters). If a disk is mounted somewhere else, add that folder on the Ingest page or in Settings.

---

## B2 Configuration

The tool looks for B2 credentials in the following order:

1. **B2 CLI Config:** If you have the [B2 CLI](https://www.backblaze.com/docs/cloud-storage-command-line-tools) installed and authorized (`b2 account authorize`), it will automatically use those credentials.
2. **Environment Variables:**
   ```bash
   export B2_KEY_ID='your-key-id'
   export B2_APPLICATION_KEY='your-application-key'
   ```

---

## Usage

```bash
python b2_dedup.py <action> [arguments] [flags]
```

### Actions

| Action | Description |
| :--- | :--- |
| `upload` / `ingest` | Upload files to B2 with deduplication and pointer file creation (same `core.ingest` as the web job) |
| `download` / `restore` | Download files from B2 with automatic pointer resolution |
| `verify` | Check that catalog originals still exist on B2 (`--sample N` or `--full`) |
| `dates` | Fill `best_taken_at` from folder names and file times (no B2) |
| `thumbs` | Build JPEG previews for unique images (`--limit N`, downloads from B2) |
| `serve` | Launch StorageInsights (use `--legacy` for the previous Streamlit UI) |

---

## Upload Command

```bash
python b2_dedup.py upload [SOURCE] --drive-name [DRIVE] --bucket [BUCKET] [FLAGS]
```

### Arguments & Flags

| Flag | Description |
| :--- | :--- |
| `source` | The local path you want to back up. |
| `--drive-root` | Optional. Base directory for relative paths (useful for uploading a subdirectory while keeping its place in the drive structure). |
| `--drive-name` | **Required.** The root directory name in B2 where files will be placed. |
| `--bucket` | **Required.** The name of your Backblaze B2 bucket. |
| `--scan-only` | Hash files and add them to the local DB, but do not upload. |
| `--dry-run` | Simulate uploads and DB updates without actually performing them. |
| `--workers` | Number of parallel worker threads (default: 10). |
| `--refresh-count` | Ignore the file count cache and re-scan the source directory. |
| `-v`, `--verbose` | Show each file being processed above the progress bar. |

### Upload Examples

#### 1. Initial Scan (Recommended)
Before your first big upload, you might want to index your files. This builds the local database without using any bandwidth.
```bash
python b2_dedup.py upload /mnt/my_drive --drive-name MyBackup --bucket my-safe-storage --scan-only
```

#### 2. Dry Run
Test your settings and see exactly how many files *would* be uploaded and how many are duplicates.
```bash
python b2_dedup.py upload /mnt/my_drive --drive-name MyBackup --bucket my-safe-storage --dry-run
```

#### 3. Full Upload
Kick off the actual backup process.
```bash
python b2_dedup.py upload /mnt/my_drive --drive-name MyBackup --bucket my-safe-storage
```

#### 4. High-Performance Upload
If you have a fast connection, you can increase the number of worker threads.
```bash
python b2_dedup.py upload /mnt/my_drive --drive-name MyBackup --bucket my-safe-storage --workers 25
```

#### 5. Uploading a Subdirectory (Preserving Path)
If you only want to upload a specific folder, but want it stored in its correct relative location within the "drive":
```bash
python b2_dedup.py upload /mnt/my_drive/Documents --drive-root /mnt/my_drive --drive-name MyBackup --bucket my-safe-storage
```
This will scan only `/Documents`, but store files as `MyBackup/Documents/...` instead of flattening them into `MyBackup/...`.

---

## Download Command

```bash
python b2_dedup.py download [REMOTE_PATH] --dest [DESTINATION] --bucket [BUCKET] [FLAGS]
```

### Arguments & Flags

| Flag | Description |
| :--- | :--- |
| `remote_path` | The B2 path to download (e.g., `MyDrive/` or `MyDrive/projects/`). |
| `--dest` | **Required.** Local destination folder for downloaded files. |
| `--bucket` | **Required.** The name of your Backblaze B2 bucket. |
| `--workers` | Number of parallel worker threads (default: 10). |
| `--dry-run` | Show what would be downloaded without actually downloading. |
| `-v`, `--verbose` | Show each file being downloaded above the progress bar. |

### Download Examples

#### 1. Download Entire Drive
```bash
python b2_dedup.py download MyBackup/ --dest /restore/my_backup --bucket my-safe-storage
```

#### 2. Download Specific Directory
```bash
python b2_dedup.py download MyBackup/projects/wordpress-site/ --dest /restore/wordpress --bucket my-safe-storage
```

#### 3. Dry Run Download
```bash
python b2_dedup.py download MyBackup/ --dest /restore/test --bucket my-safe-storage --dry-run
```

---

## Verify Command

```bash
python b2_dedup.py verify [--bucket BUCKET] [--sample 200] [--full]
```

Checks that catalog originals still exist on B2. Sample mode (default) probes a random subset. `--full` lists the bucket once and diffs it against every original that has an upload path. Scan-only rows without a remote path are counted, not treated as missing. The same check is a job on **Settings**.

---

## Serve Command (Web UI)

```bash
python b2_dedup.py serve [--port PORT]
```

Opens StorageInsights at `http://localhost:8501` (or the given port). Home shows every drive, catalog totals, catalog-backup health, and the last B2 verify. Search is global. **Ingest** (`/ingest`) lists plugged-in disks, previews junk skips and already-tracked files, then starts a job. **Scratch** (`/scratch`) accepts files or folders dragged into the browser and ingests them as the `Scratch` drive, reusing the same dedup and junk rules. The waiting area is `data/scratch/` unless another folder is chosen; a chosen folder's name is the default path inside the drive (`asus-backup` is stored as `Scratch/asus-backup/…`). Staged copies are removed after a successful ingest. From a drive in Search, or from a file page, a move updates the catalog path and the object in B2. **Settings** (`/settings`) holds the bucket, restore destination, extra ingest folders, Scratch drive name, catalog backup/restore, and the verify job. Job logs live at `/jobs`.

### Scratch uploads and the request size limit

Scratch copies each file to the computer running StorageInsights over HTTP, then ingests that copy. The web server in front of the app often rejects a large body before StorageInsights sees it. Nginx’s default is 1 MB. The failure shows up in the browser as **413**, and a video folder dies on the first file that crosses the limit.

Run the proxy at **256 MB** per request. That is large enough for a normal video and small enough that one request cannot be many gigabytes. Scratch sends a file in one request until it is larger than **248 MB**. Anything bigger is split into 248 MB pieces (`POST /scratch/chunk`). The 8 MB gap is the multipart wrapper, so a piece still fits inside the 256 MB cap. A half-finished file is kept out of the batch until its last piece arrives. If one file fails, the rest of the folder keeps uploading.

Nginx, on the StorageInsights site only:

```nginx
client_max_body_size 256m;
proxy_read_timeout 180s;
proxy_send_timeout 180s;
```

Caddy, inside the site block: `request_body max_size 256MB`. Apache: `LimitRequestBody 268435456`.

If the proxy allows less than 248 MB, the browser retries that file in smaller pieces, down to 256 KB. A piece that still will not go through is skipped, and the folder continues. Raising the proxy past 256 MB does not help. Larger files are already split. A short proxy read timeout can cut off a large piece the same way a small body limit does. 180 seconds is enough on a home network.

Downloading does not need a restore folder or a local copy of the vault:

- Every row in Search and Browse, and every file page, has a **⬇ Download** link. It streams straight from B2 to the browser and resolves `.b2ptr` duplicates, so a remote user gets the real file.
- The **Restore set** page downloads the whole selection as a ZIP. Selections at or below the stream threshold (500 MB by default) can be *prepared* as a background job: progress, a download link, and resume support. Larger selections stream instead and use no server disk.
- Limits are in **Settings → Browser downloads**: 1,000 files and 5 GB per download by default, plus how long prepared ZIPs are kept. Single-file downloads are never capped.
- Prepared archives are written to `data/exports/` and cleaned up automatically.

Optional LAN password: set `VAULT_PASSWORD` in `.env`. Leave it empty to keep the UI open on the local network.

```bash
# Previous Streamlit UI, until the new pages replace it
python b2_dedup.py serve --legacy --port 8501
```

---

## How It Works

### Upload Process

1. **Database:** The script creates a SQLite database at `~/b2_dedup.db`. 
2. **Hashing:** New or changed files are hashed using SHA-256. If `(drive_name, file_path)` already exists with the same size and mtime, the hash is skipped.
3. **Junk:** `node_modules`, `venv`, AppleDouble `._*`, `.DS_Store`, and similar paths are never hashed or uploaded. `.git` / `.hg` stay in the backup.
4. **Detection:** Before uploading, the script checks the local database:
   - **New file:** Upload to B2 and record in database as the original.
   - **Duplicate:** Create a pointer file (`.b2ptr`) containing the original file's B2 path, upload that instead.
5. **Bucket Check:** If the hash is new but the file name exists in the B2 bucket, it is recorded without re-uploading.
6. **Streaming:** Files are read and uploaded in chunks to keep memory usage low even for large files.

### Pointer Files

When a duplicate file is detected, instead of skipping it entirely, a small JSON pointer file is created:

```json
{
  "type": "b2_dedup_pointer",
  "version": 1,
  "original_hash": "abc123...",
  "original_path": "MyBackup/site1/wp-includes/version.php",
  "pointer_created": "2026-01-21T15:00:00Z"
}
```

This file is uploaded to B2 with a `.b2ptr` extension (e.g., `version.php.b2ptr`).

### Download Process

1. **List Files:** The script lists all files in the specified B2 path.
2. **Regular Files:** Downloaded directly to the destination.
3. **Pointer Files:** Downloaded, parsed, then the original file is fetched and saved with the correct name (without `.b2ptr`).
4. **Caching:** Downloaded originals are cached in memory to speed up resolution of multiple pointers to the same file.

---

## Database Schema

### `files` Table
Stores all file occurrences (originals and duplicates):
- `id` - Auto-increment primary key
- `hash` - SHA-256 hash (indexed for fast lookups)
- `size` - File size in bytes
- `drive_name` - Which drive this file is on
- `file_path` - Relative path on that drive
- `upload_path` - Full B2 path (only for originals)
- `is_original` - 1 if this is the uploaded copy, 0 if it's a pointer
- `created_at` - Timestamp

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
