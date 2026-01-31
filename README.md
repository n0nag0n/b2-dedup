# B2 Deduplicating Uploader

A powerful, parallel, streaming deduplicating uploader/downloader for Backblaze B2. This tool is designed to help you back up large amounts of data efficiently by ensuring files are only uploaded once across multiple runs or even different source drives. When duplicates are detected, lightweight pointer files are created that reference the original, allowing complete restoration of your backed-up directories.

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
Clone this repository and install the dependencies:

```bash
git clone https://github.com/yourusername/b2-dedup.git
cd b2-dedup
pip install -r requirements.txt
# or install venv and run pip install -r requirements.txt
```

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
| `upload` | Upload files to B2 with deduplication and pointer file creation |
| `download` | Download files from B2 with automatic pointer resolution |
| `serve` | Launch the Streamlit-based Web UI for browsing and searching files |

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

## Serve Command (Web UI)

The `serve` command launches a modern, browser-based interface for exploring your backup database.

```bash
python b2_dedup.py serve [--port PORT]
```

### Features:
- 📂 **File Browser:** Navigate through your drives and folders as they appear in B2.
- 🔍 **Powerful Search:** Instant search across all drives using SQLite FTS (Full Text Search).
- 📊 **Statistics:** View total storage usage, file counts, and deduplication efficiency.
- 📋 **Metadata View:** See file sizes, upload dates, and original paths for duplicates.

### Example:
```bash
python b2_dedup.py serve --port 8501
```
Once running, open your browser to `http://localhost:8501`.

---

## How It Works

### Upload Process

1. **Database:** The script creates a SQLite database at `~/b2_dedup.db`. 
2. **Hashing:** Every file is hashed using SHA-256. This hash is the primary key in the database.
3. **Detection:** Before uploading, the script checks the local database:
   - **New file:** Upload to B2 and record in database as the original.
   - **Duplicate:** Create a pointer file (`.b2ptr`) containing the original file's B2 path, upload that instead.
4. **Bucket Check:** If the hash is new but the file name exists in the B2 bucket, it is recorded without re-uploading.
5. **Streaming:** Files are read and uploaded in chunks to keep memory usage low even for large files.

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
