# B2 Deduplicating Uploader

A powerful, parallel, streaming deduplicating uploader for Backblaze B2. This tool is designed to help you back up large amounts of data efficiently by ensuring files are only uploaded once across multiple runs or even different source drives.

## Purpose

When backing up multiple drives or performing incremental backups, you often end up with duplicate files across different directories. This tool uses a local SQLite database to track file hashes (SHA-256), ensuring that:
1. **Deduplication:** The same file is never uploaded twice to your B2 bucket.
2. **Resumption:** If a backup is interrupted, it can pick up right where it left off.
3. **Speed:** Parallel workers and streaming uploads ensure your bandwidth is fully utilized.

---

## Recommended Workflow

To get the most out of deduplication, it is recommended to:
1. **Upload a "Primary" Drive:** Start by processing your main or most complete data source. This establishes the baseline in your local database.
2. **Add Secondary Sources:** Any subsequent drives or folders you upload will be compared against this baseline. Only files that are unique (not found on the primary drive) will be uploaded to B2, saving you time and storage costs.

---

## Features

- 🚀 **Parallel Uploads:** Multi-threaded architecture for high performance.
- 🔍 **Deduplication:** Tracks SHA-256 hashes in a local database to skip duplicates.
- 🛠 **Scan-Only Mode:** Pre-calculate hashes and fill your database without uploading anything.
- 🧪 **Dry-Run Mode:** Simulate the entire process to see what would happen.
- 📊 **Progress Indicators:** Detailed progress bars for file counting and processing.
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
python b2_dedup.py [SOURCE] --drive-name [DRIVE] --bucket [BUCKET] [FLAGS]
```

### Arguments & Flags

| Flag | Description |
| :--- | :--- |
| `source` | The local path you want to back up. |
| `--drive-name` | **Required.** The root directory name in B2 where files will be placed. |
| `--bucket` | **Required.** The name of your Backblaze B2 bucket. |
| `--scan-only` | Hash files and add them to the local DB, but do not upload. |
| `--dry-run` | Simulate uploads and DB updates without actually performing them. |
| `--workers` | Number of parallel worker threads (default: 10). |
| `--refresh-count` | Ignore the file count cache and re-scan the source directory. |

---

## Examples

### 1. Initial Scan (Recommended)
Before your first big upload, you might want to index your files. This builds the local database without using any bandwidth.
```bash
python b2_dedup.py /mnt/my_drive --drive-name MyBackup --bucket my-safe-storage --scan-only
```

### 2. Dry Run
Test your settings and see exactly how many files *would* be uploaded and how many are already known.
```bash
python b2_dedup.py /mnt/my_drive --drive-name MyBackup --bucket my-safe-storage --dry-run
```

### 3. Full Upload
Kick off the actual backup process.
```bash
python b2_dedup.py /mnt/my_drive --drive-name MyBackup --bucket my-safe-storage
```

### 4. High-Performance Upload
If you have a fast connection, you can increase the number of worker threads.
```bash
python b2_dedup.py /mnt/my_drive --drive-name MyBackup --bucket my-safe-storage --workers 25
```

---

## How It Works

1. **Database:** The script creates a SQLite database at `~/b2_dedup.db`. 
2. **Hashing:** Every file is hashed using SHA-256. This hash is the primary key in the database.
3. **Detection:** Before uploading, the script checks the local database. If the hash exists, it is considered a duplicate and skipped.
4. **Bucket Check:** If the hash is new but the file name exists in the B2 bucket (under that specific drive name), it is skipped to prevent overwriting.
5. **Streaming:** Files are read and uploaded in chunks to keep memory usage low even for large files.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
