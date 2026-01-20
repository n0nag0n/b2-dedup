#!/usr/bin/env python3
"""
Parallel streaming deduplicating uploader to Backblaze B2
Supports --scan-only mode, --dry-run, and accurate progress bars
"""

import os
import hashlib
import sqlite3
import argparse
import json
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from tqdm import tqdm

# ================= CONFIG =================
DB_PATH = Path.home() / "b2_dedup.db"
CACHE_PATH = Path.home() / ".b2_dedup_cache.json"
DEFAULT_MAX_WORKERS = 10
CHUNK_SIZE = 4 * 1024 * 1024                 # 4MB
FILE_COUNT_CACHE_DAYS = 7                    # Cache file count for 1 week

# Thread-local storage for SQLite connections
thread_local = threading.local()


def get_thread_connection():
    """Get or create a SQLite connection for the current thread."""
    if not hasattr(thread_local, 'connection'):
        thread_local.connection = sqlite3.connect(DB_PATH)
    return thread_local.connection


def init_db():
    """Initialize the database schema (run once from main thread)."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS files (
            hash TEXT PRIMARY KEY,
            size INTEGER NOT NULL,
            drive_name TEXT,
            original_path TEXT,
            upload_path TEXT,
            uploaded_at TEXT
        )
    ''')
    conn.commit()
    conn.close()


def load_file_count_cache() -> dict:
    """Load the file count cache from disk."""
    if CACHE_PATH.exists():
        try:
            with open(CACHE_PATH, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def save_file_count_cache(cache: dict):
    """Save the file count cache to disk."""
    try:
        with open(CACHE_PATH, 'w') as f:
            json.dump(cache, f, indent=2)
    except IOError as e:
        print(f"⚠ Could not save file count cache: {e}")


def get_cached_file_count(source_path: Path, drive_name: str, refresh: bool = False) -> tuple[int, bool]:
    """
    Get file count from cache if valid, otherwise return None.
    Returns (count, was_cached) tuple.
    """
    if refresh:
        return None, False
    
    cache = load_file_count_cache()
    cache_key = f"{drive_name}:{source_path}"
    
    if cache_key in cache:
        entry = cache[cache_key]
        cached_time = datetime.fromisoformat(entry['timestamp'])
        if datetime.now() - cached_time < timedelta(days=FILE_COUNT_CACHE_DAYS):
            return entry['count'], True
    
    return None, False


def save_file_count_to_cache(source_path: Path, drive_name: str, count: int):
    """Save the file count to cache."""
    cache = load_file_count_cache()
    cache_key = f"{drive_name}:{source_path}"
    cache[cache_key] = {
        'count': count,
        'timestamp': datetime.now().isoformat(),
        'path': str(source_path)
    }
    save_file_count_cache(cache)


def count_files_with_progress(source_path: Path) -> int:
    """Count files with progress output per top-level directory."""
    total_count = 0
    
    # Get top-level directories and files
    top_level_items = list(source_path.iterdir())
    top_level_dirs = [d for d in top_level_items if d.is_dir()]
    top_level_files = [f for f in top_level_items if f.is_file()]
    
    # Count top-level files first
    top_level_file_count = len(top_level_files)
    if top_level_file_count > 0:
        print(f"  [root files]: {top_level_file_count:,} files")
        total_count += top_level_file_count
    
    # Process each top-level directory
    for i, subdir in enumerate(sorted(top_level_dirs), 1):
        dir_count = 0
        try:
            for root, _, files in os.walk(subdir):
                dir_count += len(files)
        except PermissionError:
            print(f"  [{i}/{len(top_level_dirs)}] {subdir.name}/: ⚠ Permission denied")
            continue
        
        total_count += dir_count
        print(f"  [{i}/{len(top_level_dirs)}] {subdir.name}/: {dir_count:,} files (total: {total_count:,})")
    
    return total_count


def sha256_file(filepath: Path) -> tuple[str, int]:
    sha256 = hashlib.sha256()
    size = 0
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
            sha256.update(chunk)
            size += len(chunk)
    return sha256.hexdigest(), size


class B2Manager:
    def __init__(self, bucket_name: str):
        from b2sdk.v2 import SqliteAccountInfo, InMemoryAccountInfo, B2Api
        try:
            info = SqliteAccountInfo()
            self.api = B2Api(info)
            self.api.authorize_automatically()
            print("✓ Using stored B2 credentials (~/.b2/account_info)")
        except Exception as e:
            print(f"⚠ Could not load stored credentials: {e}")
            print("Checking environment variables for fallback...")
            key_id = os.getenv('B2_KEY_ID')
            app_key = os.getenv('B2_APPLICATION_KEY')

            if key_id and app_key:
                print("✓ Using B2 credentials from environment variables")
                info = InMemoryAccountInfo()
                self.api = B2Api(info)
                self.api.authorize_account("production", key_id, app_key)
            else:
                raise RuntimeWarning("No B2 credentials found (tried ~/.b2/ and env vars B2_KEY_ID/B2_APPLICATION_KEY)")

        self.bucket = self.api.get_bucket_by_name(bucket_name)

    def file_exists(self, remote_path: str) -> bool:
        try:
            versions = list(self.bucket.list_file_versions(remote_path, max_versions=1))
            return len(versions) > 0 and versions[0].file_name == remote_path
        except:
            return False

    def upload_file(self, local_path: Path, remote_path: str):
        with open(local_path, "rb") as f:
            self.bucket.upload_bytes(f.read(), file_name=remote_path)


def process_file(args_tuple):
    filepath, rel_path, drive_name, scan_only, dry_run, b2 = args_tuple
    
    # Get thread-local connection
    conn = get_thread_connection()
    c = conn.cursor()
    
    remote_name = f"{drive_name}/{rel_path.as_posix()}"
    original_rel_path = rel_path.as_posix()  # Relative original path (from drive root)

    try:
        file_size = filepath.stat().st_size
        file_hash, actual_size = sha256_file(filepath)

        c.execute("SELECT hash FROM files WHERE hash = ?", (file_hash,))
        if c.fetchone():
            return "duplicate", filepath

        if scan_only:
            c.execute(
                "INSERT OR IGNORE INTO files (hash, size, drive_name, original_path, upload_path) "
                "VALUES (?, ?, ?, ?, NULL)",
                (file_hash, actual_size, drive_name, original_rel_path)
            )
            conn.commit()
            return "scanned", filepath
        else:
            # Normal mode: check if already in bucket
            if b2.file_exists(remote_name):
                return "exists", filepath

            if dry_run:
                return "would_upload", filepath

            # Actual upload
            b2.upload_file(filepath, remote_name)
            c.execute(
                "INSERT INTO files (hash, size, drive_name, original_path, upload_path, uploaded_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (file_hash, actual_size, drive_name, original_rel_path, remote_name, datetime.now(datetime.UTC).isoformat())
            )
            conn.commit()
            return "uploaded", filepath

    except Exception as e:
        return "error", filepath, str(e)


def main():
    parser = argparse.ArgumentParser(description="Streaming B2 deduplicator with scan-only and dry-run modes")
    parser.add_argument("source", help="Path to the drive/folder")
    parser.add_argument("--drive-name", required=True, help="Root folder name in B2 (e.g. MasterDrive)")
    parser.add_argument("--bucket", required=True, help="B2 bucket name")
    parser.add_argument("--scan-only", action="store_true", help="Only build hash database, no upload")
    parser.add_argument("--dry-run", action="store_true", help="Simulate full mode without uploading")
    parser.add_argument("--refresh-count", action="store_true", help="Force re-count files (ignore cache)")
    parser.add_argument("--workers", type=int, default=DEFAULT_MAX_WORKERS, help=f"Parallel workers (default: {DEFAULT_MAX_WORKERS})")
    parser.add_argument("-v", "--verbose", action="store_true", help="Show each file being processed")
    args = parser.parse_args()

    if args.scan_only and args.dry_run:
        print("Note: --dry-run is ignored in --scan-only mode")

    source_path = Path(args.source).resolve()
    if not source_path.is_dir():
        print(f"Error: {source_path} is not a valid directory")
        return

    mode = "SCAN-ONLY" if args.scan_only else ("DRY-RUN" if args.dry_run else "FULL (upload)")
    print(f"Source:  {source_path}")
    print(f"Bucket:  {args.bucket}")
    print(f"Drive:   {args.drive_name}/")
    print(f"Mode:    {mode}")
    print(f"Workers: {args.workers}\n")

    # Initialize database schema
    init_db()
    
    # Initialize B2 if needed
    b2 = None if args.scan_only else B2Manager(args.bucket)

    # Check file count cache
    cached_count, was_cached = get_cached_file_count(source_path, args.drive_name, args.refresh_count)
    
    if was_cached:
        total_files = cached_count
        print(f"Using cached file count: {total_files:,} files")
        print(f"  (Use --refresh-count to force re-count)\n")
    else:
        print("Counting files...")
        total_files = count_files_with_progress(source_path)
        save_file_count_to_cache(source_path, args.drive_name, total_files)
        print(f"\nTotal: {total_files:,} files (cached for {FILE_COUNT_CACHE_DAYS} days)\n")

    stats = {"scanned": 0, "duplicate": 0, "uploaded": 0, "would_upload": 0, "exists": 0, "error": 0}
    errors = []

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        for root, _, files in os.walk(source_path):
            for filename in files:
                filepath = Path(root) / filename
                try:
                    rel_path = filepath.relative_to(source_path)
                    futures.append(executor.submit(process_file, (filepath, rel_path, args.drive_name, args.scan_only, args.dry_run, b2)))
                except:
                    pass

        with tqdm(total=total_files, desc="Processing", unit="file") as pbar:
            for future in as_completed(futures):
                result = future.result()
                if len(result) == 3:
                    status, filepath, err = result
                    errors.append((filepath, err))
                else:
                    status, filepath = result
                stats[status] += 1
                if args.verbose:
                    # Print above progress bar without disrupting it
                    status_icon = {"uploaded": "↑", "scanned": "✓", "duplicate": "≡", "exists": "○", "would_upload": "?", "error": "✗"}
                    pbar.write(f"  {status_icon.get(status, ' ')} [{status}] {filepath}")
                pbar.update(1)

    print("\nSummary:")
    if args.scan_only:
        print(f"  New files scanned into DB: {stats['scanned']:,}")
    else:
        print(f"  Would upload (dry-run): {stats['would_upload']:,}")
        print(f"  Uploaded:               {stats['uploaded']:,}")
        print(f"  Already exists in bucket: {stats['exists']:,}")
    print(f"  Duplicates skipped: {stats['duplicate']:,}")
    if stats['error']:
        print(f"  Errors: {stats['error']:,}")

    if errors:
        print(f"\nFirst 5 errors:")
        for fp, err in errors[:5]:
            print(f"  {fp}: {err}")


if __name__ == "__main__":
    main()