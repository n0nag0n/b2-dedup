#!/usr/bin/env python3
"""
Parallel streaming deduplicating uploader/downloader for Backblaze B2
Supports upload with pointer files for duplicates, download with pointer resolution,
--scan-only mode, --dry-run, and accurate progress bars
"""

import os
import hashlib
import sqlite3
import argparse
import json
import threading
import io
import time
import errno
import re
import urllib.parse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime, timedelta, timezone
from tqdm import tqdm
from file_utils import get_file_metadata

# ================= CONFIG =================
DB_PATH = Path.home() / "b2_dedup.db"
CACHE_PATH = Path.home() / ".b2_dedup_cache.json"
DEFAULT_MAX_WORKERS = 10
CHUNK_SIZE = 4 * 1024 * 1024                 # 4MB
FILE_COUNT_CACHE_DAYS = 7                    # Cache file count for 1 week
POINTER_EXTENSION = ".b2ptr"

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
    
    # Unified schema - files table with id primary key
    c.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash TEXT NOT NULL,
            size INTEGER NOT NULL,
            drive_name TEXT NOT NULL,
            file_path TEXT NOT NULL,
            upload_path TEXT,
            is_original INTEGER DEFAULT 0,
            created_at TEXT,
            file_mtime TEXT,
            file_ctime TEXT,
            file_atime TEXT,
            mime_type TEXT,
            file_type TEXT,
            UNIQUE(drive_name, file_path)
        )
    ''') # Keep compatible with existing setup

    # Groups table for "serve" feature
    c.execute('''
        CREATE TABLE IF NOT EXISTS groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            created_at TEXT
        )
    ''')

    # Group members table (Many-to-Many)
    c.execute('''
        CREATE TABLE IF NOT EXISTS group_members (
            group_id INTEGER,
            file_id INTEGER,
            added_at TEXT,
            PRIMARY KEY (group_id, file_id),
            FOREIGN KEY(group_id) REFERENCES groups(id) ON DELETE CASCADE,
            FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
        )
    ''')
    
    # --- Indexes for Performance (100k-3M rows) ---

    # 1. Covering index for browsing (Drive -> Path)
    c.execute('CREATE INDEX IF NOT EXISTS idx_files_drive_path ON files(drive_name, file_path)')

    # 2. Sorting indexes
    c.execute('CREATE INDEX IF NOT EXISTS idx_files_size ON files(size)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_files_created_at ON files(created_at)')
    
    # 2b. New Metadata Indexes
    c.execute('CREATE INDEX IF NOT EXISTS idx_files_mtime ON files(file_mtime)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_files_ctime ON files(file_ctime)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_files_file_type ON files(file_type)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_files_mime_type ON files(mime_type)')
    
    # 3. Original lookup (existing + enhancement)
    c.execute('CREATE INDEX IF NOT EXISTS idx_files_hash_original ON files(hash, is_original)')
    
    # 4. Group lookup efficiency
    c.execute('CREATE INDEX IF NOT EXISTS idx_group_members_file_id ON group_members(file_id)')

    # --- Full Text Search (FTS5) ---
    # We use external content tables to save space, referencing the 'files' table.
    # Note: Triggers are needed to keep FTS index in sync with main table.
    
    try:
        c.execute('''
            CREATE VIRTUAL TABLE IF NOT EXISTS files_fts USING fts5(
                file_path,
                content='files',
                content_rowid='id'
            )
        ''')

        # Triggers to keep FTS in sync
        c.execute('''
            CREATE TRIGGER IF NOT EXISTS files_ai AFTER INSERT ON files BEGIN
                INSERT INTO files_fts(rowid, file_path) VALUES (new.id, new.file_path);
            END;
        ''')
        c.execute('''
            CREATE TRIGGER IF NOT EXISTS files_ad AFTER DELETE ON files BEGIN
                INSERT INTO files_fts(files_fts, rowid, file_path) VALUES('delete', old.id, old.file_path);
            END;
        ''')
        c.execute('''
            CREATE TRIGGER IF NOT EXISTS files_au AFTER UPDATE ON files BEGIN
                INSERT INTO files_fts(files_fts, rowid, file_path) VALUES('delete', old.id, old.file_path);
                INSERT INTO files_fts(rowid, file_path) VALUES (new.id, new.file_path);
            END;
        ''')
    except sqlite3.OperationalError:
        # FTS5 might not be enabled in SQLite build, typically it is on Linux/Mac
        print("⚠ Warning: SQLite FTS5 extension not available. Search performance may be degraded.")

    # Check if FTS index needs rebuilding (e.g. if table created after data existed)
    try:
        # Check if files exist
        has_files = c.execute("SELECT 1 FROM files LIMIT 1").fetchone()
        if has_files:
            # Check if FTS data exists (shadow table)
            # Safe way: check if query returns any matches for a common term, or check shadow table
            # Checking shadow table is reliable for FTS5
            has_index = False
            try:
                # files_fts_data is the main data table for FTS5
                row_count = c.execute("SELECT count(*) FROM files_fts_data").fetchone()[0]
                has_index = row_count > 10 # heuristic
            except sqlite3.OperationalError:
                pass
            
            if not has_index:
                print("⚠ FTS index appears empty. Rebuilding (this may take a moment)...")
                c.execute("INSERT INTO files_fts(files_fts) VALUES('rebuild')")
                print("✓ FTS index rebuilt.")
    except Exception as e:
        print(f"⚠ Could not verify FTS index: {e}")

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
    """Hash a file using SHA256 with retries for transient IO errors."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            sha256 = hashlib.sha256()
            size = 0
            with open(filepath, "rb") as f:
                for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
                    sha256.update(chunk)
                    size += len(chunk)
            return sha256.hexdigest(), size
        except OSError as e:
            if e.errno == errno.EIO and attempt < max_retries - 1:
                time.sleep(0.5 * (attempt + 1))
                continue
            raise


def create_pointer_content(original_hash: str, original_path: str) -> bytes:
    """Create JSON content for a pointer file."""
    pointer = {
        "type": "b2_dedup_pointer",
        "version": 1,
        "original_hash": original_hash,
        "original_path": original_path,
        "pointer_created": datetime.now(timezone.utc).isoformat()
    }
    return json.dumps(pointer, indent=2).encode('utf-8')


def sanitize_b2_path(path_str: str) -> str:
    """
    Sanitize a path for B2 by URL-encoding characters that B2 forbids 
    (control characters 0x00-0x1F and 0x7F).
    """
    return re.sub(
        r'[\x00-\x1f\x7f]', 
        lambda m: f'%{ord(m.group(0)):02X}', 
        path_str
    )


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
        """Upload a local file to B2 with retries for transient IO errors."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.bucket.upload_local_file(
                    local_file=str(local_path),
                    file_name=remote_path
                )
                return
            except OSError as e:
                # Handle transient Input/output errors (e.g. from overloaded drives)
                if e.errno == errno.EIO and attempt < max_retries - 1:
                    time.sleep(1 * (attempt + 1))
                    continue
                raise

    def upload_bytes(self, content: bytes, remote_path: str, content_type: str = "application/json"):
        """Upload bytes directly to B2."""
        self.bucket.upload_bytes(content, remote_path, content_type=content_type)

    def download_file_content(self, remote_path: str) -> bytes:
        """Download a file and return its content as bytes."""
        downloaded_file = self.bucket.download_file_by_name(remote_path)
        buffer = io.BytesIO()
        downloaded_file.save(buffer)
        return buffer.getvalue()

    def download_file_to_path(self, remote_path: str, local_path: Path):
        """Download a file to a local path."""
        local_path.parent.mkdir(parents=True, exist_ok=True)
        downloaded_file = self.bucket.download_file_by_name(remote_path)
        downloaded_file.save_to(str(local_path))

    def list_files(self, prefix: str = "", recursive: bool = True):
        """List files in the bucket with optional prefix."""
        if recursive:
            return self.bucket.ls(prefix, recursive=True)
        else:
            return self.bucket.ls(prefix)


def process_file(args_tuple):
    filepath, rel_path, drive_name, scan_only, dry_run, b2 = args_tuple
    
    # Get thread-local connection
    conn = get_thread_connection()
    c = conn.cursor()
    
    remote_name = sanitize_b2_path(f"{drive_name}/{rel_path.as_posix()}")
    file_path = rel_path.as_posix()  # Relative path from drive root

    try:
        file_size = filepath.stat().st_size
        file_hash, actual_size = sha256_file(filepath)
        
        # Capture enriched metadata
        meta = get_file_metadata(filepath)
        # Fallback if error (though unlikely here since we just stat'd it)
        if "error" in meta:
             # Basic fallback
             meta = {
                 "mtime": None, "ctime": None, "atime": None, 
                 "mime_type": None, "file_type": "Unknown"
             }

        # Check if this exact file path already exists in the database
        c.execute("SELECT id FROM files WHERE drive_name = ? AND file_path = ?", (drive_name, file_path))
        if c.fetchone():
            return "already_tracked", filepath

        # Check if this hash already exists (for any file) - find the original
        c.execute("SELECT upload_path FROM files WHERE hash = ? AND is_original = 1 LIMIT 1", (file_hash,))
        existing = c.fetchone()
        
        if existing:
            # This is a duplicate - create pointer file
            original_upload_path = existing[0]
            
            if scan_only:
                # In scan-only mode, just record the location
                c.execute(
                    "INSERT OR IGNORE INTO files (hash, size, drive_name, file_path, upload_path, is_original, created_at, file_mtime, file_ctime, file_atime, mime_type, file_type) "
                    "VALUES (?, ?, ?, ?, NULL, 0, ?, ?, ?, ?, ?, ?)",
                    (file_hash, actual_size, drive_name, file_path, datetime.now(timezone.utc).isoformat(),
                     meta['mtime'], meta['ctime'], meta['atime'], meta['mime_type'], meta['file_type'])
                )
                conn.commit()
                return "duplicate_recorded", filepath
            
            # Create pointer content
            pointer_content = create_pointer_content(file_hash, original_upload_path)
            pointer_remote_path = remote_name + POINTER_EXTENSION
            
            # Check if pointer already exists
            if b2.file_exists(pointer_remote_path):
                c.execute(
                    "INSERT OR IGNORE INTO files (hash, size, drive_name, file_path, upload_path, is_original, created_at, file_mtime, file_ctime, file_atime, mime_type, file_type) "
                    "VALUES (?, ?, ?, ?, NULL, 0, ?, ?, ?, ?, ?, ?)",
                    (file_hash, actual_size, drive_name, file_path, datetime.now(timezone.utc).isoformat(),
                     meta['mtime'], meta['ctime'], meta['atime'], meta['mime_type'], meta['file_type'])
                )
                conn.commit()
                return "pointer_exists", filepath
            
            if dry_run:
                return "would_create_pointer", filepath
            
            # Upload pointer file
            b2.upload_bytes(pointer_content, pointer_remote_path)
            c.execute(
                "INSERT OR IGNORE INTO files (hash, size, drive_name, file_path, upload_path, is_original, created_at, file_mtime, file_ctime, file_atime, mime_type, file_type) "
                "VALUES (?, ?, ?, ?, NULL, 0, ?, ?, ?, ?, ?, ?)",
                (file_hash, actual_size, drive_name, file_path, datetime.now(timezone.utc).isoformat(),
                 meta['mtime'], meta['ctime'], meta['atime'], meta['mime_type'], meta['file_type'])
            )
            conn.commit()
            return "pointer_created", filepath

        # This is a new unique file
        if scan_only:
            c.execute(
                "INSERT OR IGNORE INTO files (hash, size, drive_name, file_path, upload_path, is_original, created_at, file_mtime, file_ctime, file_atime, mime_type, file_type) "
                "VALUES (?, ?, ?, ?, NULL, 1, ?, ?, ?, ?, ?, ?)",
                (file_hash, actual_size, drive_name, file_path, datetime.now(timezone.utc).isoformat(),
                 meta['mtime'], meta['ctime'], meta['atime'], meta['mime_type'], meta['file_type'])
            )
            conn.commit()
            return "scanned", filepath
        else:
            # Normal mode: check if already in bucket
            if b2.file_exists(remote_name):
                c.execute(
                    "INSERT OR IGNORE INTO files (hash, size, drive_name, file_path, upload_path, is_original, created_at, file_mtime, file_ctime, file_atime, mime_type, file_type) "
                    "VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?)",
                    (file_hash, actual_size, drive_name, file_path, remote_name, datetime.now(timezone.utc).isoformat(),
                     meta['mtime'], meta['ctime'], meta['atime'], meta['mime_type'], meta['file_type'])
                )
                conn.commit()
                return "exists", filepath

            if dry_run:
                return "would_upload", filepath

            # Actual upload
            b2.upload_file(filepath, remote_name)
            
            c.execute(
                "INSERT OR IGNORE INTO files (hash, size, drive_name, file_path, upload_path, is_original, created_at, file_mtime, file_ctime, file_atime, mime_type, file_type) "
                "VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?)",
                (file_hash, actual_size, drive_name, file_path, remote_name, datetime.now(timezone.utc).isoformat(),
                 meta['mtime'], meta['ctime'], meta['atime'], meta['mime_type'], meta['file_type'])
            )
            
            # Check if row was actually inserted (race condition handling)
            if c.rowcount == 0:
                conn.commit()
                return "race_duplicate", filepath

            conn.commit()
            return "uploaded", filepath

    except Exception as e:
        return "error", filepath, str(e)


def upload_action(args):
    """Handle the upload subcommand."""
    if args.scan_only and args.dry_run:
        print("Note: --dry-run is ignored in --scan-only mode")

    source_path = Path(args.source).resolve()
    if not source_path.is_dir():
        print(f"Error: {source_path} is not a valid directory")
        return

    # Determine root for relative paths
    if args.drive_root:
        drive_root = Path(args.drive_root).resolve()
        try:
            source_path.relative_to(drive_root)
        except ValueError:
            print(f"Error: Source {source_path} is not inside drive root {drive_root}")
            return
    else:
        drive_root = source_path

    mode = "SCAN-ONLY" if args.scan_only else ("DRY-RUN" if args.dry_run else "FULL (upload)")
    print(f"Source:  {source_path}")
    if args.drive_root:
        print(f"Root:    {drive_root}")
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

    stats = {
        "scanned": 0, 
        "duplicate_recorded": 0,
        "already_tracked": 0,
        "uploaded": 0, 
        "would_upload": 0, 
        "exists": 0, 
        "pointer_created": 0,
        "pointer_exists": 0,
        "would_create_pointer": 0,
        "race_duplicate": 0,
        "error": 0
    }
    errors = []

    def file_generator():
        """Generator that yields file tasks one at a time (low memory)."""
        for root, _, files in os.walk(source_path):
            for filename in files:
                filepath = Path(root) / filename
                
                # Skip symlinks and non-regular files (pipes, devices, etc.)
                # This prevents common EIO errors from special files.
                if filepath.is_symlink():
                    continue
                if not filepath.is_file():
                    continue
                    
                try:
                    rel_path = filepath.relative_to(drive_root)
                    yield (filepath, rel_path, args.drive_name, args.scan_only, args.dry_run, b2)
                except Exception:
                    # e.g. Path.relative_to might fail in very weird edge cases
                    pass

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        # Use bounded submission: only keep max_in_flight futures in memory at once
        max_in_flight = args.workers * 2
        pending = set()
        file_iter = file_generator()
        
        with tqdm(total=total_files, desc="Processing", unit="file") as pbar:
            # Initial fill of the pending set
            for task in file_iter:
                pending.add(executor.submit(process_file, task))
                if len(pending) >= max_in_flight:
                    break
            
            # Process as we go, refilling as futures complete
            while pending:
                # Wait for at least one future to complete
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                
                for future in done:
                    result = future.result()
                    err_msg = ""
                    if len(result) == 3:
                        status, filepath, err = result
                        errors.append((filepath, err))
                        err_msg = f": {err}"
                    else:
                        status, filepath = result
                    stats[status] += 1
                    if args.verbose:
                        status_icon = {
                            "uploaded": "↑", 
                            "scanned": "✓", 
                            "duplicate_recorded": "≡",
                            "already_tracked": "○",
                            "exists": "○", 
                            "would_upload": "?", 
                            "pointer_created": "→",
                            "pointer_exists": "○",
                            "would_create_pointer": "?",
                            "race_duplicate": "≡",
                            "error": "✗"
                        }
                        pbar.write(f"  {status_icon.get(status, ' ')} [{status}] {filepath}{err_msg}")
                    pbar.update(1)
                    
                    # Refill: submit one new task for each completed one
                    try:
                        new_task = next(file_iter)
                        pending.add(executor.submit(process_file, new_task))
                    except StopIteration:
                        pass  # No more files to process

    print("\nSummary:")
    if args.scan_only:
        print(f"  New files scanned (original):  {stats['scanned']:,}")
        print(f"  Duplicates recorded:           {stats['duplicate_recorded']:,}")
        print(f"  Already in database:           {stats['already_tracked']:,}")
    else:
        print(f"  Uploaded (original):           {stats['uploaded']:,}")
        print(f"  Pointer files created:         {stats['pointer_created']:,}")
        print(f"  Already exists in bucket:      {stats['exists']:,}")
        print(f"  Pointer already exists:        {stats['pointer_exists']:,}")
        print(f"  Already in database:           {stats['already_tracked']:,}")
        if args.dry_run:
            print(f"  Would upload (dry-run):        {stats['would_upload']:,}")
            print(f"  Would create pointer:          {stats['would_create_pointer']:,}")
    if stats['error']:
        print(f"  Errors:                        {stats['error']:,}")

    if errors:
        print(f"\nFirst 5 errors:")
        for fp, err in errors[:5]:
            print(f"  {fp}: {err}")


def download_action(args):
    """Handle the download subcommand."""
    dest_path = Path(args.dest).resolve()
    
    print(f"Remote:  {args.remote_path}")
    print(f"Dest:    {dest_path}")
    print(f"Bucket:  {args.bucket}")
    print(f"Workers: {args.workers}")
    if args.dry_run:
        print(f"Mode:    DRY-RUN")
    print()

    b2 = B2Manager(args.bucket)
    
    # Normalize remote path (ensure it ends with / for prefix matching, unless it's empty)
    sanitized_remote = sanitize_b2_path(args.remote_path)
    remote_prefix = sanitized_remote.rstrip('/') + '/' if sanitized_remote else ""
    
    # Count files first
    print("Listing files...")
    files_to_download = []
    for file_version, folder_name in b2.list_files(remote_prefix):
        files_to_download.append(file_version)
    
    print(f"Found {len(files_to_download):,} files\n")
    
    if not files_to_download:
        print("No files to download.")
        return

    stats = {"downloaded": 0, "pointer_resolved": 0, "would_download": 0, "error": 0}
    errors = []
    
    # Cache for resolved pointers (avoid downloading same original multiple times)
    original_cache = {}
    cache_lock = threading.Lock()

    def download_file(file_version):
        """Download a single file, resolving pointers as needed."""
        remote_name = file_version.file_name
        
        # Calculate local path (remove the prefix to get relative path)
        if remote_prefix and remote_name.startswith(remote_prefix):
            rel_path = remote_name[len(remote_prefix):]
        else:
            rel_path = remote_name
        
        # Unquote to restore original filename characters (e.g. control chars, %)
        rel_path = urllib.parse.unquote(rel_path)
        
        try:
            if remote_name.endswith(POINTER_EXTENSION):
                # It's a pointer file - resolve it
                actual_rel_path = rel_path[:-len(POINTER_EXTENSION)]  # Remove .b2ptr
                local_path = dest_path / actual_rel_path
                
                if args.dry_run:
                    return "would_download", remote_name, f"(pointer → original)"
                
                # Download and parse pointer
                pointer_content = b2.download_file_content(remote_name)
                pointer = json.loads(pointer_content.decode('utf-8'))
                original_path = pointer['original_path']
                
                # Check cache for already-downloaded original
                with cache_lock:
                    if original_path in original_cache:
                        # Copy from cache
                        cached_content = original_cache[original_path]
                        local_path.parent.mkdir(parents=True, exist_ok=True)
                        with open(local_path, 'wb') as f:
                            f.write(cached_content)
                        return "pointer_resolved", remote_name, f"(cached)"
                
                # Download original file
                original_content = b2.download_file_content(original_path)
                
                # Cache it for future pointers
                with cache_lock:
                    original_cache[original_path] = original_content
                
                # Save to local path
                local_path.parent.mkdir(parents=True, exist_ok=True)
                with open(local_path, 'wb') as f:
                    f.write(original_content)
                
                return "pointer_resolved", remote_name, None
            else:
                # Regular file - download directly
                local_path = dest_path / rel_path
                
                if args.dry_run:
                    return "would_download", remote_name, None
                
                b2.download_file_to_path(remote_name, local_path)
                return "downloaded", remote_name, None
                
        except Exception as e:
            return "error", remote_name, str(e)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(download_file, fv): fv for fv in files_to_download}
        
        with tqdm(total=len(files_to_download), desc="Downloading", unit="file") as pbar:
            for future in futures:
                result = future.result()
                status, remote_name, extra = result
                
                stats[status] += 1
                
                if status == "error":
                    errors.append((remote_name, extra))
                
                if args.verbose:
                    status_icon = {"downloaded": "↓", "pointer_resolved": "→", "would_download": "?", "error": "✗"}
                    extra_msg = f" {extra}" if extra else ""
                    pbar.write(f"  {status_icon.get(status, ' ')} [{status}] {remote_name}{extra_msg}")
                
                pbar.update(1)

    print("\nSummary:")
    print(f"  Downloaded directly:           {stats['downloaded']:,}")
    print(f"  Pointers resolved:             {stats['pointer_resolved']:,}")
    if args.dry_run:
        print(f"  Would download (dry-run):      {stats['would_download']:,}")
    if stats['error']:
        print(f"  Errors:                        {stats['error']:,}")

    if errors:
        print(f"\nFirst 5 errors:")
        for fp, err in errors[:5]:
            print(f"  {fp}: {err}")



def serve_action(args):
    """Handle the serve subcommand (launch Web UI)."""
    import subprocess
    import sys
    
    # Check for dependencies
    try:
        import streamlit
        import pandas
    except ImportError:
        print("Error: 'serve' mode requires streamlit and pandas.")
        print("Please install them with: pip install streamlit pandas")
        return

    # Path to the b2_gui.py file (assumed to be in same directory)
    base_dir = Path(__file__).parent.resolve()
    gui_script = base_dir / "b2_gui.py"
    
    if not gui_script.exists():
        print(f"Error: Could not find {gui_script}")
        return
        
    print(f"Docs: Starting Web UI...")
    
    # Construct the command
    # streamlit run b2_gui.py --server.headless true ...
    cmd = [
        sys.executable, "-m", "streamlit", "run", str(gui_script),
        "--server.headless", "true" # Optional, makes it cleaner in some environments
    ]
    
    if args.port:
        cmd.extend(["--server.port", str(args.port)])
        
    try:
        subprocess.run(cmd)
    except KeyboardInterrupt:
        print("\nWeb UI stopped.")


def main():
    parser = argparse.ArgumentParser(
        description="B2 deduplicating backup tool with pointer file support",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Upload files:
    %(prog)s upload /path/to/drive --drive-name MyDrive --bucket my-bucket
  
  Upload subdirectory (preserving path):
    %(prog)s upload /path/to/drive/Docs --drive-root /path/to/drive --drive-name MyDrive --bucket my-bucket

  Download files:
    %(prog)s download MyDrive/projects/ --dest /path/to/restore --bucket my-bucket
    
  Serve Web UI:
    %(prog)s serve
"""
    )
    
    subparsers = parser.add_subparsers(dest="action", help="Action to perform")
    
    # Upload subcommand
    upload_parser = subparsers.add_parser("upload", help="Upload files to B2 with deduplication")
    upload_parser.add_argument("source", help="Path to the drive/folder to upload")
    upload_parser.add_argument("--drive-root", help="Base directory for relative paths (e.g. if uploading a subdir)")
    upload_parser.add_argument("--drive-name", required=True, help="Root folder name in B2 (e.g. MasterDrive)")
    upload_parser.add_argument("--bucket", required=True, help="B2 bucket name")
    upload_parser.add_argument("--scan-only", action="store_true", help="Only build hash database, no upload")
    upload_parser.add_argument("--dry-run", action="store_true", help="Simulate full mode without uploading")
    upload_parser.add_argument("--refresh-count", action="store_true", help="Force re-count files (ignore cache)")
    upload_parser.add_argument("--workers", type=int, default=DEFAULT_MAX_WORKERS, 
                               help=f"Parallel workers (default: {DEFAULT_MAX_WORKERS})")
    upload_parser.add_argument("-v", "--verbose", action="store_true", help="Show each file being processed")
    
    # Download subcommand
    download_parser = subparsers.add_parser("download", help="Download files from B2 with pointer resolution")
    download_parser.add_argument("remote_path", help="B2 path to download (e.g. MyDrive/ or MyDrive/subdir/)")
    download_parser.add_argument("--dest", required=True, help="Local destination folder")
    download_parser.add_argument("--bucket", required=True, help="B2 bucket name")
    download_parser.add_argument("--workers", type=int, default=DEFAULT_MAX_WORKERS,
                                help=f"Parallel workers (default: {DEFAULT_MAX_WORKERS})")
    download_parser.add_argument("--dry-run", action="store_true", help="Show what would be downloaded")
    download_parser.add_argument("-v", "--verbose", action="store_true", help="Show each file being downloaded")
    
    # Serve subcommand
    serve_parser = subparsers.add_parser("serve", help="Launch Web UI for searching and browsing")
    serve_parser.add_argument("--port", type=int, default=8501, help="Port to run the web server on")

    args = parser.parse_args()
    
    if args.action is None:
        parser.print_help()
        return
    
    if args.action == "upload":
        upload_action(args)
    elif args.action == "download":
        download_action(args)
    elif args.action == "serve":
        serve_action(args)


if __name__ == "__main__":
    main()