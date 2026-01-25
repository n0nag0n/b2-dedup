#!/usr/bin/env python3
import sqlite3
import os
import argparse
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from file_utils import get_file_metadata

# Config
DB_PATH = Path.home() / "b2_dedup.db"
DEFAULT_MAX_WORKERS = 10

# Thread-local for connections
thread_local = threading.local()

def get_thread_connection():
    if not hasattr(thread_local, 'connection'):
        thread_local.connection = sqlite3.connect(DB_PATH, timeout=60.0)
    return thread_local.connection

def add_column_if_not_exists(c, table, column, col_type):
    c.execute(f"PRAGMA table_info({table})")
    columns = [row[1] for row in c.fetchall()]
    if column not in columns:
        print(f"Adding column {column} to {table}...")
        c.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")

def init_or_update_schema():
    print(f"Connecting to database at {DB_PATH}")
    conn = sqlite3.connect(DB_PATH, timeout=60.0)
    # Enable WAL mode for better concurrency
    conn.execute("PRAGMA journal_mode=WAL")
    c = conn.cursor()
    
    # Add new columns
    add_column_if_not_exists(c, 'files', 'file_mtime', 'TEXT')
    add_column_if_not_exists(c, 'files', 'file_ctime', 'TEXT')
    add_column_if_not_exists(c, 'files', 'file_atime', 'TEXT')
    add_column_if_not_exists(c, 'files', 'mime_type', 'TEXT')
    add_column_if_not_exists(c, 'files', 'file_type', 'TEXT')
    
    # New indexes
    print("Creating indexes...")
    c.execute('CREATE INDEX IF NOT EXISTS idx_files_mtime ON files(file_mtime)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_files_ctime ON files(file_ctime)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_files_atime ON files(file_atime)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_files_mime_type ON files(mime_type)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_files_file_type ON files(file_type)')
    
    conn.commit()
    conn.close()
    print("✓ Schema updated with new columns and indexes.\n")

def process_file_update(args_tuple):
    filepath, rel_path, drive_name = args_tuple
    file_path = rel_path.as_posix()
    
    conn = get_thread_connection()
    c = conn.cursor()
    
    # Check if file exists in DB
    c.execute("SELECT id FROM files WHERE drive_name = ? AND file_path = ?", (drive_name, file_path))
    row = c.fetchone()
    
    if not row:
        return "not_in_db", filepath, None

    file_id = row[0]
    
    # Get metadata
    meta = get_file_metadata(filepath)
    if "error" in meta:
        return "error", filepath, meta["error"]
    
    try:
        c.execute("""
            UPDATE files 
            SET file_mtime = ?, file_ctime = ?, file_atime = ?, mime_type = ?, file_type = ? 
            WHERE id = ?
        """, (
            meta['mtime'], 
            meta['ctime'], 
            meta['atime'], 
            meta['mime_type'], 
            meta['file_type'], 
            file_id
        ))
        conn.commit()
        return "updated", filepath, None
        
    except Exception as e:
        return "error", filepath, str(e)

def count_files(source_path: Path) -> int:
    total = 0
    for root, _, files in os.walk(source_path):
        total += len(files)
    return total

def main():
    parser = argparse.ArgumentParser(description="Update file metadata in B2 Dedup DB")
    parser.add_argument("source", help="Path to the drive/folder to scan")
    parser.add_argument("--drive-name", required=True, help="Drive name in DB (e.g. MyDrive)")
    parser.add_argument("--workers", type=int, default=DEFAULT_MAX_WORKERS, help=f"Parallel workers (default: {DEFAULT_MAX_WORKERS})")
    parser.add_argument("-v", "--verbose", action="store_true", help="Show each file processed")
    
    args = parser.parse_args()
    
    source_path = Path(args.source).resolve()
    if not source_path.is_dir():
        print(f"Error: {source_path} is not a valid directory")
        return
    
    print(f"Source: {source_path}")
    print(f"Drive:  {args.drive_name}")
    print(f"Workers: {args.workers}")
    print("-" * 40)
    
    # 1. Update Schema
    init_or_update_schema()
    
    # 2. Count files
    print("Counting files to process...")
    total_files = count_files(source_path)
    print(f"Found {total_files:,} files in source directory.\n")
    
    stats = {"updated": 0, "not_in_db": 0, "error": 0}
    errors = []
    
    def file_generator():
        for root, _, files in os.walk(source_path):
            for filename in files:
                filepath = Path(root) / filename
                try:
                    rel_path = filepath.relative_to(source_path)
                    yield (filepath, rel_path, args.drive_name)
                except ValueError:
                    pass
    
    # 3. Process Files
    print("Starting metadata update...")
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        # We use a list comprehension here assuming the file count isn't massive (millions). 
        # If it is, we should use the generator pattern from the main script.
        # But for 'rescan', usually we want to cover everything.
        
        futures_map = {}
        for task in file_generator():
            future = executor.submit(process_file_update, task)
            futures_map[future] = task[0] # Store filepath for reference
            
        with tqdm(total=len(futures_map), desc="Updating Metadata", unit="file") as pbar:
            for future in as_completed(futures_map):
                filepath = futures_map[future]
                try:
                    result = future.result()
                    status, _, err = result
                    
                    stats[status] += 1
                    
                    if status == "error":
                        errors.append((filepath, err))
                    
                    if args.verbose:
                        icon = "✓" if status == "updated" else ("?" if status == "not_in_db" else "✗")
                        extra = f": {err}" if err else ""
                        pbar.write(f"[{icon}] {status}: {filepath}{extra}")
                        
                except Exception as e:
                    stats["error"] += 1
                    errors.append((filepath, str(e)))
                
                pbar.update(1)
    
    print("\n" + "=" * 40)
    print("Summary:")
    print(f"  Updated in DB: {stats['updated']:,}")
    print(f"  Skipped (not in DB): {stats['not_in_db']:,}")
    print(f"  Errors: {stats['error']:,}")
    
    if errors:
        print("\nFirst 5 errors:")
        for fp, err in errors[:5]:
            print(f"  {fp}: {err}")

if __name__ == "__main__":
    main()
