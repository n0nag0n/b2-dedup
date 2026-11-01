#!/usr/bin/env python3
"""
StorageInsights CLI — thin dispatcher over core/ and the web app.

Legacy Streamlit UI remains available via `serve --legacy`. Ingest, search,
browse, restore, verify, and Settings live in StorageInsights.
"""
import argparse

from core import (
    B2Manager,
    CACHE_PATH,
    CHUNK_SIZE,
    DATA_DIR,
    DB_PATH,
    DB_REMOTE_PATH,
    DEFAULT_MAX_WORKERS,
    FILE_COUNT_CACHE_DAYS,
    POINTER_EXTENSION,
    create_pointer_content,
    get_thread_connection,
    init_db,
    sanitize_b2_path,
    sha256_file,
)
from core.verify import DEFAULT_SAMPLE_SIZE, verify_action
from core.library import backfill_dates, dates_status
from core.thumbs import run_thumbs, thumbs_status
from core.ingest import (
    _insert_file_record,
    count_files_with_progress,
    get_cached_file_count,
    insert_file_record,
    load_file_count_cache,
    process_file,
    save_file_count_cache,
    save_file_count_to_cache,
    upload_action,
)
from core.restore import download_action

# Re-exports for gui/*.py and any external imports of this module.
_DATA_DIR = DATA_DIR

__all__ = [
    "B2Manager",
    "CACHE_PATH",
    "CHUNK_SIZE",
    "DATA_DIR",
    "DB_PATH",
    "DB_REMOTE_PATH",
    "DEFAULT_MAX_WORKERS",
    "FILE_COUNT_CACHE_DAYS",
    "POINTER_EXTENSION",
    "_DATA_DIR",
    "_insert_file_record",
    "count_files_with_progress",
    "create_pointer_content",
    "download_action",
    "get_cached_file_count",
    "get_thread_connection",
    "init_db",
    "insert_file_record",
    "load_file_count_cache",
    "process_file",
    "sanitize_b2_path",
    "save_file_count_cache",
    "save_file_count_to_cache",
    "sha256_file",
    "upload_action",
]


def serve_action(args):
    """Launch StorageInsights (FastAPI) or the legacy Streamlit UI."""
    if getattr(args, "legacy", False):
        _serve_streamlit(args)
        return

    try:
        import uvicorn
    except ImportError:
        print("Error: 'serve' requires fastapi, jinja2, and uvicorn.")
        print("Install them with: pip install fastapi jinja2 uvicorn python-multipart")
        return

    print("Starting StorageInsights…")
    print(f"  http://127.0.0.1:{args.port}")
    if args.legacy is False:
        print("  (legacy Streamlit UI: python b2_dedup.py serve --legacy)")

    uvicorn.run(
        "web.app:app",
        host="0.0.0.0",
        port=args.port,
        log_level="info",
    )


def _serve_streamlit(args):
    import subprocess
    import sys
    from pathlib import Path

    try:
        import pandas  # noqa: F401
        import streamlit  # noqa: F401
    except ImportError:
        print("Error: --legacy requires streamlit and pandas.")
        print("Please install them with: pip install streamlit pandas")
        return

    base_dir = Path(__file__).parent.resolve()
    gui_script = base_dir / "b2_gui.py"
    if not gui_script.exists():
        print(f"Error: Could not find {gui_script}")
        return

    print("Starting legacy Streamlit UI…")
    cmd = [
        sys.executable, "-m", "streamlit", "run", str(gui_script),
        "--server.headless", "true",
    ]
    if args.port:
        cmd.extend(["--server.port", str(args.port)])
    try:
        subprocess.run(cmd)
    except KeyboardInterrupt:
        print("\nWeb UI stopped.")


def main():
    parser = argparse.ArgumentParser(
        description="StorageInsights — searchable home vault for Backblaze B2 backups",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Start the web app:
    %(prog)s serve

  Upload / ingest a drive:
    %(prog)s upload /path/to/drive --drive-name MyDrive --bucket my-bucket

  Upload a subdirectory (preserving path):
    %(prog)s upload /path/to/drive/Docs --drive-root /path/to/drive --drive-name MyDrive --bucket my-bucket

  Download / restore:
    %(prog)s download MyDrive/projects/ --dest /path/to/restore --bucket my-bucket

  Verify catalog originals exist on B2:
    %(prog)s verify --bucket my-bucket
    %(prog)s verify --full --bucket my-bucket
""",
    )

    subparsers = parser.add_subparsers(dest="action", help="Action to perform")

    upload_parser = subparsers.add_parser("upload", help="Upload files to B2 with deduplication")
    upload_parser.add_argument("source", help="Path to the drive/folder to upload")
    upload_parser.add_argument("--drive-root", help="Base directory for relative paths (e.g. if uploading a subdir)")
    upload_parser.add_argument("--drive-name", required=True, help="Root folder name in B2 (e.g. MasterDrive)")
    upload_parser.add_argument("--bucket", required=True, help="B2 bucket name")
    upload_parser.add_argument("--scan-only", action="store_true", help="Only build hash database, no upload")
    upload_parser.add_argument("--dry-run", action="store_true", help="Simulate full mode without uploading")
    upload_parser.add_argument("--refresh-count", action="store_true", help="Force re-count files (ignore cache)")
    upload_parser.add_argument(
        "--workers", type=int, default=DEFAULT_MAX_WORKERS,
        help=f"Parallel workers (default: {DEFAULT_MAX_WORKERS})",
    )
    upload_parser.add_argument("-v", "--verbose", action="store_true", help="Show each file being processed")

    ingest_parser = subparsers.add_parser("ingest", help="Alias for upload")
    ingest_parser.add_argument("source", help="Path to the drive/folder to upload")
    ingest_parser.add_argument("--drive-root", help="Base directory for relative paths")
    ingest_parser.add_argument("--drive-name", required=True, help="Root folder name in B2")
    ingest_parser.add_argument("--bucket", required=True, help="B2 bucket name")
    ingest_parser.add_argument("--scan-only", action="store_true", help="Only build hash database, no upload")
    ingest_parser.add_argument("--dry-run", action="store_true", help="Simulate full mode without uploading")
    ingest_parser.add_argument("--refresh-count", action="store_true", help="Force re-count files (ignore cache)")
    ingest_parser.add_argument(
        "--workers", type=int, default=DEFAULT_MAX_WORKERS,
        help=f"Parallel workers (default: {DEFAULT_MAX_WORKERS})",
    )
    ingest_parser.add_argument("-v", "--verbose", action="store_true", help="Show each file being processed")

    download_parser = subparsers.add_parser("download", help="Download files from B2 with pointer resolution")
    download_parser.add_argument("remote_path", help="B2 path to download (e.g. MyDrive/ or MyDrive/subdir/)")
    download_parser.add_argument("--dest", required=True, help="Local destination folder")
    download_parser.add_argument("--bucket", required=True, help="B2 bucket name")
    download_parser.add_argument(
        "--workers", type=int, default=DEFAULT_MAX_WORKERS,
        help=f"Parallel workers (default: {DEFAULT_MAX_WORKERS})",
    )
    download_parser.add_argument("--dry-run", action="store_true", help="Show what would be downloaded")
    download_parser.add_argument("-v", "--verbose", action="store_true", help="Show each file being downloaded")

    restore_parser = subparsers.add_parser("restore", help="Alias for download")
    restore_parser.add_argument("remote_path", help="B2 path to download")
    restore_parser.add_argument("--dest", required=True, help="Local destination folder")
    restore_parser.add_argument("--bucket", required=True, help="B2 bucket name")
    restore_parser.add_argument(
        "--workers", type=int, default=DEFAULT_MAX_WORKERS,
        help=f"Parallel workers (default: {DEFAULT_MAX_WORKERS})",
    )
    restore_parser.add_argument("--dry-run", action="store_true", help="Show what would be downloaded")
    restore_parser.add_argument("-v", "--verbose", action="store_true", help="Show each file being downloaded")

    serve_parser = subparsers.add_parser("serve", help="Launch StorageInsights web UI")
    serve_parser.add_argument("--port", type=int, default=8501, help="Port to run the web server on")
    serve_parser.add_argument(
        "--legacy", action="store_true",
        help="Launch the previous Streamlit UI instead of StorageInsights",
    )

    verify_parser = subparsers.add_parser(
        "verify", help="Check that catalog originals still exist on B2",
    )
    verify_parser.add_argument(
        "--bucket",
        help="B2 bucket name (defaults to the bucket saved in Settings)",
    )
    verify_parser.add_argument(
        "--sample", type=int, default=DEFAULT_SAMPLE_SIZE,
        help=f"Random originals to check (default: {DEFAULT_SAMPLE_SIZE})",
    )
    verify_parser.add_argument(
        "--full", action="store_true",
        help="List the whole bucket and compare every original with an upload path",
    )
    verify_parser.add_argument(
        "--workers", type=int, default=DEFAULT_MAX_WORKERS,
        help=f"Parallel existence checks for sample mode (default: {DEFAULT_MAX_WORKERS})",
    )

    dates_parser = subparsers.add_parser(
        "dates", help="Fill best_taken_at from folder names and file times",
    )
    dates_parser.add_argument(
        "--limit", type=int, default=0,
        help="Max catalog rows to scan (0 = all)",
    )

    thumbs_parser = subparsers.add_parser(
        "thumbs", help="Build JPEG previews for unique images (downloads from B2)",
    )
    thumbs_parser.add_argument(
        "--bucket",
        help="B2 bucket name (defaults to the bucket saved in Settings)",
    )
    thumbs_parser.add_argument(
        "--limit", type=int, default=500,
        help="Unique originals to process this run (0 = all remaining)",
    )

    args = parser.parse_args()

    if args.action is None:
        parser.print_help()
        return

    if args.action in ("upload", "ingest"):
        upload_action(args)
    elif args.action in ("download", "restore"):
        download_action(args)
    elif args.action == "serve":
        serve_action(args)
    elif args.action == "verify":
        verify_action(args)
    elif args.action == "dates":
        dates_action(args)
    elif args.action == "thumbs":
        thumbs_action(args)


def dates_action(args) -> None:
    init_db()
    limit = int(getattr(args, "limit", 0) or 0) or None
    before = dates_status()
    print(f"Searchable images: {before['images']:,}")
    print(f"Already dated:     {before['dated']:,}")
    print()
    stats = backfill_dates(limit=limit)
    after = dates_status()
    print(f"Scanned:   {stats['checked']:,}")
    print(f"Updated:   {stats['updated']:,}")
    print(f"Unchanged: {stats['unchanged']:,}")
    print(f"No date:   {stats['skipped']:,}")
    print(f"Now dated: {after['dated']:,} (folder {after['folder']:,}, mtime {after['mtime']:,}, exif {after['exif']:,})")


def thumbs_action(args) -> None:
    from core.config import get_bucket_name

    init_db()
    bucket = (getattr(args, "bucket", None) or get_bucket_name() or "").strip()
    try:
        limit = int(getattr(args, "limit", 500) or 0) or None
    except (TypeError, ValueError):
        limit = 500
    status = thumbs_status()
    print(f"Unique image originals: {status['original_images']:,}")
    print(f"Previews on disk:       {status['thumbs_on_disk']:,}")
    print(f"Limit:                  {limit or 'all remaining'}")
    if bucket:
        print(f"Bucket:                 {bucket}")
    print()
    try:
        stats = run_thumbs(bucket or None, limit=limit)
    except Exception as e:
        print(f"Error: {e}")
        return
    print(f"Created: {stats['created']:,}")
    print(f"Existed: {stats['exists']:,}")
    print(f"Errors:  {stats['error']:,}")
    print(f"Skipped: {stats['skipped']:,}")


if __name__ == "__main__":
    main()
