"""Download / restore from B2 with pointer resolution.

The CLI still lists B2. The web restore job uses the local catalog.
"""
import json
import threading
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from tqdm import tqdm

from core.b2 import B2Manager, sanitize_b2_path
from core.paths import POINTER_EXTENSION


def download_action(args):
    """Handle the download subcommand."""
    dest_path = Path(args.dest).resolve()

    print(f"Remote:  {args.remote_path}")
    print(f"Dest:    {dest_path}")
    print(f"Bucket:  {args.bucket}")
    print(f"Workers: {args.workers}")
    if args.dry_run:
        print("Mode:    DRY-RUN")
    print()

    b2 = B2Manager(args.bucket)

    sanitized_remote = sanitize_b2_path(args.remote_path)
    remote_prefix = sanitized_remote.rstrip("/") + "/" if sanitized_remote else ""

    print("Listing files...")
    files_to_download = []
    for file_version, _folder_name in b2.list_files(remote_prefix):
        files_to_download.append(file_version)

    print(f"Found {len(files_to_download):,} files\n")

    if not files_to_download:
        print("No files to download.")
        return

    stats = {"downloaded": 0, "pointer_resolved": 0, "would_download": 0, "error": 0}
    errors = []

    original_cache = {}
    cache_lock = threading.Lock()

    def download_file(file_version):
        remote_name = file_version.file_name

        if remote_prefix and remote_name.startswith(remote_prefix):
            rel_path = remote_name[len(remote_prefix):]
        else:
            rel_path = remote_name

        rel_path = urllib.parse.unquote(rel_path)

        try:
            if remote_name.endswith(POINTER_EXTENSION):
                actual_rel_path = rel_path[:-len(POINTER_EXTENSION)]
                local_path = dest_path / actual_rel_path

                if args.dry_run:
                    return "would_download", remote_name, "(pointer → original)"

                pointer_content = b2.download_file_content(remote_name)
                pointer = json.loads(pointer_content.decode("utf-8"))
                original_path = pointer["original_path"]

                with cache_lock:
                    if original_path in original_cache:
                        cached_content = original_cache[original_path]
                        local_path.parent.mkdir(parents=True, exist_ok=True)
                        with open(local_path, "wb") as f:
                            f.write(cached_content)
                        return "pointer_resolved", remote_name, "(cached)"

                original_content = b2.download_file_content(original_path)

                with cache_lock:
                    original_cache[original_path] = original_content

                local_path.parent.mkdir(parents=True, exist_ok=True)
                with open(local_path, "wb") as f:
                    f.write(original_content)

                return "pointer_resolved", remote_name, None

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
                    status_icon = {
                        "downloaded": "↓",
                        "pointer_resolved": "→",
                        "would_download": "?",
                        "error": "✗",
                    }
                    extra_msg = f" {extra}" if extra else ""
                    pbar.write(f"  {status_icon.get(status, ' ')} [{status}] {remote_name}{extra_msg}")

                pbar.update(1)

    print("\nSummary:")
    print(f"  Downloaded directly:           {stats['downloaded']:,}")
    print(f"  Pointers resolved:             {stats['pointer_resolved']:,}")
    if args.dry_run:
        print(f"  Would download (dry-run):      {stats['would_download']:,}")
    if stats["error"]:
        print(f"  Errors:                        {stats['error']:,}")

    if errors:
        print("\nFirst 5 errors:")
        for fp, err in errors[:5]:
            print(f"  {fp}: {err}")


def restore_one(b2: B2Manager, rec: dict, dest_root: Path) -> str:
    """Stream one catalog row to dest_root / drive / path. Returns status label."""
    from core.download import resolve_source

    dest = dest_root / rec["drive_name"] / rec["file_path"]
    dest.parent.mkdir(parents=True, exist_ok=True)
    b2.download_file_to_path(resolve_source(b2, rec), dest)
    return "downloaded" if rec.get("is_original") else "pointer_resolved"


def run_restore_job(job: dict) -> None:
    """Catalog-driven restore used by the job worker."""
    from core.catalog import get_files_by_ids
    from core.config import get_bucket_name
    from core.jobs import finish_job, update_progress

    job_id = job["id"]
    payload = job["payload"]
    ids = [int(i) for i in payload.get("ids") or []]
    dest_root = Path(payload.get("dest") or "").expanduser()
    bucket = (payload.get("bucket") or get_bucket_name() or "").strip()

    if not ids:
        finish_job(job_id, "error", "Restore set was empty.")
        return
    if not dest_root:
        finish_job(job_id, "error", "No restore destination configured.")
        return
    if not bucket:
        finish_job(job_id, "error", "No B2 bucket configured.")
        return

    update_progress(job_id, 0, len(ids), f"Connecting to B2 bucket {bucket}…")
    try:
        b2 = B2Manager(bucket)
    except Exception as e:
        finish_job(job_id, "error", f"Could not connect to B2: {e}")
        return

    dest_root.mkdir(parents=True, exist_ok=True)
    records = get_files_by_ids(ids)
    by_id = {r["id"]: r for r in records}
    skipped = 0
    ok = 0

    for i, fid in enumerate(ids, start=1):
        rec = by_id.get(fid)
        label = rec["file_path"] if rec else f"id {fid}"
        try:
            if not rec:
                skipped += 1
                update_progress(job_id, i, len(ids), f"skipped missing {label}")
                continue
            restore_one(b2, rec, dest_root)
            ok += 1
            if i == 1 or i == len(ids) or i % 10 == 0:
                update_progress(job_id, i, len(ids), f"{i}/{len(ids)} {rec['drive_name']}/{label}")
            else:
                update_progress(job_id, i, len(ids))
        except Exception as e:
            skipped += 1
            update_progress(job_id, i, len(ids), f"error {label}: {e}")

    update_progress(
        job_id, len(ids), len(ids),
        f"Done. Restored {ok}, skipped {skipped}. Destination: {dest_root}",
    )
    finish_job(job_id, "done")

