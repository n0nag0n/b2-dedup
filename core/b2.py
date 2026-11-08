"""Backblaze B2 client wrapper."""
import io
import os
import re
from pathlib import Path
from typing import Optional


def sanitize_b2_path(path_str: str) -> str:
    """
    Sanitize a path for B2 by URL-encoding characters that B2 forbids
    (control characters 0x00-0x1F and 0x7F).
    """
    return re.sub(
        r"[\x00-\x1f\x7f]",
        lambda m: f"%{ord(m.group(0)):02X}",
        path_str,
    )


class B2Manager:
    def __init__(self, bucket_name: str):
        from b2sdk.v2 import B2Api, InMemoryAccountInfo, SqliteAccountInfo

        key_id = os.getenv("B2_KEY_ID")
        app_key = os.getenv("B2_APPLICATION_KEY")

        if key_id and app_key:
            print("✓ Using B2 credentials from environment variables")
            info = InMemoryAccountInfo()
            self.api = B2Api(info)
            self.api.authorize_account("production", key_id, app_key)
        else:
            try:
                info = SqliteAccountInfo()
                self.api = B2Api(info)
                self.api.authorize_automatically()
                print("✓ Using stored B2 credentials (~/.config/b2/account_info)")
            except Exception as e:
                raise RuntimeError(
                    "No B2 credentials found (tried ~/.config/b2/ and env vars B2_KEY_ID/B2_APPLICATION_KEY)"
                ) from e

        self.bucket = self.api.get_bucket_by_name(bucket_name)

    def file_exists(self, remote_path: str) -> bool:
        try:
            for fv in self.bucket.list_file_versions(remote_path, fetch_count=1):
                return fv.file_name == remote_path
        except Exception:
            pass
        return False

    def get_file_info(self, remote_path: str) -> Optional[dict]:
        """Returns {'upload_timestamp_ms': int, 'size': int} or None if not found."""
        try:
            for fv in self.bucket.list_file_versions(remote_path, fetch_count=1):
                if fv.file_name == remote_path:
                    return {"upload_timestamp_ms": fv.upload_timestamp, "size": fv.size}
                break
        except Exception:
            pass
        return None

    def copy_file(self, source_path: str, dest_path: str):
        """Perform a server-side copy of a file if it exists."""
        try:
            fv = self.bucket.get_file_info_by_name(source_path)
            self.bucket.copy(fv.id_, dest_path)
        except Exception as e:
            print(f"Failed to copy {source_path} to {dest_path}: {e}")
            pass

    def find_file_version(self, remote_path: str):
        """Latest version of one object name, or None. Raises on API errors."""
        for fv in self.bucket.list_file_versions(remote_path, fetch_count=1):
            if fv.file_name == remote_path:
                return fv
            return None
        return None

    def copy_remote(self, source_path: str, dest_path: str) -> None:
        """Server-side copy that raises when the source is missing or the dest exists."""
        source = self.find_file_version(source_path)
        if source is None:
            raise FileNotFoundError(source_path)
        if self.find_file_version(dest_path) is not None:
            raise FileExistsError(dest_path)
        self.bucket.copy(source.id_, dest_path)

    def delete_remote(self, remote_path: str) -> None:
        """Delete every version of one object name. Raises when nothing is there."""
        found = False
        for fv in self.bucket.list_file_versions(remote_path):
            if fv.file_name != remote_path:
                break
            found = True
            self.api.delete_file_version(fv.id_, fv.file_name)
        if not found:
            raise FileNotFoundError(remote_path)

    def upload_file(self, local_path: Path, remote_path: str, progress_listener=None):
        """Upload a local file to B2 with retries for transient IO errors."""
        import errno
        import time

        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.bucket.upload_local_file(
                    local_file=str(local_path),
                    file_name=remote_path,
                    progress_listener=progress_listener,
                )
                return
            except OSError as e:
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

    def download_file_range(self, remote_path: str, start: int, end: int) -> bytes:
        """Download an inclusive byte range and return the bytes."""
        downloaded_file = self.bucket.download_file_by_name(
            remote_path, range_=(int(start), int(end))
        )
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
        return self.bucket.ls(prefix)

    def delete_directory(self, prefix: str, progress_callback=None, dry_run=False) -> list[str]:
        """Delete all file versions under a given prefix.

        If dry_run is True, return list of file names without deleting.
        """
        files_to_delete = []
        for file_version, _ in self.list_files(prefix):
            files_to_delete.append(file_version)

        total = len(files_to_delete)
        if dry_run:
            return [fv.file_name for fv in files_to_delete]

        for i, fv in enumerate(files_to_delete):
            try:
                self.api.delete_file_version(fv.id_, fv.file_name)
            except Exception as e:
                print(f"Failed to delete {fv.file_name}: {e}")
            if progress_callback:
                progress_callback(i + 1, total)
        return [fv.file_name for fv in files_to_delete]
