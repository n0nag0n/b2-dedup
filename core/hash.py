"""SHA-256 hashing with retries for flaky drives."""
import errno
import hashlib
import time
from pathlib import Path

from core.paths import CHUNK_SIZE


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
