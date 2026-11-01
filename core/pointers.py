"""`.b2ptr` pointer JSON used for duplicate files in B2."""
import json
from datetime import datetime, timezone

from core.paths import POINTER_EXTENSION

__all__ = ["POINTER_EXTENSION", "create_pointer_content"]


def create_pointer_content(original_hash: str, original_path: str) -> bytes:
    """Create JSON content for a pointer file."""
    pointer = {
        "type": "b2_dedup_pointer",
        "version": 1,
        "original_hash": original_hash,
        "original_path": original_path,
        "pointer_created": datetime.now(timezone.utc).isoformat(),
    }
    return json.dumps(pointer, indent=2).encode("utf-8")
