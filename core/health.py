"""Home-page trust summary: catalog backup + last verify. No B2 calls."""
from __future__ import annotations

from core.backup import backup_status
from core.config import get_bucket_name, get_last_verify, get_restore_dest


def _verify_view(last: dict) -> dict:
    if not last:
        return {
            "state": "never",
            "summary": "Not verified yet. Run a sample check from Settings.",
            "at": "",
            "at_fmt": "",
            "mode": "",
            "checked": 0,
            "missing": 0,
            "errors": 0,
            "no_remote": 0,
        }
    missing = int(last.get("missing") or 0)
    errors = int(last.get("errors") or 0)
    if missing or errors:
        state = "issues"
        parts = []
        if missing:
            parts.append(f"{missing:,} missing on B2")
        if errors:
            parts.append(f"{errors:,} lookup error(s)")
        summary = "; ".join(parts) + "."
    else:
        state = "ok"
        mode = last.get("mode") or "sample"
        checked = int(last.get("checked") or 0)
        if mode == "full":
            summary = f"Full check: {checked:,} originals found on B2."
        else:
            summary = f"Sample of {checked:,} originals found on B2."
    at = last.get("at") or ""
    return {
        "state": state,
        "summary": summary,
        "at": at,
        "at_fmt": at[:19].replace("T", " "),
        "mode": last.get("mode") or "",
        "checked": int(last.get("checked") or 0),
        "missing": missing,
        "errors": errors,
        "no_remote": int(last.get("no_remote") or 0),
        "missing_paths": last.get("missing_paths") or [],
    }


def get_vault_health() -> dict:
    backup = backup_status()
    verify = _verify_view(get_last_verify())
    bucket = get_bucket_name()
    return {
        "bucket": bucket,
        "bucket_set": bool(bucket),
        "restore_dest": get_restore_dest(),
        "backup": backup,
        "verify": verify,
        "db_size_fmt": backup["local"]["size_fmt"],
    }
