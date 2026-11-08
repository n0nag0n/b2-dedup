"""Move catalog rows and the matching B2 objects.

A folder move keeps the folder's name (`Documents` into `asus-backup` becomes
`asus-backup/Documents`). Moving the contents of the current folder places
those files under the destination. Originals are copied in B2, the catalog row
is updated, pointer JSON that names the old object is rewritten, and the old
object is removed.
"""
from __future__ import annotations

import json

from core.b2 import sanitize_b2_path
from core.db import connect
from core.paths import POINTER_EXTENSION
from core.scratch import ScratchError, sanitize_rel_path


class RelocateError(ValueError):
    """A move that cannot be started, or one file that could not be moved."""


def clean_catalog_path(raw: str) -> str:
    """A relative catalog path. Empty means the top of the drive."""
    text = (raw or "").strip()
    if not text:
        return ""
    try:
        return sanitize_rel_path(text)
    except ScratchError as exc:
        raise RelocateError(str(exc)) from exc


def _like_prefix(prefix: str) -> str:
    escaped = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return escaped + "/%"


def _rows_under(drive: str, prefix: str) -> list[dict]:
    conn = connect(readonly=True)
    try:
        columns = "id, hash, drive_name, file_path, upload_path, is_original"
        if prefix:
            rows = conn.execute(
                f"SELECT {columns} FROM files WHERE drive_name = ? "
                "AND (file_path = ? OR file_path LIKE ? ESCAPE '\\') "
                "ORDER BY file_path",
                (drive, prefix, _like_prefix(prefix)),
            ).fetchall()
        else:
            rows = conn.execute(
                f"SELECT {columns} FROM files WHERE drive_name = ? ORDER BY file_path",
                (drive,),
            ).fetchall()
    finally:
        conn.close()
    return [dict(row) for row in rows]


def _replace_prefix(file_path: str, source: str, dest: str) -> str:
    if not source:
        return f"{dest}/{file_path}" if dest else file_path
    if file_path == source:
        return dest or file_path.rsplit("/", 1)[-1]
    head = source + "/"
    if file_path.startswith(head):
        tail = file_path[len(head):]
        return f"{dest}/{tail}" if dest else tail
    raise RelocateError(f"{file_path} is not inside {source}.")


def _check_nesting(source: str, dest: str, same_drive: bool) -> None:
    if not same_drive or not source or not dest:
        return
    if dest == source:
        raise RelocateError("Those files are already in that folder.")
    if dest.startswith(source + "/"):
        raise RelocateError(
            "That folder is inside the one being moved. Pick a destination beside it."
        )


def _exact_taken(drive: str, paths: list[str], moving_ids: set[int]) -> set[str]:
    if not paths:
        return set()
    conn = connect(readonly=True)
    try:
        found: set[str] = set()
        for start in range(0, len(paths), 400):
            chunk = paths[start:start + 400]
            marks = ",".join("?" for _ in chunk)
            rows = conn.execute(
                f"SELECT id, file_path FROM files WHERE drive_name = ? AND file_path IN ({marks})",
                (drive, *chunk),
            ).fetchall()
            for row in rows:
                if int(row["id"]) not in moving_ids:
                    found.add(row["file_path"])
        return found
    finally:
        conn.close()


def _assemble(source_drive: str, dest_drive: str, pairs: list[tuple[dict, str]]) -> dict:
    if not pairs:
        raise RelocateError("Nothing is stored at that path.")
    moving_ids = {int(row["id"]) for row, _new in pairs}
    old_paths = {row["file_path"] for row, _new in pairs} if source_drive == dest_drive else set()
    kept: list[tuple[dict, str]] = []
    candidate_paths: list[str] = []
    for row, new in pairs:
        if source_drive == dest_drive and new == row["file_path"]:
            continue
        if new in old_paths and new != row["file_path"]:
            raise RelocateError(
                "That destination overlaps files included in this move. "
                "Move one folder at a time."
            )
        kept.append((row, new))
        candidate_paths.append(new)
    if not kept:
        raise RelocateError("Those files are already in that folder.")
    taken = _exact_taken(dest_drive, candidate_paths, moving_ids)
    conflicts = [path for path in candidate_paths if path in taken]
    if conflicts:
        sample = ", ".join(dict.fromkeys(conflicts[:5]))
        raise RelocateError(
            f"Those paths already have files ({sample}). Nothing was moved."
        )
    seen: set[str] = set()
    moves = []
    for row, new in kept:
        if new in seen:
            raise RelocateError(f"{dest_drive}/{new} would be used twice. Nothing was moved.")
        seen.add(new)
        moves.append({
            "id": int(row["id"]),
            "hash": row["hash"] or "",
            "drive": row["drive_name"],
            "old_path": row["file_path"],
            "new_path": new,
            "dest_drive": dest_drive,
            "upload_path": (row["upload_path"] or "").strip(),
            "is_original": bool(row["is_original"]),
        })
    needs_b2 = any(
        (item["is_original"] and item["upload_path"]) or not item["is_original"]
        for item in moves
    )
    return {
        "source_drive": source_drive,
        "dest_drive": dest_drive,
        "moves": moves,
        "needs_b2": needs_b2,
        "count": len(moves),
    }


def plan_move_folder(source_drive: str, source_path: str, into: str, dest_drive: str) -> dict:
    """Move one folder, keeping its name, into ``into`` on ``dest_drive``."""
    source_drive = (source_drive or "").strip()
    dest_drive = (dest_drive or source_drive).strip()
    source = clean_catalog_path(source_path)
    if not source_drive or not dest_drive:
        raise RelocateError("A drive name is required.")
    if not source:
        raise RelocateError("Choose a folder to move.")
    into_path = clean_catalog_path(into)
    if not into_path and dest_drive == source_drive:
        raise RelocateError("Enter the folder to move this into, or choose a different drive.")
    name = source.rsplit("/", 1)[-1]
    dest = f"{into_path}/{name}" if into_path else name
    _check_nesting(source, dest, dest_drive == source_drive)
    rows = _rows_under(source_drive, source)
    pairs = [(row, _replace_prefix(row["file_path"], source, dest)) for row in rows]
    plan = _assemble(source_drive, dest_drive, pairs)
    plan.update({
        "kind": "folder",
        "source_path": source,
        "into": into_path,
        "dest_prefix": dest,
    })
    return plan


def plan_move_contents(source_drive: str, source_path: str, into: str, dest_drive: str) -> dict:
    """Place everything inside ``source_path`` under ``into``.

    At the top of a drive, folder names are kept: ``Documents/a.jpg`` becomes
    ``asus-backup/Documents/a.jpg``. Inside a folder, that folder's own name is
    left behind: ``Work/a.jpg`` becomes ``archive/a.jpg``. Files already inside
    the destination stay put when the move stays on the same drive.
    """
    source_drive = (source_drive or "").strip()
    dest_drive = (dest_drive or source_drive).strip()
    source = clean_catalog_path(source_path)
    into_path = clean_catalog_path(into)
    if not source_drive or not dest_drive:
        raise RelocateError("A drive name is required.")
    if not into_path and dest_drive == source_drive:
        raise RelocateError("Enter the folder to move these files into.")
    if source:
        _check_nesting(source, into_path, dest_drive == source_drive)
    rows = _rows_under(source_drive, source)
    if not rows:
        raise RelocateError("Nothing is stored at that path.")
    pairs: list[tuple[dict, str]] = []
    for row in rows:
        path = row["file_path"]
        if not source and dest_drive == source_drive and into_path:
            if path == into_path or path.startswith(into_path + "/"):
                continue
        pairs.append((row, _replace_prefix(path, source, into_path)))
    if not pairs:
        raise RelocateError("Those files are already in that folder.")
    plan = _assemble(source_drive, dest_drive, pairs)
    plan.update({
        "kind": "contents",
        "source_path": source,
        "into": into_path,
    })
    return plan


def plan_move_file(file_id: int, dest_drive: str, dest_path: str) -> dict:
    """Move one catalog row onto ``dest_drive`` at ``dest_path``."""
    dest = clean_catalog_path(dest_path)
    dest_drive = (dest_drive or "").strip()
    if not dest:
        raise RelocateError("Enter the path to move this file to.")
    if not dest_drive:
        raise RelocateError("A drive name is required.")
    conn = connect(readonly=True)
    try:
        row = conn.execute(
            "SELECT id, hash, drive_name, file_path, upload_path, is_original "
            "FROM files WHERE id = ?",
            (int(file_id),),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        raise RelocateError("That file is not in the catalog.")
    record = dict(row)
    _check_nesting(record["file_path"], dest, dest_drive == record["drive_name"])
    plan = _assemble(record["drive_name"], dest_drive, [(record, dest)])
    plan.update({
        "kind": "file",
        "source_path": record["file_path"],
        "into": "",
        "file_id": int(file_id),
        "dest_path": dest,
    })
    return plan


def plan_from_payload(payload: dict) -> dict:
    """Rebuild a plan from the spec stored on a job."""
    kind = (payload.get("kind") or "").strip()
    dest_drive = payload.get("dest_drive") or payload.get("source_drive") or ""
    if kind == "folder":
        return plan_move_folder(
            payload.get("source_drive") or "",
            payload.get("source_path") or "",
            payload.get("into") or "",
            dest_drive,
        )
    if kind == "contents":
        return plan_move_contents(
            payload.get("source_drive") or "",
            payload.get("source_path") or "",
            payload.get("into") or "",
            dest_drive,
        )
    if kind == "file":
        return plan_move_file(
            int(payload.get("file_id") or 0),
            dest_drive,
            payload.get("dest_path") or "",
        )
    raise RelocateError("That move is not valid.")


def _remote(drive: str, file_path: str) -> str:
    return sanitize_b2_path(f"{drive}/{file_path}")


def _pointer_remote(drive: str, file_path: str) -> str:
    return _remote(drive, file_path) + POINTER_EXTENSION


def _write_row(file_id: int, drive: str, file_path: str, upload_path: str | None) -> None:
    conn = connect()
    try:
        conn.execute(
            "UPDATE files SET drive_name = ?, file_path = ?, upload_path = ? WHERE id = ?",
            (drive, file_path, upload_path or None, file_id),
        )
        conn.commit()
    finally:
        conn.close()


def _pointer_body(raw: bytes, original_map: dict[str, str]) -> tuple[bytes, bool]:
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RelocateError("That pointer file is not catalog JSON.") from exc
    if not isinstance(data, dict) or "original_path" not in data:
        raise RelocateError("That pointer file is not a catalog pointer.")
    current = data.get("original_path") or ""
    updated = original_map.get(current, current)
    if updated == current:
        return raw, False
    data["original_path"] = updated
    return json.dumps(data, indent=2).encode("utf-8"), True


def _move_original(b2, item: dict, original_map: dict[str, str]) -> None:
    old_remote = item["upload_path"]
    new_remote = _remote(item["dest_drive"], item["new_path"])
    if old_remote:
        if b2 is None:
            raise RelocateError("Set a B2 bucket before moving files that are already uploaded.")
        if old_remote != new_remote:
            try:
                b2.copy_remote(old_remote, new_remote)
            except FileNotFoundError as exc:
                raise RelocateError(f"{old_remote} is not in B2.") from exc
            except FileExistsError as exc:
                raise RelocateError(f"{new_remote} is already in B2.") from exc
        original_map[old_remote] = new_remote
        _write_row(item["id"], item["dest_drive"], item["new_path"], new_remote)
        return
    _write_row(item["id"], item["dest_drive"], item["new_path"], None)


def _move_duplicate(b2, item: dict, original_map: dict[str, str]) -> None:
    old_remote = _pointer_remote(item["drive"], item["old_path"])
    new_remote = _pointer_remote(item["dest_drive"], item["new_path"])
    new_upload = item["upload_path"] or None
    if new_upload and new_upload in original_map:
        new_upload = original_map[new_upload]
    if b2 is None:
        _write_row(item["id"], item["dest_drive"], item["new_path"], new_upload)
        return
    try:
        raw = b2.download_file_content(old_remote)
    except Exception as exc:
        raise RelocateError(f"{old_remote} is not in B2.") from exc
    body, changed = _pointer_body(raw, original_map)
    if old_remote != new_remote or changed:
        try:
            if old_remote != new_remote and not changed:
                b2.copy_remote(old_remote, new_remote)
            else:
                b2.upload_bytes(body, new_remote)
        except FileExistsError as exc:
            raise RelocateError(f"{new_remote} is already in B2.") from exc
        _write_row(item["id"], item["dest_drive"], item["new_path"], new_upload)
        if old_remote != new_remote:
            try:
                b2.delete_remote(old_remote)
            except Exception as exc:
                raise RelocateError(f"Moved the catalog row, but left {old_remote} in B2: {exc}") from exc
        return
    _write_row(item["id"], item["dest_drive"], item["new_path"], new_upload)


def _retarget_other_pointers(b2, original_map: dict[str, str], moved_ids: set[int]) -> list[str]:
    """Point duplicates that stayed put at the original's new B2 path."""
    if b2 is None or not original_map:
        return []
    notes: list[str] = []
    hashes = []
    conn = connect(readonly=True)
    try:
        if moved_ids:
            marks = ",".join("?" for _ in moved_ids)
            rows = conn.execute(
                f"SELECT hash FROM files WHERE id IN ({marks}) AND is_original = 1",
                tuple(moved_ids),
            ).fetchall()
            hashes = [row["hash"] for row in rows if row["hash"]]
        if not hashes:
            return notes
        marks = ",".join("?" for _ in hashes)
        dupes = conn.execute(
            f"SELECT id, drive_name, file_path, upload_path FROM files "
            f"WHERE is_original = 0 AND hash IN ({marks})",
            tuple(hashes),
        ).fetchall()
    finally:
        conn.close()
    for row in dupes:
        file_id = int(row["id"])
        if file_id in moved_ids:
            continue
        remote = _pointer_remote(row["drive_name"], row["file_path"])
        try:
            raw = b2.download_file_content(remote)
            body, changed = _pointer_body(raw, original_map)
            if changed:
                b2.upload_bytes(body, remote)
        except Exception as exc:
            notes.append(f"{row['drive_name']}/{row['file_path']}: {exc}")
        upload = (row["upload_path"] or "").strip()
        if upload and upload in original_map:
            _write_row(file_id, row["drive_name"], row["file_path"], original_map[upload])
    return notes


def execute_plan(b2, plan: dict, progress=None) -> dict:
    """Copy, update the catalog, rewrite pointers, then delete the old objects."""
    moves = list(plan.get("moves") or [])
    total = len(moves)
    original_map: dict[str, str] = {}
    errors: list[str] = []
    moved_ids: set[int] = set()
    done = 0

    def tick(line: str | None = None) -> None:
        if progress:
            progress(done, total, line)

    for item in moves:
        if not item["is_original"]:
            continue
        label = f"{item['drive']}/{item['old_path']} → {item['dest_drive']}/{item['new_path']}"
        try:
            _move_original(b2, item, original_map)
            moved_ids.add(item["id"])
            tick(label)
        except Exception as exc:
            errors.append(f"{label}: {exc}")
            tick(f"failed {label}: {exc}")
        done += 1

    for item in moves:
        if item["is_original"]:
            continue
        label = f"{item['drive']}/{item['old_path']} → {item['dest_drive']}/{item['new_path']}"
        try:
            _move_duplicate(b2, item, original_map)
            moved_ids.add(item["id"])
            tick(label)
        except Exception as exc:
            errors.append(f"{label}: {exc}")
            tick(f"failed {label}: {exc}")
        done += 1

    for note in _retarget_other_pointers(b2, original_map, moved_ids):
        errors.append(note)
        tick(f"pointer: {note}")

    for old, new in original_map.items():
        if not old or old == new or b2 is None:
            continue
        try:
            b2.delete_remote(old)
            tick(f"removed {old}")
        except Exception as exc:
            errors.append(f"left the old B2 object {old}: {exc}")
            tick(f"left the old B2 object {old}: {exc}")

    return {"moved": len(moved_ids), "errors": errors, "total": total}


def run_relocate_job(job: dict, b2=None) -> None:
    """Background move used by the job worker."""
    from core.config import get_bucket_name
    from core.jobs import finish_job, update_progress

    job_id = job["id"]
    payload = job["payload"]

    def progress(done, total, line=None):
        update_progress(job_id, done, total, line)

    try:
        plan = plan_from_payload(payload)
    except RelocateError as exc:
        finish_job(job_id, "error", str(exc))
        return

    bucket = (payload.get("bucket") or get_bucket_name() or "").strip()
    if plan["needs_b2"] and b2 is None:
        if not bucket:
            finish_job(job_id, "error", "Set a B2 bucket in Settings first. Nothing was moved.")
            return
        from core.b2 import B2Manager

        try:
            b2 = B2Manager(bucket)
        except Exception as exc:
            finish_job(job_id, "error", str(exc))
            return

    where = payload.get("into") or payload.get("dest_path") or plan["dest_drive"]
    progress(0, plan["count"], f"Move {plan['count']} file(s) to {plan['dest_drive']}/{where}")
    try:
        result = execute_plan(b2, plan, progress)
    except Exception as exc:
        finish_job(job_id, "error", str(exc))
        return

    progress(
        plan["count"],
        plan["count"],
        f"Moved {result['moved']} of {result['total']}.",
    )
    if result["errors"]:
        progress(plan["count"], plan["count"], "Problems:")
        for line in result["errors"][:8]:
            progress(plan["count"], plan["count"], f"  {line}")
        if result["moved"] == 0:
            finish_job(job_id, "error", result["errors"][0])
            return
    finish_job(job_id, "done")
