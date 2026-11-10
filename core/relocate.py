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


def _pack_moves(dest_drive: str, kept: list[tuple[dict, str]]) -> dict:
    """Turn kept (row, new path) pairs into the plan ``execute_plan`` runs."""
    if not kept:
        raise RelocateError("Those files are already in that folder.")
    moving_ids = {int(row["id"]) for row, _new in kept}
    candidate_paths = [new for _row, new in kept]
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
        "source_drive": "",
        "dest_drive": dest_drive,
        "moves": moves,
        "needs_b2": needs_b2,
        "count": len(moves),
    }


def _assemble(source_drive: str, dest_drive: str, pairs: list[tuple[dict, str]]) -> dict:
    if not pairs:
        raise RelocateError("Nothing is stored at that path.")
    same = source_drive == dest_drive
    old_paths = {row["file_path"] for row, _new in pairs} if same else set()
    kept: list[tuple[dict, str]] = []
    for row, new in pairs:
        if same and new == row["file_path"]:
            continue
        if new in old_paths and new != row["file_path"]:
            raise RelocateError(
                "That destination overlaps files included in this move. "
                "Move one folder at a time."
            )
        kept.append((row, new))
    plan = _pack_moves(dest_drive, kept)
    plan["source_drive"] = source_drive
    return plan


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


def _parent_path(path: str) -> str:
    if not path or "/" not in path:
        return ""
    return path.rsplit("/", 1)[0]


def clean_name(raw: str) -> str:
    """A single folder or file name. Slashes would create a path the user did not pick."""
    text = (raw or "").strip()
    if not text:
        return ""
    if "/" in text or "\\" in text:
        raise RelocateError("Use one name, without slashes.")
    cleaned = clean_catalog_path(text)
    if "/" in cleaned:
        raise RelocateError("Use one name, without slashes.")
    return cleaned


def default_move_folder(kind: str, source_path: str, file_path: str = "") -> str:
    """Folder the destination screen opens on.

    A folder opens on its parent, so the folder itself is not a one-click
    destination. A file opens on the folder it is already in. Moving the
    contents of a folder opens on that folder, so nothing moves until a
    different folder is chosen.
    """
    source = clean_catalog_path(source_path) if (source_path or "").strip() else ""
    if kind == "file":
        path = clean_catalog_path(file_path) if (file_path or "").strip() else ""
        return _parent_path(path)
    if kind == "folder":
        return _parent_path(source)
    return source


def destination_paths(
    kind: str,
    source_drive: str,
    source_path: str,
    into: str,
    dest_drive: str,
    *,
    file_path: str = "",
    file_name: str = "",
) -> dict:
    """The From and To labels for a move, without reading the catalog.

    ``to_label`` is empty when the screen still needs a destination folder.
    ``catalog_dest`` is the folder prefix for a contents move, the resulting
    folder for a folder move, and the full file path for a file move.
    """
    kind = (kind or "").strip()
    source_drive = (source_drive or "").strip()
    dest_drive = (dest_drive or source_drive).strip()
    source = clean_catalog_path(source_path) if (source_path or "").strip() else ""
    into_path = clean_catalog_path(into) if (into or "").strip() else ""
    current = clean_catalog_path(file_path) if (file_path or "").strip() else ""
    leaf = clean_name(file_name) if (file_name or "").strip() else ""
    if not source_drive or not dest_drive:
        raise RelocateError("A drive name is required.")
    same = dest_drive == source_drive
    blocked = ""
    kept = ""
    catalog_dest = ""
    title = "Move"
    from_label = f"{source_drive}/{source}" if source else source_drive
    placement = ""

    if kind == "folder":
        if not source:
            raise RelocateError("Choose a folder to move.")
        kept = source.rsplit("/", 1)[-1]
        title = f"Move {kept}"
        catalog_dest = f"{into_path}/{kept}" if into_path else kept
        if same and catalog_dest == source:
            blocked = (
                "This folder is already at the top of this drive. Pick a folder to put it in."
                if not into_path
                else "This folder is already in that folder. Pick a different one."
            )
        else:
            try:
                _check_nesting(source, catalog_dest, same)
            except RelocateError as exc:
                blocked = str(exc)
        if into_path:
            placement = f"{kept} keeps its name and is stored inside {into_path}."
        else:
            placement = f"{kept} keeps its name and is stored at the top of {dest_drive}."
    elif kind == "contents":
        kept = source.rsplit("/", 1)[-1] if source else source_drive
        title = f"Move everything in {kept}"
        catalog_dest = into_path
        if same and into_path == source:
            blocked = (
                "Pick a folder. These files are already at the top of this drive."
                if not source
                else "These files are already in that folder. Pick a different one."
            )
        elif same and not into_path:
            blocked = "Pick a folder. On this drive the files have to land inside a folder."
        elif source:
            try:
                _check_nesting(source, into_path, same)
            except RelocateError as exc:
                blocked = str(exc)
        if not source and into_path:
            placement = f"Each folder keeps its name and is stored inside {into_path}."
        elif into_path:
            placement = (
                f"What is inside {kept} is stored inside {into_path}. "
                f"{kept} is not part of the new path."
            )
        elif not same:
            placement = f"Each folder keeps its name and is stored at the top of {dest_drive}."
    elif kind == "file":
        if not current:
            raise RelocateError("Choose a file to move.")
        if not leaf:
            leaf = current.rsplit("/", 1)[-1]
        kept = leaf
        title = f"Move {leaf}"
        from_label = f"{source_drive}/{current}"
        catalog_dest = f"{into_path}/{leaf}" if into_path else leaf
        if same and catalog_dest == current:
            blocked = "This file is already in that folder. Pick a different one."
        if into_path:
            placement = f"{leaf} is stored inside {into_path}."
        else:
            placement = f"{leaf} is stored at the top of {dest_drive}."
    else:
        raise RelocateError("That move is not valid.")

    same_place = bool(blocked) and (
        (kind == "contents" and into_path == source)
        or (kind == "folder" and catalog_dest == source)
        or (kind == "file" and catalog_dest == current)
    )
    if same_place:
        to_label = ""
        placement = ""
    elif kind == "contents" and into_path:
        to_label = f"{dest_drive}/{into_path}/…"
    elif kind == "contents":
        to_label = f"{dest_drive}/…"
    else:
        to_label = f"{dest_drive}/{catalog_dest}" if catalog_dest else ""
    return {
        "kind": kind,
        "blocked": blocked,
        "title": title,
        "from_label": from_label,
        "to_label": to_label,
        "placement": placement,
        "catalog_dest": catalog_dest,
        "into": into_path,
        "kept": kept,
        "source_path": source,
        "dest_drive": dest_drive,
        "source_drive": source_drive,
    }


def catalog_has_prefix(drive: str, prefix: str) -> bool:
    """True when any catalog row is that path or inside it."""
    prefix = clean_catalog_path(prefix) if (prefix or "").strip() else ""
    if not drive or not prefix:
        return False
    conn = connect(readonly=True)
    try:
        row = conn.execute(
            "SELECT 1 FROM files WHERE drive_name = ? "
            "AND (file_path = ? OR file_path LIKE ? ESCAPE '\\') LIMIT 1",
            (drive, prefix, _like_prefix(prefix)),
        ).fetchone()
    finally:
        conn.close()
    return row is not None


def preview_moves(
    kind: str,
    source_drive: str,
    source_path: str,
    into: str,
    dest_drive: str,
    *,
    limit: int = 3,
) -> list[dict]:
    """A few before/after catalog paths, using the same rewrite as a real move."""
    described = destination_paths(kind, source_drive, source_path, into, dest_drive)
    source = described["source_path"]
    into_path = described["into"]
    source_drive = described["source_drive"]
    dest_drive = described["dest_drive"]
    if kind == "file":
        return []
    conn = connect(readonly=True)
    try:
        if kind == "contents" and not source and into_path:
            rows = conn.execute(
                "SELECT file_path FROM files WHERE drive_name = ? "
                "AND NOT (file_path = ? OR file_path LIKE ? ESCAPE '\\') "
                "ORDER BY file_path LIMIT ?",
                (source_drive, into_path, _like_prefix(into_path), limit),
            ).fetchall()
        elif source or kind == "folder":
            rows = conn.execute(
                "SELECT file_path FROM files WHERE drive_name = ? "
                "AND (file_path = ? OR file_path LIKE ? ESCAPE '\\') "
                "ORDER BY file_path LIMIT ?",
                (source_drive, source, _like_prefix(source), limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT file_path FROM files WHERE drive_name = ? ORDER BY file_path LIMIT ?",
                (source_drive, limit),
            ).fetchall()
    finally:
        conn.close()
    dest_prefix = described["catalog_dest"] if kind == "folder" else into_path
    examples = []
    for row in rows:
        path = row["file_path"]
        new = _replace_prefix(path, source, dest_prefix)
        examples.append({
            "before": f"{source_drive}/{path}",
            "after": f"{dest_drive}/{new}",
        })
    return examples


def _rows_by_ids(ids: list[int]) -> list[dict]:
    if not ids:
        return []
    columns = "id, hash, drive_name, file_path, upload_path, is_original"
    found: dict[int, dict] = {}
    conn = connect(readonly=True)
    try:
        for start in range(0, len(ids), 400):
            chunk = [int(i) for i in ids[start:start + 400]]
            marks = ",".join("?" for _ in chunk)
            rows = conn.execute(
                f"SELECT {columns} FROM files WHERE id IN ({marks})",
                tuple(chunk),
            ).fetchall()
            for row in rows:
                found[int(row["id"])] = dict(row)
    finally:
        conn.close()
    return [found[int(i)] for i in ids if int(i) in found]


def _covered_by_folder(drive: str, path: str, folders: list[tuple[str, str]]) -> bool:
    for folder_drive, folder_path in folders:
        if drive != folder_drive:
            continue
        if path == folder_path or path.startswith(folder_path + "/"):
            return True
    return False


def suggest_selection_folder(
    file_ids: list[int],
    folders: list[tuple[str, str]],
) -> tuple[str, str]:
    """Drive and folder the selection move screen opens on."""
    if folders:
        drive, path = folders[0]
        parent = path.rsplit("/", 1)[0] if "/" in path else ""
        return drive, parent
    rows = _rows_by_ids(list(file_ids)[:1])
    if not rows:
        return "", ""
    path = rows[0]["file_path"]
    parent = path.rsplit("/", 1)[0] if "/" in path else ""
    return rows[0]["drive_name"], parent


def plan_move_selection(
    file_ids: list[int],
    folders: list[tuple[str, str]],
    into: str,
    dest_drive: str,
) -> dict:
    """Move checked files and folders into one existing folder.

    A folder keeps its name. A file keeps its name and lands directly in the
    destination, so ``Photos/2014/a.jpg`` becomes ``inbox/a.jpg``. A file that
    is already inside a checked folder is moved with that folder.
    """
    dest_drive = (dest_drive or "").strip()
    into_path = clean_catalog_path(into) if (into or "").strip() else ""
    if not dest_drive:
        raise RelocateError("A drive name is required.")
    if not into_path:
        raise RelocateError("Pick a folder. Selected items land inside it.")

    normalized: list[tuple[str, str]] = []
    for drive, path in folders:
        drive = (drive or "").strip()
        source = clean_catalog_path(path) if (path or "").strip() else ""
        if not drive or not source:
            raise RelocateError("Choose a folder to move.")
        normalized.append((drive, source))
    kept_folders: list[tuple[str, str]] = []
    for drive, source in normalized:
        if any(
            drive == other_drive and other != source and source.startswith(other + "/")
            for other_drive, other in normalized
        ):
            continue
        kept_folders.append((drive, source))

    pairs: list[tuple[dict, str]] = []
    seen: set[int] = set()
    for drive, source in kept_folders:
        name = source.rsplit("/", 1)[-1]
        dest = f"{into_path}/{name}"
        _check_nesting(source, dest, dest_drive == drive)
        rows = _rows_under(drive, source)
        if not rows:
            raise RelocateError(f"Nothing is stored at {drive}/{source}.")
        for row in rows:
            file_id = int(row["id"])
            if file_id in seen:
                continue
            seen.add(file_id)
            pairs.append((row, _replace_prefix(row["file_path"], source, dest)))

    for row in _rows_by_ids([int(i) for i in file_ids]):
        file_id = int(row["id"])
        if file_id in seen:
            continue
        if _covered_by_folder(row["drive_name"], row["file_path"], kept_folders):
            continue
        leaf = clean_name(row["file_path"].rsplit("/", 1)[-1])
        new = f"{into_path}/{leaf}"
        seen.add(file_id)
        pairs.append((row, new))

    if not pairs:
        raise RelocateError("Nothing is stored at that path.")

    old_paths = {
        row["file_path"]
        for row, _new in pairs
        if row["drive_name"] == dest_drive
    }
    kept_pairs: list[tuple[dict, str]] = []
    for row, new in pairs:
        if row["drive_name"] == dest_drive and new == row["file_path"]:
            continue
        if new in old_paths and new != row["file_path"]:
            raise RelocateError(
                "That destination overlaps files included in this move. "
                "Move one folder at a time."
            )
        kept_pairs.append((row, new))

    plan = _pack_moves(dest_drive, kept_pairs)
    plan.update({
        "kind": "selection",
        "into": into_path,
        "file_ids": [int(i) for i in file_ids],
        "folders": [[drive, path] for drive, path in folders],
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
    if kind == "selection":
        folders = []
        for item in payload.get("folders") or []:
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                folders.append((str(item[0]), str(item[1])))
        return plan_move_selection(
            [int(i) for i in (payload.get("file_ids") or [])],
            folders,
            payload.get("into") or "",
            dest_drive,
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
