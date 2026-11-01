"""Detect plugged-in disks and keep ingest browsing inside those roots."""
from __future__ import annotations

import getpass
import os
import platform
import shutil
import string
from pathlib import Path

from core.catalog import format_size

SYSTEM_ROOT_NAMES = {
    "/",
    "/bin",
    "/boot",
    "/dev",
    "/etc",
    "/proc",
    "/root",
    "/sbin",
    "/sys",
    "/tmp",
    "/usr",
    "/var",
}

SKIP_VOLUME_PREFIXES = (".", "com.apple.")
HOST_DRIVES_ENV = "HOST_DRIVES_ROOT"


def display_name(path: Path) -> str:
    name = path.name
    if name:
        return name
    anchor = path.drive or path.anchor.rstrip("\\/")
    return anchor or str(path)


def default_volume_parents(
    system: str | None = None,
    username: str | None = None,
) -> list[Path]:
    """Directories whose children are typically removable volumes."""
    system = system or platform.system()
    username = username or getpass.getuser()
    if system == "Darwin":
        return [Path("/Volumes")]
    if system == "Windows":
        return []
    # Prefer /media/$USER so /media/root (another account) is not a "disk".
    # Bare /media is only used when this machine does not use per-user dirs.
    parents = [
        Path(f"/run/media/{username}"),
        Path(f"/media/{username}"),
        Path("/mnt"),
    ]
    user_media = Path(f"/media/{username}")
    user_run = Path(f"/run/media/{username}")
    if not user_media.is_dir() and not user_run.is_dir():
        parents.append(Path("/media"))
    return parents


def scan_places(system: str | None = None, username: str | None = None) -> dict:
    system = system or platform.system()
    username = username or getpass.getuser()
    if system == "Darwin":
        label = "macOS"
        places = ["/Volumes"]
    elif system == "Windows":
        label = "Windows"
        places = ["drive letters (C:, D:, …)"]
    else:
        label = "Linux"
        places = [
            f"/run/media/{username}",
            f"/media/{username}",
            "/mnt",
        ]
        if not Path(f"/media/{username}").is_dir() and not Path(f"/run/media/{username}").is_dir():
            places.append("/media")
    return {"os": label, "places": places}


def _is_skippable_name(name: str) -> bool:
    return any(name.startswith(prefix) for prefix in SKIP_VOLUME_PREFIXES)


def _windows_drive_roots() -> list[Path]:
    roots = []
    for letter in string.ascii_uppercase:
        candidate = Path(f"{letter}:/")
        try:
            if candidate.exists():
                roots.append(candidate.resolve())
        except OSError:
            continue
    return roots


def discover_volume_paths(
    parents: list[Path] | None = None,
    system: str | None = None,
    username: str | None = None,
) -> list[Path]:
    system = system or platform.system()
    if system == "Windows" and parents is None:
        return _windows_drive_roots()

    parents = parents if parents is not None else default_volume_parents(system, username)
    parent_resolved = []
    for parent in parents:
        try:
            if parent.is_dir():
                parent_resolved.append(parent.resolve())
        except OSError:
            continue
    skip = set(parent_resolved)
    found: list[Path] = []
    seen: set[Path] = set()
    for parent in parent_resolved:
        try:
            children = list(parent.iterdir())
        except OSError:
            continue
        for child in children:
            if _is_skippable_name(child.name):
                continue
            try:
                if child.is_symlink() or not child.is_dir():
                    continue
                # Skip other users' media dirs and anything we cannot list.
                next(child.iterdir(), None)
                resolved = child.resolve()
            except OSError:
                continue
            if resolved in skip or resolved in seen:
                continue
            found.append(resolved)
            seen.add(resolved)
    return found


def extra_root_paths(configured: list[str] | None = None) -> list[Path]:
    """Configured extra folders plus optional HOST_DRIVES_ROOT for tests."""
    if configured is None:
        from core.config import get_ingest_extra_roots

        configured = get_ingest_extra_roots()
    raw_values = list(configured)
    env = (os.environ.get(HOST_DRIVES_ENV) or "").strip()
    if env:
        raw_values.append(env)
    paths: list[Path] = []
    seen: set[Path] = set()
    for raw in raw_values:
        path = Path(raw).expanduser()
        try:
            if not path.is_dir():
                continue
            resolved = path.resolve()
        except OSError:
            continue
        if resolved in seen:
            continue
        paths.append(resolved)
        seen.add(resolved)
    return paths


def allowed_roots(
    extra: list[Path] | None = None,
    volumes: list[Path] | None = None,
) -> list[Path]:
    roots = []
    seen: set[Path] = set()
    for path in (volumes if volumes is not None else discover_volume_paths()) + (
        extra if extra is not None else extra_root_paths()
    ):
        if path in seen:
            continue
        roots.append(path)
        seen.add(path)
    return roots


def containing_root(path: Path, roots: list[Path]) -> Path | None:
    resolved = path.resolve()
    match = None
    match_len = -1
    for root in roots:
        try:
            resolved.relative_to(root)
        except ValueError:
            continue
        length = len(root.parts)
        if length > match_len:
            match = root
            match_len = length
    return match


def resolve_allowed_path(
    raw: str,
    roots: list[Path] | None = None,
    *,
    must_exist: bool = True,
) -> Path | None:
    """Resolve a user-supplied path if it stays inside an allowed root."""
    text = (raw or "").strip()
    if not text:
        return None
    roots = roots if roots is not None else allowed_roots()
    if not roots:
        return None
    candidate = Path(text).expanduser()
    try:
        resolved = candidate.resolve()
    except OSError:
        return None
    if must_exist and not resolved.exists():
        return None
    if containing_root(resolved, roots) is None:
        return None
    return resolved


def is_forbidden_root(path: Path) -> bool:
    try:
        resolved = path.resolve()
    except OSError:
        return True
    posix = resolved.as_posix()
    if posix in SYSTEM_ROOT_NAMES:
        return True
    if platform.system() == "Windows":
        name = resolved.name.lower()
        if name in {"windows", "program files", "program files (x86)"}:
            return True
    return False


def suggest_drive(
    path: Path,
    volume_paths: list[Path],
    extra_roots: list[Path],
) -> tuple[str, Path]:
    """Pick a drive name and path-root for catalog / B2 prefixes."""
    resolved = path.resolve()
    for volume in volume_paths:
        try:
            resolved.relative_to(volume)
            return display_name(volume), volume
        except ValueError:
            continue
    for extra in extra_roots:
        try:
            rel = resolved.relative_to(extra)
        except ValueError:
            continue
        if rel.parts:
            child = extra / rel.parts[0]
            return display_name(child), child
        return display_name(extra), extra
    return display_name(resolved), resolved


def volume_usage(path: Path) -> dict:
    try:
        usage = shutil.disk_usage(path)
    except OSError:
        return {"total": None, "free": None, "total_fmt": "", "free_fmt": ""}
    return {
        "total": usage.total,
        "free": usage.free,
        "total_fmt": format_size(usage.total),
        "free_fmt": format_size(usage.free),
    }


def describe_target(
    path: Path,
    *,
    kind: str,
    catalog_names: set[str] | list[str],
    suggested_name: str,
) -> dict:
    usage = volume_usage(path)
    name = display_name(path)
    in_catalog = suggested_name in set(catalog_names)
    return {
        "name": name,
        "path": str(path),
        "kind": kind,
        "total_fmt": usage["total_fmt"],
        "free_fmt": usage["free_fmt"],
        "suggested_drive": suggested_name,
        "in_catalog": in_catalog,
    }


def list_ingest_targets(
    catalog_names: list[str] | None = None,
    extra: list[Path] | None = None,
    volumes: list[Path] | None = None,
) -> dict:
    catalog = list(catalog_names or [])
    catalog_set = set(catalog)
    volume_paths = volumes if volumes is not None else discover_volume_paths()
    extra_paths = extra if extra is not None else extra_root_paths()
    volume_set = set(volume_paths)

    disks = [
        describe_target(
            path,
            kind="volume",
            catalog_names=catalog_set,
            suggested_name=display_name(path),
        )
        for path in volume_paths
    ]
    folders = []
    for path in extra_paths:
        if path in volume_set:
            continue
        folders.append(
            describe_target(
                path,
                kind="folder",
                catalog_names=catalog_set,
                suggested_name=display_name(path),
            )
        )
    return {
        "disks": disks,
        "folders": folders,
        "catalog_drives": catalog,
        "scan": scan_places(),
    }
