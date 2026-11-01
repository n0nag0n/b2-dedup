"""Volume detection, path sandbox, and drive-name suggestions."""
from pathlib import Path

from core.volumes import (
    containing_root,
    describe_target,
    discover_volume_paths,
    display_name,
    extra_root_paths,
    is_forbidden_root,
    list_ingest_targets,
    resolve_allowed_path,
    scan_places,
    suggest_drive,
)


def test_display_name_uses_folder_name():
    assert display_name(Path("/media/me/OldSeagate")) == "OldSeagate"


def test_discover_lists_children_and_skips_hidden(tmp_path):
    disk = tmp_path / "OldSeagate"
    disk.mkdir()
    (tmp_path / ".hidden").mkdir()
    (tmp_path / "notes.txt").write_text("no")
    found = discover_volume_paths(parents=[tmp_path])
    names = {path.name for path in found}
    assert names == {"OldSeagate"}


def test_skip_nested_scan_parent(tmp_path):
    user = tmp_path / "n0nag0n"
    user.mkdir()
    disk = user / "CameraCard"
    disk.mkdir()
    found = discover_volume_paths(parents=[tmp_path, user])
    assert {path.name for path in found} == {"CameraCard"}


def test_resolve_stays_inside_allowed_root(tmp_path):
    disk = tmp_path / "disk"
    photos = disk / "Photos"
    photos.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    roots = [disk.resolve()]
    assert resolve_allowed_path(str(photos), roots) == photos.resolve()
    assert resolve_allowed_path(str(outside), roots) is None
    assert resolve_allowed_path("../outside", roots) is None
    assert resolve_allowed_path(str(disk / ".." / "outside"), roots) is None


def test_containing_root_picks_longest_match(tmp_path):
    disk = tmp_path / "disk"
    photos = disk / "Photos"
    photos.mkdir(parents=True)
    roots = [tmp_path.resolve(), disk.resolve()]
    assert containing_root(photos, roots) == disk.resolve()


def test_suggest_drive_keeps_volume_name_in_subfolder(tmp_path):
    volume = tmp_path / "OldSeagate"
    photos = volume / "July" / "Photos"
    photos.mkdir(parents=True)
    name, root = suggest_drive(photos, [volume.resolve()], [])
    assert name == "OldSeagate"
    assert root == volume.resolve()


def test_suggest_drive_uses_child_of_extra_root(tmp_path):
    extra = tmp_path / "old-disks"
    seagate = extra / "Seagate1"
    (seagate / "DCIM").mkdir(parents=True)
    extra = extra.resolve()
    name, root = suggest_drive(seagate / "DCIM", [], [extra])
    assert name == "Seagate1"
    assert root == seagate.resolve()


def test_forbidden_system_roots():
    assert is_forbidden_root(Path("/"))
    assert is_forbidden_root(Path("/etc"))
    assert not is_forbidden_root(Path("/media"))


def test_list_targets_flags_existing_catalog_name(tmp_path):
    disk = tmp_path / "FamilyPhotos"
    disk.mkdir()
    extra = tmp_path / "archive"
    extra.mkdir()
    targets = list_ingest_targets(
        catalog_names=["FamilyPhotos"],
        extra=[extra.resolve()],
        volumes=[disk.resolve()],
    )
    assert targets["disks"][0]["in_catalog"] is True
    assert targets["folders"][0]["in_catalog"] is False
    assert targets["folders"][0]["name"] == "archive"


def test_scan_places_linux():
    info = scan_places(system="Linux", username="sam")
    assert info["os"] == "Linux"
    assert "/media/sam" in info["places"]
    assert "/mnt" in info["places"]


def test_extra_root_from_env(tmp_path, monkeypatch):
    monkeypatch.setenv("HOST_DRIVES_ROOT", str(tmp_path))
    roots = extra_root_paths([])
    assert tmp_path.resolve() in roots


def test_describe_target_size(tmp_path):
    info = describe_target(
        tmp_path,
        kind="folder",
        catalog_names=[],
        suggested_name=tmp_path.name,
    )
    assert info["kind"] == "folder"
    assert info["path"] == str(tmp_path.resolve()) or info["path"] == str(tmp_path)
