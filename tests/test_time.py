"""Folder dates, EXIF parsing, and moment folder titles."""
import struct

from core.time import (
    SOURCE_EXIF,
    SOURCE_FOLDER,
    SOURCE_MTIME,
    choose_taken_at,
    exif_datetime_from_bytes,
    extract_jpeg_preview,
    is_moment_noise,
    moment_folder,
    moment_title,
    parse_exif_datetime,
    parse_folder_date,
    should_replace,
)


def test_folder_month_year():
    dt = parse_folder_date("Desktop/2016 backup/2016/July 2016/4th of July BBQ/IMG_1.JPG")
    assert dt is not None
    assert (dt.year, dt.month) == (2016, 7)


def test_folder_month_day_year():
    dt = parse_folder_date(
        "Desktop/Photography/20- Moua Family - December 6, 2017/photo.jpg"
    )
    assert dt is not None
    assert (dt.year, dt.month, dt.day) == (2017, 12, 6)


def test_folder_year_fills_month_day_title():
    dt = parse_folder_date("Pictures/Family Life/2014/July 2nd the triplets are here!/IMG.JPG")
    assert dt is not None
    assert (dt.year, dt.month, dt.day) == (2014, 7, 2)


def test_backup_stamp_is_not_a_folder_date():
    dt = parse_folder_date("Austin Dell Backup 12-13-20/Documents/photo.jpg")
    assert dt is None


def test_exact_year_folder():
    dt = parse_folder_date("Pictures/Family Life/2010/snap.jpg")
    assert dt is not None
    assert dt.year == 2010


def test_choose_prefers_exif_then_folder_then_mtime():
    iso, source = choose_taken_at(
        "Pictures/Family Life/2010/August 2010/IMG.JPG",
        "2020-01-01T00:00:00+00:00",
        exif_dt=parse_exif_datetime("2010:08:15 09:00:00"),
    )
    assert source == SOURCE_EXIF
    assert iso.startswith("2010-08-15")

    iso, source = choose_taken_at(
        "Pictures/Family Life/2010/August 2010/IMG.JPG",
        "2020-01-01T00:00:00+00:00",
    )
    assert source == SOURCE_FOLDER
    assert iso.startswith("2010-08")

    iso, source = choose_taken_at("random/photo.jpg", "2011-03-04T12:00:00+00:00")
    assert source == SOURCE_MTIME
    assert iso.startswith("2011-03-04")


def test_implausible_mtime_ignored():
    iso, source = choose_taken_at("random/photo.jpg", "1970-01-01T00:00:00+00:00")
    assert iso is None
    assert source is None


def test_should_not_replace_exif_with_folder():
    assert should_replace(SOURCE_MTIME, SOURCE_FOLDER)
    assert not should_replace(SOURCE_EXIF, SOURCE_FOLDER)


def test_moment_skips_camera_roll_and_picasa():
    folder = moment_folder(
        "Desktop/Photos to organize/They're Both Home!/140CANON/IMG_001.JPG"
    )
    assert folder.endswith("They're Both Home!")
    assert moment_title(folder) == "They're Both Home!"

    folder = moment_folder(
        "Desktop/2016 backup/2016/April 2016/Kaden's Baptism Photos/.picasaoriginals/x.jpg"
    )
    assert folder.endswith("Kaden's Baptism Photos")


def test_chrome_and_version_folders_are_not_moments():
    path = (
        "Austin/AppData/Local/Google/Chrome/User Data/Default/Extensions/"
        "onajjgekdldckfgodnmoallcmdmfcfom/3.5.1_0/icon.png"
    )
    assert is_moment_noise(path)
    assert not is_moment_noise(
        "Desktop/Photos to organize/They're Both Home!/140CANON/IMG_001.JPG"
    )


def test_exif_from_handmade_tiff():
    dt_ascii = b"2016:07:02 14:30:00\x00"
    # IFD0 at 8 with DateTime tag 0x0132 pointing at the string.
    ifd = struct.pack("<H", 1)
    ifd += struct.pack("<HHI", 0x0132, 2, len(dt_ascii))
    ifd += struct.pack("<I", 8 + 2 + 12 + 4)
    ifd += struct.pack("<I", 0)
    data = b"II*\x00" + struct.pack("<I", 8) + ifd + dt_ascii
    dt = exif_datetime_from_bytes(data)
    assert dt is not None
    assert (dt.year, dt.month, dt.day, dt.hour) == (2016, 7, 2, 14)


def test_extract_embedded_jpeg():
    import os
    from io import BytesIO

    from PIL import Image

    buf = BytesIO()
    Image.frombytes("RGB", (240, 180), os.urandom(240 * 180 * 3)).save(
        buf, format="JPEG", quality=85
    )
    jpeg = buf.getvalue()
    assert jpeg[:2] == b"\xff\xd8"
    assert len(jpeg) >= 8000
    blob = b"CR2HEAD" + jpeg + b"END"
    preview = extract_jpeg_preview(blob)
    assert preview == jpeg
