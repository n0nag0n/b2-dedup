"""Best-effort capture time: EXIF, then folder name, then filesystem mtime.

Folder titles like ``July 2016`` or ``December 6, 2017`` are treated as
dates. Names that look like backup stamps (``Austin Dell Backup 12-13-20``)
are ignored so the timeline is not a copy-history.
"""
from __future__ import annotations

import re
import struct
from datetime import datetime, timezone
from pathlib import Path

SOURCE_EXIF = "exif"
SOURCE_FOLDER = "folder"
SOURCE_MTIME = "mtime"

SOURCE_RANK = {
    None: 0,
    "": 0,
    SOURCE_MTIME: 1,
    SOURCE_FOLDER: 2,
    SOURCE_EXIF: 3,
}

MIN_PLAUSIBLE_YEAR = 1985
EXIF_MIN_YEAR = 1990

MONTHS = {
    "january": 1, "jan": 1,
    "february": 2, "feb": 2,
    "march": 3, "mar": 3,
    "april": 4, "apr": 4,
    "may": 5,
    "june": 6, "jun": 6,
    "july": 7, "jul": 7,
    "august": 8, "aug": 8,
    "september": 9, "sep": 9, "sept": 9,
    "october": 10, "oct": 10,
    "november": 11, "nov": 11,
    "december": 12, "dec": 12,
}

_MONTH_ALT = "|".join(sorted(MONTHS, key=len, reverse=True))

# Camera / export folders that should not become moment titles.
GENERIC_FOLDER_NAMES = frozenset({
    "dcim", "images", "image", "pictures", "picture", "photos", "photo",
    "img", "pics", "pic", "media", "camera", "originals", "original",
    "fullsize", "full", "thumbs", "thumb", "thumbnail", "thumbnails",
    "favorites", "favourite", "favourites", ".picasaoriginals",
    "picasaoriginals", "canonsmsp", "canonmsc", "misc", "tmp", "temp",
    "100media", "video", "videos", "movie", "movies",
    "assets", "static", "www",
})

_CAMERA_ROLL = re.compile(
    r"^(?:\d{3}(?:CANON|NIKON|APPLE|MSDCF|MEDIA)|CANONMSC)$",
    re.I,
)
_WEAK_TITLE = re.compile(
    r"^(?:\d+|\d+(?:\.\d+)+(?:_\d+)?|(?:19|20)\d{2}[-_]\d{2}(?:[-_]\d{2})?)$"
)
_MOMENT_SKIP_PARTS = frozenset({
    "appdata", "application data", "application support",
    "extensions", "cache", "caches", "tmp", "temp",
})
_WEAK_CONTEXT = re.compile(r"backup|copy|archive|\bold\b", re.I)

_ISO_DATE = re.compile(
    r"(?<!\d)((?:19|20)\d{2})[-_/.](\d{1,2})[-_/.](\d{1,2})(?!\d)"
)
_COMPACT = re.compile(r"(?<!\d)((?:19|20)\d{2})(\d{2})(\d{2})(?!\d)")
_ISO_MONTH = re.compile(r"(?<!\d)((?:19|20)\d{2})[-_/.](\d{1,2})(?!\d)")
_MONTH_DAY_YEAR = re.compile(
    rf"\b({_MONTH_ALT})\s+(\d{{1,2}})(?:st|nd|rd|th)?,?\s+((?:19|20)\d{{2}})\b",
    re.I,
)
_MONTH_YEAR = re.compile(rf"\b({_MONTH_ALT})\s+((?:19|20)\d{{2}})\b", re.I)
_MONTH_DAY = re.compile(
    rf"\b({_MONTH_ALT})\s+(\d{{1,2}})(?:st|nd|rd|th)?\b",
    re.I,
)
_US_DATE = re.compile(
    r"(?<!\d)(\d{1,2})[-/](\d{1,2})[-/]((?:19|20)\d{2}|\d{2})(?!\d)"
)
_YEAR_ONLY = re.compile(r"(?<!\d)((?:19|20)\d{2})(?!\d)")
_EXIF_DT = re.compile(
    r"^(\d{4}):(\d{2}):(\d{2})[ T](\d{2}):(\d{2}):(\d{2})"
)

# TIFF / EXIF
_TAG_DATETIME = 0x0132
_TAG_EXIF_IFD = 0x8769
_TAG_DATETIME_ORIGINAL = 0x9003
_TAG_DATETIME_DIGITIZED = 0x9004
_TAG_JPEG_OFF = 0x0201
_TAG_JPEG_LEN = 0x0202
_TAG_COMPRESSION = 0x0103
_TAG_STRIP_OFF = 0x0111
_TAG_STRIP_LEN = 0x0117
_TYPE_SIZES = {
    1: 1, 2: 1, 3: 2, 4: 4, 5: 8, 6: 1, 7: 1, 8: 2, 9: 4, 10: 8, 11: 4, 12: 8,
}


def current_year() -> int:
    return datetime.now(timezone.utc).year


def is_plausible_year(year: int, *, for_exif: bool = False) -> bool:
    lo = EXIF_MIN_YEAR if for_exif else MIN_PLAUSIBLE_YEAR
    return lo <= year <= current_year() + 1


def parse_exif_datetime(text: str | None) -> datetime | None:
    if not text:
        return None
    match = _EXIF_DT.match(str(text).strip())
    if not match:
        return None
    year, month, day, hour, minute, second = (int(g) for g in match.groups())
    if not is_plausible_year(year, for_exif=True):
        return None
    try:
        return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
    except ValueError:
        return None


def parse_iso_datetime(text: str | None) -> datetime | None:
    if not text:
        return None
    raw = str(text).strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return parse_exif_datetime(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    if not is_plausible_year(dt.year):
        return None
    return dt.astimezone(timezone.utc)


def to_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _valid_ymd(year: int, month: int, day: int) -> datetime | None:
    if not is_plausible_year(year):
        return None
    try:
        return datetime(year, month, day, tzinfo=timezone.utc)
    except ValueError:
        return None


def _month_num(name: str) -> int | None:
    return MONTHS.get(name.lower())


class _Hint:
    """A date parsed from a path segment, with how much of it we trust."""

    def __init__(self, year=None, month=None, day=None, weak=False):
        self.year = year
        self.month = month
        self.day = day
        self.weak = weak

    @property
    def specificity(self) -> int:
        if self.day and self.month and self.year:
            return 3
        if self.month and self.year:
            return 2
        if self.year:
            return 1
        if self.month and self.day:
            return 2
        return 0

    def to_datetime(self) -> datetime | None:
        if not self.year:
            return None
        month = self.month or 1
        day = self.day or 1
        return _valid_ymd(self.year, month, day)


def _parse_segment(text: str) -> _Hint | None:
    if not text:
        return None
    weak = bool(_WEAK_CONTEXT.search(text))

    match = _MONTH_DAY_YEAR.search(text)
    if match:
        month = _month_num(match.group(1))
        if month:
            return _Hint(int(match.group(3)), month, int(match.group(2)), weak=False)

    match = _MONTH_YEAR.search(text)
    if match:
        month = _month_num(match.group(1))
        if month:
            return _Hint(int(match.group(2)), month, None, weak=False)

    match = _MONTH_DAY.search(text)
    if match:
        month = _month_num(match.group(1))
        if month:
            return _Hint(None, month, int(match.group(2)), weak=False)

    match = _ISO_DATE.search(text)
    if match:
        year, month, day = (int(g) for g in match.groups())
        if _valid_ymd(year, month, day) and not weak:
            return _Hint(year, month, day, weak=False)

    match = _COMPACT.search(text)
    if match:
        year, month, day = (int(g) for g in match.groups())
        if _valid_ymd(year, month, day) and not weak:
            return _Hint(year, month, day, weak=False)

    match = _ISO_MONTH.search(text)
    if match:
        year, month = int(match.group(1)), int(match.group(2))
        if 1 <= month <= 12 and is_plausible_year(year) and not weak:
            return _Hint(year, month, None, weak=False)

    match = _US_DATE.search(text)
    if match and not weak:
        month, day, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
        if year < 100:
            year += 2000 if year < 80 else 1900
        if _valid_ymd(year, month, day):
            return _Hint(year, month, day, weak=False)

    # Exact year folder ("2010", "2016") only — not "2016 backup".
    stripped = text.strip()
    if re.fullmatch(r"(?:19|20)\d{2}", stripped):
        year = int(stripped)
        if is_plausible_year(year):
            return _Hint(year, None, None, weak=False)

    match = _YEAR_ONLY.search(text)
    if match and not weak and re.fullmatch(r"(?:19|20)\d{2}", stripped):
        return _Hint(int(match.group(1)), None, None, weak=False)

    return None


def parse_folder_date(file_path: str) -> datetime | None:
    """Best date encoded in folder (and file) names along the path."""
    parts = [p for p in str(file_path).replace("\\", "/").split("/") if p]
    if not parts:
        return None

    hint = _Hint()
    # Walk from the file toward the root so a parent year can fill a child month.
    for part in reversed(parts):
        found = _parse_segment(part)
        if not found:
            continue
        if found.year and hint.year is None:
            hint.year = found.year
        if found.month and hint.month is None:
            hint.month = found.month
        if found.day and hint.day is None:
            hint.day = found.day
        if hint.year and hint.month and hint.day:
            break
    return hint.to_datetime()


def path_parts(file_path: str) -> list[str]:
    return [p for p in str(file_path).replace("\\", "/").split("/") if p]


def parent_folder(file_path: str) -> str:
    parts = path_parts(file_path)
    if len(parts) <= 1:
        return ""
    return "/".join(parts[:-1])


def is_generic_folder(name: str) -> bool:
    if not name:
        return True
    if name.lower() in GENERIC_FOLDER_NAMES:
        return True
    if name.startswith("."):
        return True
    if _WEAK_TITLE.match(name):
        return True
    return bool(_CAMERA_ROLL.match(name))


def is_moment_noise(file_path: str) -> bool:
    """True for software trees that should not become timeline moments."""
    return any(part.lower() in _MOMENT_SKIP_PARTS for part in path_parts(file_path))


def moment_folder(file_path: str) -> str:
    """Parent path used to group a moment (skips DCIM / 140CANON / etc.)."""
    parts = path_parts(file_path)
    if len(parts) <= 1:
        return ""
    folders = parts[:-1]
    while folders and is_generic_folder(folders[-1]):
        folders.pop()
    return "/".join(folders)


def moment_title(folder: str) -> str:
    if not folder:
        return "Camera roll"
    name = folder.replace("\\", "/").rstrip("/").split("/")[-1]
    return name or "Camera roll"


def choose_taken_at(
    file_path: str,
    file_mtime: str | None,
    *,
    exif_dt: datetime | None = None,
) -> tuple[str | None, str | None]:
    """Return ``(iso_timestamp, source)`` using EXIF > folder > mtime."""
    if exif_dt is not None:
        return to_iso(exif_dt), SOURCE_EXIF

    folder_dt = parse_folder_date(file_path)
    mtime_dt = parse_iso_datetime(file_mtime)

    if folder_dt and mtime_dt:
        # Folder often has year/month only (day defaults to 1). If mtime
        # landed in that same month, keep the more precise day-of-month.
        if (
            folder_dt.year == mtime_dt.year
            and folder_dt.month == mtime_dt.month
            and folder_dt.day == 1
            and mtime_dt.day != 1
        ):
            return to_iso(mtime_dt.replace(hour=0, minute=0, second=0, microsecond=0)), SOURCE_FOLDER
        return to_iso(folder_dt), SOURCE_FOLDER

    if folder_dt:
        return to_iso(folder_dt), SOURCE_FOLDER
    if mtime_dt:
        return to_iso(mtime_dt), SOURCE_MTIME
    return None, None


def should_replace(existing_source: str | None, new_source: str | None) -> bool:
    return SOURCE_RANK.get(new_source, 0) > SOURCE_RANK.get(existing_source, 0)


def _tiff_endian(data: bytes) -> str | None:
    if len(data) < 8:
        return None
    if data[:2] == b"II":
        return "<"
    if data[:2] == b"MM":
        return ">"
    return None


def _read_ifd(data: bytes, offset: int, endian: str) -> list[tuple[int, int, int, bytes]]:
    if offset < 0 or offset + 2 > len(data):
        return []
    (count,) = struct.unpack(endian + "H", data[offset:offset + 2])
    entries = []
    pos = offset + 2
    for _ in range(min(count, 256)):
        if pos + 12 > len(data):
            break
        tag, typ, cnt = struct.unpack(endian + "HHI", data[pos:pos + 8])
        raw = data[pos + 8:pos + 12]
        entries.append((tag, typ, cnt, raw))
        pos += 12
    return entries


def _ifd_next(data: bytes, offset: int, endian: str) -> int:
    if offset + 2 > len(data):
        return 0
    (count,) = struct.unpack(endian + "H", data[offset:offset + 2])
    tail = offset + 2 + count * 12
    if tail + 4 > len(data):
        return 0
    (nxt,) = struct.unpack(endian + "I", data[tail:tail + 4])
    return int(nxt)


def _value_offset(raw: bytes, typ: int, cnt: int, endian: str) -> int:
    size = _TYPE_SIZES.get(typ, 1) * cnt
    if size <= 4:
        return 0
    (off,) = struct.unpack(endian + "I", raw)
    return int(off)


def _as_int(raw: bytes, typ: int, endian: str) -> int:
    if typ == 3:
        (val,) = struct.unpack(endian + "H", raw[:2])
        return int(val)
    (val,) = struct.unpack(endian + "I", raw[:4])
    return int(val)


def _as_ascii(data: bytes, raw: bytes, typ: int, cnt: int, endian: str) -> str:
    if typ != 2 or cnt <= 0:
        return ""
    if cnt <= 4:
        blob = raw[:cnt]
    else:
        off = _value_offset(raw, typ, cnt, endian)
        blob = data[off:off + cnt]
    return blob.split(b"\x00", 1)[0].decode("ascii", errors="replace")


def _walk_ifds(data: bytes) -> list[list[tuple[int, int, int, bytes]]]:
    endian = _tiff_endian(data)
    if not endian or data[2:4] not in (b"\x2a\x00", b"\x00\x2a"):
        return []
    (first,) = struct.unpack(endian + "I", data[4:8])
    ifds = []
    offset = int(first)
    seen = set()
    for _ in range(8):
        if offset <= 0 or offset in seen or offset >= len(data):
            break
        seen.add(offset)
        entries = _read_ifd(data, offset, endian)
        if not entries:
            break
        ifds.append(entries)
        offset = _ifd_next(data, offset, endian)
    return ifds


def exif_datetime_from_bytes(data: bytes) -> datetime | None:
    """Read DateTimeOriginal (or DateTime) from a JPEG/TIFF/CR2 header."""
    if not data:
        return None
    if data[:2] == b"\xff\xd8":
        text = _jpeg_exif_datetime(data)
        return parse_exif_datetime(text)
    endian = _tiff_endian(data)
    if not endian:
        return None
    texts = []
    for entries in _walk_ifds(data):
        for tag, typ, cnt, raw in entries:
            if tag in (_TAG_DATETIME_ORIGINAL, _TAG_DATETIME_DIGITIZED, _TAG_DATETIME):
                texts.append((tag, _as_ascii(data, raw, typ, cnt, endian)))
            elif tag == _TAG_EXIF_IFD:
                off = _as_int(raw, 4, endian)
                for etag, etyp, ecnt, eraw in _read_ifd(data, off, endian):
                    if etag in (_TAG_DATETIME_ORIGINAL, _TAG_DATETIME_DIGITIZED, _TAG_DATETIME):
                        texts.append((etag, _as_ascii(data, eraw, etyp, ecnt, endian)))
    preferred = { _TAG_DATETIME_ORIGINAL: 0, _TAG_DATETIME_DIGITIZED: 1, _TAG_DATETIME: 2 }
    texts.sort(key=lambda item: preferred.get(item[0], 9))
    for _tag, text in texts:
        dt = parse_exif_datetime(text)
        if dt:
            return dt
    return None


def _jpeg_exif_datetime(data: bytes) -> str | None:
    pos = 2
    end = len(data)
    while pos + 4 < end and data[pos] == 0xFF:
        marker = data[pos + 1]
        if marker in (0xD8, 0xD9) or marker == 0x00:
            pos += 1
            continue
        if marker == 0xDA:
            break
        seglen = struct.unpack(">H", data[pos + 2:pos + 4])[0]
        if seglen < 2 or pos + 2 + seglen > end:
            break
        payload = data[pos + 4:pos + 2 + seglen]
        if marker == 0xE1 and payload.startswith(b"Exif\x00\x00"):
            dt = exif_datetime_from_bytes(payload[6:])
            return dt.strftime("%Y:%m:%d %H:%M:%S") if dt else None
        pos += 2 + seglen
    return None


def extract_jpeg_preview(data: bytes) -> bytes | None:
    """Pull an embedded JPEG out of a TIFF/CR2 header, or the largest JPEG scan."""
    endian = _tiff_endian(data)
    if endian:
        preview = _tiff_jpeg_preview(data, endian)
        if preview:
            return preview
    return _scan_jpeg(data)


def _tiff_jpeg_preview(data: bytes, endian: str) -> bytes | None:
    best = None
    for entries in _walk_ifds(data):
        off = length = compression = strip_off = strip_len = None
        for tag, typ, _cnt, raw in entries:
            if tag == _TAG_JPEG_OFF:
                off = _as_int(raw, typ, endian)
            elif tag == _TAG_JPEG_LEN:
                length = _as_int(raw, typ, endian)
            elif tag == _TAG_COMPRESSION:
                compression = _as_int(raw, typ, endian)
            elif tag == _TAG_STRIP_OFF:
                strip_off = _as_int(raw, typ, endian)
            elif tag == _TAG_STRIP_LEN:
                strip_len = _as_int(raw, typ, endian)
        if off and length and 0 < length < 8_000_000 and off + length <= len(data):
            blob = data[off:off + length]
            if blob[:2] == b"\xff\xd8":
                if best is None or len(blob) > len(best):
                    best = blob
        if compression == 6 and strip_off and strip_len:
            if 0 < strip_len < 8_000_000 and strip_off + strip_len <= len(data):
                blob = data[strip_off:strip_off + strip_len]
                if blob[:2] == b"\xff\xd8" and (best is None or len(blob) > len(best)):
                    best = blob
    return best


def _scan_jpeg(data: bytes) -> bytes | None:
    best = None
    start = data.find(b"\xff\xd8")
    while start != -1:
        end = data.find(b"\xff\xd9", start + 2)
        if end == -1:
            break
        blob = data[start:end + 2]
        if 8_000 <= len(blob) <= 8_000_000 and (best is None or len(blob) > len(best)):
            best = blob
        start = data.find(b"\xff\xd8", start + 2)
    return best


def exif_datetime_from_path(path: Path | str) -> datetime | None:
    path = Path(path)
    try:
        with open(path, "rb") as fh:
            header = fh.read(256 * 1024)
    except OSError:
        return None
    dt = exif_datetime_from_bytes(header)
    if dt:
        return dt
    # Pillow understands more JPEG/TIFF variants than the header walk.
    try:
        from PIL import Image
        from PIL.ExifTags import Base, IFD
    except ImportError:
        return None
    try:
        with Image.open(path) as im:
            exif = im.getexif()
            if not exif:
                return None
            for tag in (Base.DateTimeOriginal, Base.DateTimeDigitized, Base.DateTime):
                text = exif.get(tag)
                if not text:
                    try:
                        text = exif.get_ifd(IFD.Exif).get(tag)
                    except Exception:
                        text = None
                dt = parse_exif_datetime(text if isinstance(text, str) else None)
                if dt:
                    return dt
    except Exception:
        return None
    return None
