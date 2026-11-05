"""Decide how a catalog file can be shown in the browser.

The file page embeds ``/preview/{id}`` for types a browser can render.
Everything else stays a download. Video and audio stream with Range so a
player can start and seek without saving the file first. Text, images, and
PDFs are size-capped because the browser loads those whole.

HTML and SVG are never served as active documents: HTML is shown as source,
and SVG is served with a script-blocking policy so a stored file cannot run
in this origin.
"""
from __future__ import annotations

import re

from core.catalog import format_size

TEXT_MAX_BYTES = 1 * 1024 * 1024
IMAGE_MAX_BYTES = 40 * 1024 * 1024
PDF_MAX_BYTES = 40 * 1024 * 1024

# Video and audio are not capped. The player asks for byte ranges, so opening
# the page does not pull a multi-gigabyte movie onto disk.
_LIMITS = {
    "text": TEXT_MAX_BYTES,
    "image": IMAGE_MAX_BYTES,
    "pdf": PDF_MAX_BYTES,
}

BROWSER_IMAGE_EXTS = frozenset(
    {"jpg", "jpeg", "png", "gif", "webp", "bmp", "svg", "avif", "ico"}
)
THUMB_ONLY_EXTS = frozenset(
    {
        "heic", "heif", "tif", "tiff",
        "cr2", "cr3", "nef", "arw", "dng", "raf", "orf", "rw2", "srw",
    }
)
AUDIO_EXTS = frozenset(
    {"mp3", "wav", "wave", "ogg", "oga", "flac", "m4a", "aac", "opus", "weba"}
)
VIDEO_EXTS = frozenset({"mp4", "webm", "ogv", "mov", "m4v"})
# Browsers will not paint these. A player element would just sit there broken.
UNPLAYABLE_VIDEO_EXTS = frozenset(
    {"avi", "mkv", "wmv", "flv", "mpg", "mpeg", "m2ts", "mts", "vob", "asf", "divx"}
)
TEXT_EXTS = frozenset(
    {
        "txt", "text", "md", "markdown", "rst", "log", "csv", "tsv",
        "json", "ndjson", "xml", "html", "htm", "css", "scss", "less", "sass",
        "js", "mjs", "cjs", "ts", "jsx", "tsx", "vue", "svelte",
        "py", "rb", "php", "java", "c", "h", "cpp", "hpp", "cc", "hh",
        "go", "rs", "sh", "bash", "zsh", "fish", "ps1", "bat", "cmd",
        "sql", "lua", "pl", "r", "scala", "kt", "kts", "swift", "dart",
        "yml", "yaml", "toml", "ini", "cfg", "conf", "env", "properties",
        "gradle", "tex", "bib", "diff", "patch", "local",
        "gitignore", "gitattributes", "editorconfig", "dockerignore",
    }
)
# Names with no suffix that are still plain text (Makefile, README, .env).
TEXT_LEAF_NAMES = frozenset(
    {
        ".gitignore", ".gitattributes", ".editorconfig", ".dockerignore",
        ".env", ".bashrc", ".zshrc", ".profile", ".vimrc", ".npmrc",
        "makefile", "dockerfile", "containerfile", "gemfile", "rakefile",
        "license", "readme", "changelog", "authors", "copying",
    }
)

BROWSER_IMAGE_MIMES = frozenset(
    {
        "image/jpeg", "image/png", "image/gif", "image/webp", "image/bmp",
        "image/svg+xml", "image/avif", "image/x-icon", "image/vnd.microsoft.icon",
    }
)
THUMB_ONLY_MIMES = frozenset({"image/heic", "image/heif", "image/tiff"})
VIDEO_MIMES = frozenset(
    {"video/mp4", "video/webm", "video/ogg", "video/quicktime"}
)
UNPLAYABLE_VIDEO_MIMES = frozenset(
    {
        "video/x-msvideo", "video/x-matroska", "video/x-ms-wmv",
        "video/x-flv", "video/mpeg",
    }
)
TEXT_MIMES = frozenset(
    {
        "application/json", "application/ld+json", "application/xml",
        "application/javascript", "application/x-javascript",
        "application/sql", "application/x-sh", "application/yaml",
        "application/x-yaml", "application/toml", "application/xhtml+xml",
        "application/x-httpd-php",
    }
)

IMAGE_MIME_BY_EXT = {
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "png": "image/png",
    "gif": "image/gif",
    "webp": "image/webp",
    "bmp": "image/bmp",
    "svg": "image/svg+xml",
    "avif": "image/avif",
    "ico": "image/x-icon",
}
AUDIO_MIME_BY_EXT = {
    "mp3": "audio/mpeg",
    "wav": "audio/wav",
    "wave": "audio/wav",
    "ogg": "audio/ogg",
    "oga": "audio/ogg",
    "flac": "audio/flac",
    "m4a": "audio/mp4",
    "aac": "audio/aac",
    "opus": "audio/ogg",
    "weba": "audio/webm",
}
VIDEO_MIME_BY_EXT = {
    "mp4": "video/mp4",
    "m4v": "video/mp4",
    "webm": "video/webm",
    "ogv": "video/ogg",
    "mov": "video/quicktime",
}

SVG_CSP = "default-src 'none'; style-src 'unsafe-inline'; sandbox"

_RANGE_RE = re.compile(r"bytes=(\d*)-(\d*)$", re.IGNORECASE)
_HASH_RE = re.compile(r"^[0-9a-fA-F]{64}$")

THUMB_FORMAT_NOTE = (
    "This image format does not display in the browser. Showing the stored thumbnail."
)
THUMB_LARGE_NOTE = (
    "The original is too large to preview here. Showing the stored thumbnail."
)
BROWSER_IMAGE_NOTE = "This image format does not display in the browser."
UNPLAYABLE_VIDEO_NOTE = (
    "This video format does not play in the browser. Download it to watch it."
)


class UnsatisfiableRange(ValueError):
    """A Range header asked for bytes outside the file."""


def file_extension(name: str) -> str:
    leaf = str(name or "").replace("\\", "/").rsplit("/", 1)[-1]
    # ".env" has no suffix. ".env.local" and "notes.txt" do.
    if leaf.startswith(".") and "." not in leaf[1:]:
        return ""
    if "." not in leaf:
        return ""
    return leaf.rsplit(".", 1)[-1].lower()


def _leaf(rec: dict) -> str:
    name = (rec.get("name") or "").strip()
    if name:
        return name
    return str(rec.get("file_path") or "").replace("\\", "/").rsplit("/", 1)[-1]


def _mime(rec: dict) -> str:
    return (rec.get("mime_type") or "").split(";", 1)[0].strip().lower()


def classify_preview(rec: dict) -> str | None:
    """``image``, ``thumb_image``, ``text``, ``pdf``, ``audio``, ``video``,
    ``unplayable_video``, or None.

    The extension wins over a stored MIME type. ``thumb_image`` means the
    bytes are a picture the browser cannot paint, so the page may use a
    generated JPEG thumbnail instead.
    """
    leaf = _leaf(rec)
    ext = file_extension(leaf)
    mime = _mime(rec)
    if leaf.lower() in TEXT_LEAF_NAMES:
        return "text"
    if ext in BROWSER_IMAGE_EXTS:
        return "image"
    if ext in THUMB_ONLY_EXTS:
        return "thumb_image"
    if ext == "pdf":
        return "pdf"
    if ext in AUDIO_EXTS:
        return "audio"
    if ext in VIDEO_EXTS:
        return "video"
    if ext in UNPLAYABLE_VIDEO_EXTS:
        return "unplayable_video"
    if ext in TEXT_EXTS:
        return "text"
    if mime in BROWSER_IMAGE_MIMES:
        return "image"
    if mime in THUMB_ONLY_MIMES or mime.startswith("image/"):
        return "thumb_image"
    if mime == "application/pdf":
        return "pdf"
    if mime.startswith("audio/"):
        return "audio"
    if mime in VIDEO_MIMES:
        return "video"
    if mime in UNPLAYABLE_VIDEO_MIMES:
        return "unplayable_video"
    if mime.startswith("text/") or mime in TEXT_MIMES:
        return "text"
    return None


def _too_big(size: int) -> str:
    return (
        f"This file is {format_size(size)}, which is too large to preview here. "
        "Download it to open it locally."
    )


def describe_preview(rec: dict, *, thumb_available: bool = False) -> dict:
    """How the file page should present this row.

    ``kind`` is ``image``, ``text``, ``pdf``, ``audio``, ``video``, ``thumb``,
    or ``none``. ``note`` is a short limitation, or an empty string.
    """
    family = classify_preview(rec)
    size = int(rec.get("size") or 0)
    if family is None:
        return {"kind": "none", "note": ""}
    if family == "thumb_image":
        if thumb_available:
            return {"kind": "thumb", "note": THUMB_FORMAT_NOTE}
        return {"kind": "none", "note": BROWSER_IMAGE_NOTE}
    if family == "unplayable_video":
        return {"kind": "none", "note": UNPLAYABLE_VIDEO_NOTE}
    if family in {"audio", "video"}:
        return {"kind": family, "note": ""}

    limit = _LIMITS[family]
    if size > limit:
        if family == "image" and thumb_available:
            return {"kind": "thumb", "note": THUMB_LARGE_NOTE}
        return {"kind": "none", "note": _too_big(size)}
    if family != "text" and size <= 0:
        return {"kind": "none", "note": ""}
    return {"kind": family, "note": ""}


def preview_media_type(rec: dict, kind: str) -> str:
    """A Content-Type that will not execute the file as a page."""
    if kind == "text":
        return "text/plain; charset=utf-8"
    if kind == "pdf":
        return "application/pdf"
    ext = file_extension(_leaf(rec))
    mime = _mime(rec)
    if kind == "image":
        return IMAGE_MIME_BY_EXT.get(ext) or (
            mime if mime in BROWSER_IMAGE_MIMES else "application/octet-stream"
        )
    if kind == "audio":
        if ext in AUDIO_MIME_BY_EXT:
            return AUDIO_MIME_BY_EXT[ext]
        return mime if mime.startswith("audio/") else "application/octet-stream"
    if kind == "video":
        return VIDEO_MIME_BY_EXT.get(ext) or (
            mime if mime in VIDEO_MIMES else "application/octet-stream"
        )
    return "application/octet-stream"


def preview_etag(rec: dict) -> str | None:
    file_hash = str(rec.get("hash") or "")
    if _HASH_RE.fullmatch(file_hash):
        return f'"{file_hash.lower()}"'
    return None


def decode_preview_text(data: bytes) -> str:
    """UTF-8 (or UTF-16 with a BOM) text. Rejects obvious binary."""
    if data.startswith((b"\xff\xfe", b"\xfe\xff")):
        return data.decode("utf-16")
    if data.startswith(b"\xef\xbb\xbf"):
        data = data[3:]
    if b"\x00" in data[:8192]:
        raise ValueError("This file does not look like text.")
    return data.decode("utf-8", errors="replace")


def parse_byte_range(header: str | None, size: int) -> tuple[int, int] | None:
    """Inclusive byte range, or None to send the whole file.

    A missing size, a malformed header, or a multi-range request sends the
    whole file. A range that starts past the end raises ``UnsatisfiableRange``.
    """
    if not header or size <= 0:
        return None
    header = header.strip()
    if "," in header:
        return None
    match = _RANGE_RE.fullmatch(header)
    if not match:
        return None
    start_s, end_s = match.group(1), match.group(2)
    if start_s == "" and end_s == "":
        return None
    if start_s == "":
        suffix = int(end_s)
        if suffix <= 0:
            raise UnsatisfiableRange()
        return max(size - suffix, 0), size - 1
    start = int(start_s)
    end = int(end_s) if end_s else size - 1
    if start >= size or start > end:
        raise UnsatisfiableRange()
    return start, min(end, size - 1)
