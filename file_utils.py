import mimetypes
from pathlib import Path
from datetime import datetime, timezone
import os

# Map extensions to categories
EXTENSION_CATEGORY_MAP = {
    # Code & Development
    'py': 'Code', 'js': 'Code', 'ts': 'Code', 'html': 'Code', 'css': 'Code', 
    'json': 'Code', 'sql': 'Code', 'md': 'Code', 'sh': 'Code', 'yml': 'Code', 
    'yaml': 'Code', 'xml': 'Code', 'c': 'Code', 'cpp': 'Code', 'h': 'Code', 
    'java': 'Code', 'go': 'Code', 'rs': 'Code', 'php': 'Code', 'rb': 'Code',
    'dockerfile': 'Code', 'gitignore': 'Code', 'toml': 'Code', 'jsx': 'Code', 'tsx': 'Code',

    # Spreadsheets
    'csv': 'Spreadsheet', 'xlsx': 'Spreadsheet', 'xls': 'Spreadsheet', 
    'ods': 'Spreadsheet', 'numbers': 'Spreadsheet', 'tsv': 'Spreadsheet',

    # Documents
    'pdf': 'Document', 'docx': 'Document', 'doc': 'Document', 
    'txt': 'Document', 'rtf': 'Document', 'odt': 'Document', 'pages': 'Document',
    'ppt': 'Presentation', 'pptx': 'Presentation', 'key': 'Presentation',

    # Archives
    'zip': 'Archive', 'tar': 'Archive', 'gz': 'Archive', '7z': 'Archive', 
    'rar': 'Archive', 'dmg': 'Archive', 'iso': 'Archive', 'bz2': 'Archive', 'xz': 'Archive',

    # Disk Images
    'img': 'Disk Image', 'vmdk': 'Disk Image', 'qcow2': 'Disk Image', 'vhd': 'Disk Image',

    # Database
    'db': 'Database', 'sqlite': 'Database', 'sqlite3': 'Database', 'mdb': 'Database', 'accdb': 'Database',

    # Executables
    'exe': 'Executable', 'dll': 'Executable', 'so': 'Executable', 
    'bin': 'Executable', 'app': 'Executable', 'msi': 'Executable', 'bat': 'Executable',

    # Fonts
    'ttf': 'Font', 'otf': 'Font', 'woff': 'Font', 'woff2': 'Font',
    
    # Configuration
    'ini': 'Configuration', 'conf': 'Configuration', 'cfg': 'Configuration', 'env': 'Configuration'
}

def determine_file_type(extension: str, mime_type: str) -> str:
    """
    Determine the file category based on extension and mime type.
    Prioritizes extension for specific developer/office types.
    """
    ext = extension.lstrip('.').lower()
    
    # 1. Check strict extension mapping first
    if ext in EXTENSION_CATEGORY_MAP:
        return EXTENSION_CATEGORY_MAP[ext]
    
    # 2. Check MIME type for general media
    if mime_type:
        if mime_type.startswith('image/'):
            return 'Image'
        if mime_type.startswith('video/'):
            return 'Video'
        if mime_type.startswith('audio/'):
            return 'Audio'
        if mime_type.startswith('text/'):
            return 'Document' # Fallback for other text files
        
    return 'Other'

def get_file_metadata(filepath: Path) -> dict:
    """
    Extracts metadata from a file path:
    - mtime, ctime, atime (ISO8601)
    - mime_type
    - file_type
    """
    try:
        stat_result = filepath.stat()
        
        # Timestamps
        mtime = datetime.fromtimestamp(stat_result.st_mtime, tz=timezone.utc).isoformat()
        ctime = datetime.fromtimestamp(stat_result.st_ctime, tz=timezone.utc).isoformat()
        atime = datetime.fromtimestamp(stat_result.st_atime, tz=timezone.utc).isoformat()
        
        # Mime & Type
        mime_type, _ = mimetypes.guess_type(str(filepath))
        # Handle case where mime_type is None
        mime_type = mime_type or "application/octet-stream"
        
        file_type = determine_file_type(filepath.suffix, mime_type)
        
        return {
            "mtime": mtime,
            "ctime": ctime,
            "atime": atime,
            "mime_type": mime_type,
            "file_type": file_type
        }
    except Exception as e:
        # In case of permission error or disappeared file
        return {
            "error": str(e)
        }
