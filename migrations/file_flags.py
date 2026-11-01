"""Shared junk/searchable UPDATE used by migrations 003 and 005."""
import sqlite3


def apply_flag_updates(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        UPDATE files SET is_junk = 1
        WHERE file_path LIKE '%/node_modules/%' OR file_path LIKE 'node_modules/%'
           OR file_path LIKE '%/venv/%' OR file_path LIKE 'venv/%'
           OR file_path LIKE '%/.venv/%' OR file_path LIKE '.venv/%'
           OR file_path LIKE '%/vendor/%' OR file_path LIKE 'vendor/%'
           OR file_path LIKE '%/__pycache__/%' OR file_path LIKE '__pycache__/%'
           OR file_path LIKE '%/.Spotlight-V100/%' OR file_path LIKE '.Spotlight-V100/%'
           OR file_path LIKE '%/.Trashes/%' OR file_path LIKE '.Trashes/%'
           OR file_path LIKE '%/.fseventsd/%' OR file_path LIKE '.fseventsd/%'
           OR file_path LIKE '%/__MACOSX/%' OR file_path LIKE '__MACOSX/%'
           OR file_path LIKE '%/.DS_Store' OR file_path = '.DS_Store'
           OR file_path LIKE '%/Thumbs.db' OR file_path = 'Thumbs.db'
           OR file_path LIKE '%/desktop.ini' OR file_path = 'desktop.ini'
           OR file_path GLOB '._*' OR file_path GLOB '*/._*'
        """
    )
    conn.execute(
        """
        UPDATE files SET searchable = 0
        WHERE is_junk = 1
           OR file_path LIKE '%/.git/%' OR file_path LIKE '.git/%'
           OR file_path LIKE '%/.hg/%' OR file_path LIKE '.hg/%'
        """
    )
