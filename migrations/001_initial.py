"""Migration 001 — baseline schema (everything that existed before versioning)."""
import sqlite3


def up(conn: sqlite3.Connection):
    c = conn.cursor()

    c.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash TEXT NOT NULL,
            size INTEGER NOT NULL,
            drive_name TEXT NOT NULL,
            file_path TEXT NOT NULL,
            upload_path TEXT,
            is_original INTEGER DEFAULT 0,
            created_at TEXT,
            file_mtime TEXT,
            file_ctime TEXT,
            file_atime TEXT,
            mime_type TEXT,
            file_type TEXT,
            UNIQUE(drive_name, file_path)
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            created_at TEXT
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS group_members (
            group_id INTEGER,
            file_id INTEGER,
            added_at TEXT,
            PRIMARY KEY (group_id, file_id),
            FOREIGN KEY(group_id) REFERENCES groups(id) ON DELETE CASCADE,
            FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
        )
    ''')

    # Indexes
    c.execute('CREATE INDEX IF NOT EXISTS idx_files_drive_path ON files(drive_name, file_path)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_files_size ON files(size)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_files_created_at ON files(created_at)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_files_mtime ON files(file_mtime)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_files_ctime ON files(file_ctime)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_files_file_type ON files(file_type)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_files_mime_type ON files(mime_type)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_files_hash_original ON files(hash, is_original)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_group_members_file_id ON group_members(file_id)')

    # FTS5 full-text search
    try:
        c.execute('''
            CREATE VIRTUAL TABLE IF NOT EXISTS files_fts USING fts5(
                file_path,
                content='files',
                content_rowid='id'
            )
        ''')
        c.execute('''
            CREATE TRIGGER IF NOT EXISTS files_ai AFTER INSERT ON files BEGIN
                INSERT INTO files_fts(rowid, file_path) VALUES (new.id, new.file_path);
            END;
        ''')
        c.execute('''
            CREATE TRIGGER IF NOT EXISTS files_ad AFTER DELETE ON files BEGIN
                INSERT INTO files_fts(files_fts, rowid, file_path) VALUES('delete', old.id, old.file_path);
            END;
        ''')
        c.execute('''
            CREATE TRIGGER IF NOT EXISTS files_au AFTER UPDATE ON files BEGIN
                INSERT INTO files_fts(files_fts, rowid, file_path) VALUES('delete', old.id, old.file_path);
                INSERT INTO files_fts(rowid, file_path) VALUES (new.id, new.file_path);
            END;
        ''')
    except sqlite3.OperationalError:
        print("⚠ Warning: SQLite FTS5 not available — search performance may be degraded.")

    # Rebuild FTS index if data exists but index is empty
    try:
        if c.execute("SELECT 1 FROM files LIMIT 1").fetchone():
            row_count = 0
            try:
                row_count = c.execute("SELECT count(*) FROM files_fts_data").fetchone()[0]
            except sqlite3.OperationalError:
                pass
            if row_count <= 10:
                print("⚠ FTS index appears empty. Rebuilding…")
                c.execute("INSERT INTO files_fts(files_fts) VALUES('rebuild')")
                print("✓ FTS index rebuilt.")
    except Exception as e:
        print(f"⚠ Could not verify FTS index: {e}")
