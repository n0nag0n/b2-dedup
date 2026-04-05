"""Migration 002 — file_features extension table.

Sparse per-file metadata store for AI/processing features (embeddings, tags,
thumbnails, etc.).  Files uploaded before a feature existed simply have no row;
the absence of a row means "not yet processed" — that's the migration story for
backfill jobs.

Schema:
    file_id      — FK to files(id)
    feature      — feature name + version, e.g. 'embedding_clip_v1', 'ai_tags_v1'
    status       — 'pending' | 'processing' | 'done' | 'error'
    data_json    — feature output (embedding vector, tag list, etc.) as JSON
    error        — error message if status='error'
    processed_at — ISO-8601 timestamp of last successful processing

Backfill query pattern (find un-processed files for a given feature):
    SELECT f.id FROM files f
    LEFT JOIN file_features ff ON ff.file_id = f.id AND ff.feature = 'embedding_clip_v1'
    WHERE ff.file_id IS NULL
      AND f.file_type = 'image'   -- optional: scope to relevant types
"""
import sqlite3


def up(conn: sqlite3.Connection):
    c = conn.cursor()

    c.execute('''
        CREATE TABLE IF NOT EXISTS file_features (
            file_id      INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
            feature      TEXT NOT NULL,
            status       TEXT NOT NULL DEFAULT 'pending',
            data_json    TEXT,
            error        TEXT,
            processed_at TEXT,
            PRIMARY KEY (file_id, feature)
        )
    ''')

    # Fast lookup: all files for a feature at a given status (e.g. worker claiming pending jobs)
    c.execute('''
        CREATE INDEX IF NOT EXISTS idx_file_features_feature_status
        ON file_features(feature, status)
    ''')
