"""Remember a search-page selection while the move screen is open.

The session cookie is too small for a long list of file ids. One row per
browser holds the checked files and folders until the next selection replaces it.
"""


def up(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bulk_pick (
            session_key TEXT PRIMARY KEY,
            payload     TEXT NOT NULL,
            updated_at  TEXT NOT NULL
        )
        """
    )
