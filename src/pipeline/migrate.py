"""Apply the SQL files in migrations/ in order, each one exactly once.

File names start with a UTC timestamp, e.g. ``20261126120000_bronze_raw_records.sql``,
so teammates adding migrations at the same time don't pick the same number.
Applied files are recorded in ``ops.schema_migrations``, so running this twice is safe.

Each file runs in its own transaction together with its record in
``ops.schema_migrations``: a file is either fully applied and recorded, or not at all.
Don't put BEGIN / COMMIT in migration files.

Run it with: ``python -m pipeline.migrate``
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import psycopg

from pipeline.db import get_connection

logger = logging.getLogger(__name__)

# src/pipeline/migrate.py -> the repository root, which holds migrations/.
MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "migrations"
FILENAME_PATTERN = re.compile(r"^\d{14}_[a-z0-9_]+\.sql$")
# Any fixed number works, as long as every runner uses the same one.
MIGRATION_LOCK_ID = 7_420_001


def find_migrations(directory: Path = MIGRATIONS_DIR) -> list[Path]:
    """All migration files, oldest first. Rejects names without a timestamp prefix."""
    files = sorted(directory.glob("*.sql"))
    bad_names = [f.name for f in files if not FILENAME_PATTERN.match(f.name)]
    if bad_names:
        raise ValueError(
            "Migration file names must look like 20261126120000_description.sql: "
            + ", ".join(bad_names)
        )
    return files


def apply_migrations(conn: psycopg.Connection, directory: Path = MIGRATIONS_DIR) -> list[str]:
    """Apply every migration not yet recorded. Returns the names applied by this call.

    Switches the connection to autocommit, so each ``conn.transaction()`` block
    really commits (psycopg refuses if a transaction is already open).
    """
    migrations = find_migrations(directory)
    conn.autocommit = True

    # Only one runner at a time, e.g. if two containers start together.
    conn.execute("SELECT pg_advisory_lock(%s)", (MIGRATION_LOCK_ID,))
    try:
        with conn.transaction():
            conn.execute("CREATE SCHEMA IF NOT EXISTS ops")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS ops.schema_migrations ("
                " filename TEXT PRIMARY KEY,"
                " applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"
            )
        already_applied = {
            row[0] for row in conn.execute("SELECT filename FROM ops.schema_migrations")
        }

        applied_now = []
        for path in migrations:
            if path.name in already_applied:
                continue
            with conn.transaction():
                conn.execute(path.read_text(encoding="utf-8"))
                conn.execute(
                    "INSERT INTO ops.schema_migrations (filename) VALUES (%s)", (path.name,)
                )
            logger.info("Applied migration %s", path.name)
            applied_now.append(path.name)
        return applied_now
    finally:
        conn.execute("SELECT pg_advisory_unlock(%s)", (MIGRATION_LOCK_ID,))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    with get_connection() as conn:
        applied = apply_migrations(conn)
    if not applied:
        logger.info("Database is already up to date")


if __name__ == "__main__":
    main()
