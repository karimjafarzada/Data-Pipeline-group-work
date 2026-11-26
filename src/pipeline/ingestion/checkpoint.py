"""Load and save ingestion checkpoints in ops.ingestion_checkpoint.

Keys (CLAUDE.md, "Tables"):
    incremental:<endpoint>
    backfill:<endpoint>:<start>:<end>

Cursor values:
    backfill     '<id>'                 the highest id already written
    incremental  '<updated_on>|<id>'    the last change already written; updated_on is
                                        the raw API string, e.g. '2025-01-01T15:40:23.000'
"""

from __future__ import annotations

from datetime import date
from uuid import UUID

import psycopg

MODES = ("incremental", "backfill")


def checkpoint_key(
    mode: str, endpoint: str, start: date | None = None, end: date | None = None
) -> str:
    if mode == "incremental":
        if start is not None or end is not None:
            raise ValueError("incremental runs don't take start or end dates")
        return f"incremental:{endpoint}"
    if mode == "backfill":
        if start is None or end is None:
            raise ValueError("a backfill needs both start and end dates")
        return f"backfill:{endpoint}:{start.isoformat()}:{end.isoformat()}"
    raise ValueError(f"mode must be one of {MODES}, got {mode!r}")


def load_checkpoint(conn: psycopg.Connection, key: str) -> str | None:
    """The saved cursor for this key, or None if there is none yet."""
    row = conn.execute(
        "SELECT cursor_value FROM ops.ingestion_checkpoint WHERE checkpoint_key = %s", (key,)
    ).fetchone()
    return row[0] if row else None


def save_checkpoint(
    conn: psycopg.Connection, key: str, cursor_value: str, run_id: UUID
) -> None:
    """Insert or update the checkpoint. It does not commit: the caller owns the transaction."""
    conn.execute(
        "INSERT INTO ops.ingestion_checkpoint (checkpoint_key, cursor_value, last_run_id)"
        " VALUES (%s, %s, %s)"
        " ON CONFLICT (checkpoint_key) DO UPDATE SET"
        " cursor_value = EXCLUDED.cursor_value,"
        " last_run_id = EXCLUDED.last_run_id,"
        " updated_at = now()",
        (key, cursor_value, run_id),
    )


def format_change_cursor(updated_on: str, source_id: int | str) -> str:
    """The incremental cursor: '<updated_on>|<id>'."""
    return f"{updated_on}|{int(source_id)}"


def parse_change_cursor(value: str) -> tuple[str, int]:
    """Split an incremental cursor back into (updated_on, id)."""
    updated_on, separator, source_id = value.rpartition("|")
    if not separator or not updated_on:
        raise ValueError(f"Not an incremental cursor ('<updated_on>|<id>'): {value!r}")
    return updated_on, int(source_id)
