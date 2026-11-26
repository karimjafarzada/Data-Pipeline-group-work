from datetime import date
from uuid import UUID

import psycopg
import pytest

from pipeline import db as database
from pipeline.runs import RunCounts, finish_run, start_run


def test_get_connection_retries_when_postgres_is_starting(monkeypatch):
    attempts = []

    def connect(**settings):
        attempts.append(settings)
        if len(attempts) == 1:
            raise psycopg.OperationalError("connection refused")
        return connection

    connection = object()
    monkeypatch.setenv("POSTGRES_HOST", "db")
    monkeypatch.setenv("POSTGRES_DB", "warehouse")
    monkeypatch.setenv("POSTGRES_USER", "warehouse")
    monkeypatch.delenv("POSTGRES_PORT", raising=False)
    monkeypatch.setattr(database.psycopg, "connect", connect)
    monkeypatch.setattr(database, "wait_fixed", lambda _: lambda state: 0)

    assert database.get_connection() is connection
    assert len(attempts) == 2
    assert attempts[1]["host"] == "db"
    assert attempts[1]["port"] == "5432"


def test_run_lifecycle_uses_the_real_migration(db):
    start = date(2025, 1, 1)
    end = date(2025, 1, 3)
    run_id = start_run("backfill", start, end, conn=db)
    assert isinstance(run_id, UUID)
    assert db.execute(
        "SELECT mode, backfill_start, backfill_end, status, finished_at"
        " FROM ops.pipeline_runs WHERE run_id = %s",
        (run_id,),
    ).fetchone() == ("backfill", start, end, "running", None)

    finish_run(run_id, "failed", RunCounts(10, 7, 3), "quality gate failed", conn=db)
    assert db.execute(
        "SELECT status, rows_fetched, rows_valid, rows_quarantined, error_message,"
        " finished_at >= started_at FROM ops.pipeline_runs WHERE run_id = %s",
        (run_id,),
    ).fetchone() == ("failed", 10, 7, 3, "quality gate failed", True)

    with pytest.raises(ValueError, match="already finished"):
        finish_run(run_id, "success", RunCounts(), conn=db)
