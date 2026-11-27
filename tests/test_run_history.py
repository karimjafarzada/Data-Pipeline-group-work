from datetime import date
from uuid import UUID, uuid4

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



@pytest.mark.parametrize(
    ("error", "expected_attempts"),
    [
        (psycopg.OperationalError("connection refused"), 2),
        (psycopg.errors.InvalidPassword("invalid password"), 1),
        (psycopg.errors.InvalidAuthorizationSpecification("login denied"), 1),
        (psycopg.OperationalError('password authentication failed for user "bad"'), 1),
        (psycopg.OperationalError('role "bad" does not exist'), 1),
    ],
)
def test_connection_failure_is_reraised(monkeypatch, error, expected_attempts):
    attempts = 0

    def connect(**settings):
        nonlocal attempts
        attempts += 1
        raise error

    for name in ("POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER"):
        monkeypatch.setenv(name, "warehouse")
    monkeypatch.setattr(database.psycopg, "connect", connect)
    monkeypatch.setattr(database, "wait_fixed", lambda _: lambda state: 0)

    with pytest.raises(psycopg.OperationalError) as caught:
        database.get_connection(attempts=2)

    assert caught.value is error
    assert attempts == expected_attempts


def test_missing_connection_environment(monkeypatch):
    for name in ("POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER"):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(ValueError, match="POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER"):
        database.get_connection()


@pytest.mark.parametrize("mode", ["incremental", "backfill"])
def test_start_run_rejects_invalid_dates(mode):
    with pytest.raises(ValueError, match="(?i)date"):
        start_run(mode, date(2025, 2, 1), date(2025, 1, 1))


def test_unknown_run_cannot_be_finished(db):
    with pytest.raises(ValueError, match="does not exist"):
        finish_run(uuid4(), "failed", RunCounts(), conn=db)
