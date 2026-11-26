"""Shared test fixtures.

Database fixtures need a real PostgreSQL server, named by the same env vars the
pipeline uses: POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD and
POSTGRES_DB (a database we can connect to in order to create test databases).

Every test gets its own brand-new database, dropped afterwards, so existing data
is never touched. When no server is reachable these tests are skipped locally,
but they FAIL when the CI env var is set, so a broken database can't hide in CI.
"""

import os
import uuid
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

from pipeline.migrate import apply_migrations, find_migrations

PIPELINE_RUNS_STUB = Path(__file__).parent / "sql" / "ops_pipeline_runs_stub.sql"


def _in_ci() -> bool:
    return os.environ.get("CI", "").lower() not in ("", "0", "false")


def _database_unavailable(reason: str) -> None:
    message = f"PostgreSQL is not available for integration tests: {reason}"
    if _in_ci():
        pytest.fail(message, pytrace=False)
    pytest.skip(message)


def _server_settings() -> dict[str, str]:
    required = ("POSTGRES_HOST", "POSTGRES_USER", "POSTGRES_DB")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        _database_unavailable(f"{', '.join(missing)} not set")

    settings = {
        "host": os.environ["POSTGRES_HOST"],
        "port": os.environ.get("POSTGRES_PORT", "5432"),
        "user": os.environ["POSTGRES_USER"],
        "dbname": os.environ["POSTGRES_DB"],
    }
    if os.environ.get("POSTGRES_PASSWORD"):
        settings["password"] = os.environ["POSTGRES_PASSWORD"]
    return settings


def pytest_collection_modifyitems(items):
    """Mark every test that uses the database as `integration`."""
    for item in items:
        if "pg_server" in getattr(item, "fixturenames", ()):
            item.add_marker(pytest.mark.integration)


@pytest.fixture(scope="session")
def pg_server():
    """Connection settings for a reachable server, or skip / fail (see module docstring)."""
    settings = _server_settings()
    try:
        with psycopg.connect(**settings, connect_timeout=3):
            pass
    except psycopg.OperationalError as exc:
        _database_unavailable(str(exc).strip())
    return settings


@pytest.fixture
def empty_db(pg_server):
    """An autocommit connection to a brand-new, empty database, dropped after the test."""
    name = f"pipeline_test_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(**pg_server, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        with psycopg.connect(**{**pg_server, "dbname": name}, autocommit=True) as conn:
            yield conn
    finally:
        with psycopg.connect(**pg_server, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name))
            )


@pytest.fixture
def db(empty_db):
    """A fresh database with ops.pipeline_runs and every migration applied."""
    if not any("pipeline_runs" in path.name for path in find_migrations()):
        # Student 3's migration isn't here yet, so use the CLAUDE.md definition.
        empty_db.execute(PIPELINE_RUNS_STUB.read_text(encoding="utf-8"))
    apply_migrations(empty_db)
    return empty_db


@pytest.fixture
def make_run(db):
    """Insert a row into ops.pipeline_runs (bronze rows need one) and return its run_id."""

    def _make(mode="backfill"):
        run_id = uuid.uuid4()
        db.execute(
            "INSERT INTO ops.pipeline_runs (run_id, mode, status) VALUES (%s, %s, 'running')",
            (run_id, mode),
        )
        return run_id

    return _make
