"""Tests for the real migration files in migrations/."""

import psycopg
import pytest
from psycopg.types.json import Jsonb

from pipeline.migrate import apply_migrations

BRONZE_INSERT = (
    "INSERT INTO bronze.raw_records (endpoint, source_id, payload, payload_hash, run_id)"
    " VALUES ('ijzp-q8t2', %s, %s, %s, %s)"
)


def test_migrations_create_the_tables(db):
    for table in ("bronze.raw_records", "ops.ingestion_checkpoint", "ops.quarantine"):
        exists = db.execute("SELECT to_regclass(%s) IS NOT NULL", (table,)).fetchone()[0]
        assert exists, table


def test_running_migrations_again_is_a_no_op(db):
    assert apply_migrations(db) == []


def test_bronze_rejects_the_same_record_version_twice(db, make_run):
    run_id = make_run()
    db.execute(BRONZE_INSERT, ("1", Jsonb({"id": "1"}), "hash-a", run_id))
    db.execute(BRONZE_INSERT, ("1", Jsonb({"id": "1", "arrest": True}), "hash-b", run_id))

    with pytest.raises(psycopg.errors.UniqueViolation):
        db.execute(BRONZE_INSERT, ("1", Jsonb({"id": "1"}), "hash-a", run_id))


def test_bronze_rows_start_as_pending(db, make_run):
    db.execute(BRONZE_INSERT, ("1", Jsonb({"id": "1"}), "hash-a", make_run()))

    status = db.execute("SELECT validation_status FROM bronze.raw_records").fetchone()[0]

    assert status == "pending"


def test_bronze_needs_an_existing_run(db):
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        db.execute(
            BRONZE_INSERT,
            ("1", Jsonb({"id": "1"}), "hash-a", "00000000-0000-0000-0000-000000000000"),
        )


def test_quarantine_allows_a_missing_source_id_once_per_rule(db, make_run):
    insert = (
        "INSERT INTO ops.quarantine (run_id, endpoint, source_id, payload, payload_hash,"
        " rule_name, reason) VALUES (%s, 'ijzp-q8t2', NULL, %s, 'hash-x',"
        " 'missing_source_id', 'record has no id')"
    )
    run_id = make_run()
    db.execute(insert, (run_id, Jsonb({"case_number": "JA1"})))

    with pytest.raises(psycopg.errors.UniqueViolation):
        db.execute(insert, (run_id, Jsonb({"case_number": "JA1"})))
