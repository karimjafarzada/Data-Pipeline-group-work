"""Tests for the real migration files in migrations/."""

import uuid
from decimal import Decimal

import psycopg
import pytest
from psycopg.types.json import Jsonb

from pipeline.migrate import apply_migrations, find_migrations
from pipeline.quality.model import CrimeRecord

BRONZE_INSERT = (
    "INSERT INTO bronze.raw_records (endpoint, source_id, payload, payload_hash, run_id)"
    " VALUES ('ijzp-q8t2', %s, %s, %s, %s)"
)


def test_migrations_create_the_tables(db):
    for table in (
        "bronze.raw_records",
        "ops.ingestion_checkpoint",
        "ops.quarantine",
        "ops.quality_gate_results",
        "bronze.promotable_records",
        "silver.crimes",
    ):
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


# --- quality gate migration -------------------------------------------------------------

QUALITY_GATE_MIGRATION = "20261126105728_quality_gate.sql"
GATE_INSERT = (
    "INSERT INTO ops.quality_gate_results (run_id, checked, failed, threshold, passed, decided_at)"
    " VALUES (%s, %s, %s, 0.05, %s, now())"
)


def insert_checked_row(db, run_id, source_id, status="valid"):
    db.execute(
        "INSERT INTO bronze.raw_records (endpoint, source_id, payload, payload_hash, run_id,"
        " validation_status, validated_run_id) VALUES ('ijzp-q8t2', %s, %s, %s, %s, %s, %s)",
        (source_id, Jsonb({"id": source_id}), f"hash-{source_id}", run_id, status, run_id),
    )


def test_a_pending_row_cannot_name_a_validating_run(db, make_run):
    run_id = make_run()
    with pytest.raises(psycopg.errors.CheckViolation):
        insert_checked_row(db, run_id, "1", status="pending")


def test_a_checked_row_must_name_its_validating_run(db, make_run):
    db.execute(BRONZE_INSERT, ("1", Jsonb({"id": "1"}), "hash-a", make_run()))
    with pytest.raises(psycopg.errors.CheckViolation):
        db.execute("UPDATE bronze.raw_records SET validation_status = 'valid'")


def test_rows_checked_before_the_gate_get_their_fetching_run(empty_db, tmp_path):
    # Apply the migrations older than the quality gate one, add rows, then apply the rest.
    for path in find_migrations():
        if path.name < QUALITY_GATE_MIGRATION:
            (tmp_path / path.name).write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    apply_migrations(empty_db, tmp_path)
    run_id = uuid.uuid4()
    empty_db.execute(
        "INSERT INTO ops.pipeline_runs (run_id, mode, status) VALUES (%s, 'backfill', 'success')",
        (run_id,),
    )
    for source_id, status in (("1", "valid"), ("2", "invalid"), ("3", "pending")):
        empty_db.execute(
            "INSERT INTO bronze.raw_records (endpoint, source_id, payload, payload_hash, run_id,"
            " validation_status) VALUES ('ijzp-q8t2', %s, '{}', %s, %s, %s)",
            (source_id, f"hash-{source_id}", run_id, status),
        )

    assert apply_migrations(empty_db)[0] == QUALITY_GATE_MIGRATION

    rows = empty_db.execute(
        "SELECT source_id, validated_run_id FROM bronze.raw_records ORDER BY source_id"
    ).fetchall()
    assert rows == [("1", run_id), ("2", run_id), ("3", None)]
    # No gate result for that run, so its valid row is held until someone releases it.
    assert empty_db.execute("SELECT count(*) FROM bronze.promotable_records").fetchone()[0] == 0


def test_gate_result_stores_the_failure_rate(db, make_run):
    run_id = make_run()
    db.execute(GATE_INSERT, (run_id, 20, 7, False))

    rate = db.execute(
        "SELECT failure_rate FROM ops.quality_gate_results WHERE run_id = %s", (run_id,)
    ).fetchone()[0]

    assert rate == Decimal("0.35")


def test_gate_result_rate_is_zero_when_nothing_was_checked(db, make_run):
    run_id = make_run()
    db.execute(GATE_INSERT, (run_id, 0, 0, True))
    assert db.execute("SELECT failure_rate FROM ops.quality_gate_results").fetchone()[0] == 0


@pytest.mark.parametrize(
    "columns, values",
    [
        ("run_id, passed", "%s, true"),  # half a decision
        ("run_id, release_action, released_by", "%s, 'release', 'amin'"),  # half a release
        ("run_id", "%s"),  # neither a decision nor a release
    ],
)
def test_gate_result_rejects_incomplete_rows(db, make_run, columns, values):
    with pytest.raises(psycopg.errors.CheckViolation):
        db.execute(
            f"INSERT INTO ops.quality_gate_results ({columns}) VALUES ({values})", (make_run(),)
        )


def test_gate_result_rejects_more_failed_than_checked(db, make_run):
    with pytest.raises(psycopg.errors.CheckViolation):
        db.execute(GATE_INSERT, (make_run(), 5, 6, False))


def test_promotable_records_are_valid_rows_of_passed_or_released_runs(db, make_run):
    passed, failed, released, undecided = make_run(), make_run(), make_run(), make_run()
    insert_checked_row(db, passed, "1")
    insert_checked_row(db, passed, "2", status="invalid")
    insert_checked_row(db, failed, "3")
    insert_checked_row(db, released, "4")
    insert_checked_row(db, undecided, "5")  # crashed between validate() and the gate
    db.execute(GATE_INSERT, (passed, 2, 1, True))
    db.execute(GATE_INSERT, (failed, 1, 1, False))
    db.execute(GATE_INSERT, (released, 1, 1, False))
    db.execute(
        "UPDATE ops.quality_gate_results SET release_action = 'release', released_by = 'amin',"
        " release_note = 'checked by hand', released_at = now() WHERE run_id = %s",
        (released,),
    )

    rows = db.execute(
        "SELECT source_id FROM bronze.promotable_records ORDER BY source_id"
    ).fetchall()

    assert [row[0] for row in rows] == ["1", "4"]


# --- silver migration ---------------------------------------------------------------------


def columns(db, schema, table):
    rows = db.execute(
        "SELECT column_name FROM information_schema.columns"
        " WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
        (schema, table),
    ).fetchall()
    return [row[0] for row in rows]


def test_silver_crimes_has_the_crime_record_fields_in_order(db):
    # build_silver() copies CrimeRecord.model_dump() straight into these columns.
    assert columns(db, "silver", "crimes") == [*CrimeRecord.model_fields, "bronze_id", "run_id"]


def test_promotable_records_has_every_bronze_column(db):
    assert columns(db, "bronze", "promotable_records") == columns(db, "bronze", "raw_records")


def test_only_a_valid_row_can_be_handled_by_build_silver(db, make_run):
    run_id = make_run()
    insert_checked_row(db, run_id, "1", status="invalid")
    with pytest.raises(psycopg.errors.CheckViolation):
        db.execute("UPDATE bronze.raw_records SET silver_run_id = %s", (run_id,))


def test_unpromoted_rows_have_a_partial_index(db):
    definition = db.execute(
        "SELECT indexdef FROM pg_indexes WHERE indexname = 'raw_records_unpromoted_idx'"
    ).fetchone()[0]
    assert "WHERE ((validation_status = 'valid'::text) AND (silver_run_id IS NULL))" in definition
