"""Held runs: a failed run's valid rows stay out of silver until a person releases them."""

from datetime import datetime

import pytest

from pipeline.ingestion.bronze import CHICAGO, write_page
from pipeline.ingestion.client import DATASET_ID
from pipeline.ingestion.ingest import IngestStats
from pipeline.quality.gate import QualityGateError, run_quality_gate
from pipeline.quality.release import RECHECK, RELEASE, ReleaseError, release_run
from pipeline.quality.validate import validate

NOW = datetime(2026, 11, 26, 12, 0, tzinfo=CHICAGO)


@pytest.fixture
def record(crime_record):
    """A valid record with the given id, plus changed fields."""

    def _record(record_id, **changes):
        return {**crime_record, "id": str(record_id), **changes}

    return _record


def run_pipeline(db, run_id, records, *, gate=True):
    """ingest (straight into bronze) -> validate -> gate, as the CLI will run them."""
    write_page(db, run_id, DATASET_ID, records)
    validation_stats = validate(run_id, conn=db, now=NOW)
    if gate:
        run_quality_gate(
            run_id, IngestStats(rows_fetched=len(records)), validation_stats,
            conn=db, threshold="0.05",
        )


def failed_run(db, make_run, record):
    """A run that fails the gate: ids 1 and 2 valid, id 3 broken (1 in 3 = 33%)."""
    run_id = make_run()
    with pytest.raises(QualityGateError):
        run_pipeline(db, run_id, [record(1), record(2), record(3, ward="N/A")])
    return run_id


def promotable(db):
    rows = db.execute("SELECT source_id FROM bronze.promotable_records").fetchall()
    return sorted(int(row[0]) for row in rows)


def bronze(db):
    return db.execute(
        "SELECT source_id, validation_status, validated_run_id"
        " FROM bronze.raw_records ORDER BY source_id::int"
    ).fetchall()


def release_record(db, run_id):
    return db.execute(
        "SELECT passed, release_action, released_by, release_note, released_at IS NOT NULL"
        " FROM ops.quality_gate_results WHERE run_id = %s",
        (run_id,),
    ).fetchone()


def test_a_later_passing_run_does_not_publish_a_failed_runs_valid_rows(db, make_run, record):
    failed = failed_run(db, make_run, record)

    run_pipeline(db, make_run(), [record(4), record(5), record(6)])

    assert promotable(db) == [4, 5, 6]
    # Rows 1 and 2 are still valid, and still held by the failed run.
    assert bronze(db)[:2] == [("1", "valid", failed), ("2", "valid", failed)]


def test_release_makes_the_held_rows_promotable(db, make_run, record):
    failed = failed_run(db, make_run, record)

    released = release_run(failed, RELEASE, released_by="amin", note="ward N/A is one bad row",
                           conn=db)

    assert released == 2
    assert promotable(db) == [1, 2]
    # The gate's decision is kept; the release is recorded next to it.
    assert release_record(db, failed) == (False, RELEASE, "amin", "ward N/A is one bad row", True)


def test_recheck_sends_the_held_rows_back_to_validation(db, make_run, record):
    failed = failed_run(db, make_run, record)
    quarantine_before = db.execute("SELECT count(*) FROM ops.quarantine").fetchone()[0]

    rechecked = release_run(failed, RECHECK, released_by="amin", note="added a rule", conn=db)

    assert rechecked == 2
    assert bronze(db) == [("1", "pending", None), ("2", "pending", None), ("3", "invalid", failed)]
    assert db.execute("SELECT count(*) FROM ops.quarantine").fetchone()[0] == quarantine_before
    assert promotable(db) == []

    # The next run checks them again, and its gate decides.
    next_run = make_run()
    run_pipeline(db, next_run, [record(4)])
    assert promotable(db) == [1, 2, 4]
    assert [row[2] for row in bronze(db)] == [next_run, next_run, failed, next_run]


def test_a_run_that_crashed_before_the_gate_is_held_and_can_be_released(db, make_run, record):
    crashed = make_run()
    run_pipeline(db, crashed, [record(1), record(2)], gate=False)
    assert promotable(db) == []

    release_run(crashed, RELEASE, released_by="amin", note="crashed after validate", conn=db)

    assert promotable(db) == [1, 2]
    assert release_record(db, crashed)[:2] == (None, RELEASE)  # no gate decision was made


def test_a_held_newer_version_leaves_the_older_version_promotable(db, make_run, record):
    run_pipeline(db, make_run(), [record(1)])  # passes
    newer = record(1, updated_on="2026-11-25T09:00:00.000")
    failed = make_run()
    with pytest.raises(QualityGateError):
        run_pipeline(db, failed, [newer, record(2, year="1999")])

    versions = db.execute(
        "SELECT payload->>'updated_on' FROM bronze.promotable_records WHERE source_id = '1'"
    ).fetchall()

    # build_silver() takes the newest promotable version: silver keeps the older one.
    assert versions == [("2026-11-24T15:44:55.000",)]


def test_a_run_that_passed_cannot_be_released(db, make_run, record):
    run_id = make_run()
    run_pipeline(db, run_id, [record(1)])

    with pytest.raises(ReleaseError, match="passed the quality gate"):
        release_run(run_id, RELEASE, released_by="amin", note="why not", conn=db)


@pytest.mark.parametrize("second", [RELEASE, RECHECK])
def test_a_run_can_be_released_only_once(db, make_run, record, second):
    failed = failed_run(db, make_run, record)
    release_run(failed, RELEASE, released_by="amin", note="fine", conn=db)

    with pytest.raises(ReleaseError, match="already released by amin"):
        release_run(failed, second, released_by="someone", note="again", conn=db)
    assert release_record(db, failed)[2:4] == ("amin", "fine")


def test_a_run_with_no_held_rows_cannot_be_released_and_nothing_changes(db, make_run, record):
    run_id = make_run()
    with pytest.raises(QualityGateError):
        run_pipeline(db, run_id, [record(1, ward="N/A")])  # its only row is invalid

    with pytest.raises(ReleaseError, match="no held rows"):
        release_run(run_id, RECHECK, released_by="amin", note="nothing", conn=db)
    assert release_record(db, run_id)[1] is None


def test_an_unknown_run_cannot_be_released(db):
    with pytest.raises(ReleaseError, match="no held rows"):
        release_run(
            "00000000-0000-0000-0000-000000000000", RELEASE,
            released_by="amin", note="typo", conn=db,
        )


@pytest.mark.parametrize(
    "action, released_by, note, message",
    [
        ("publish", "amin", "x", "action must be one of"),
        (RELEASE, " ", "x", "released_by and note are required"),
        (RELEASE, "amin", "", "released_by and note are required"),
    ],
)
def test_release_needs_an_action_a_name_and_a_reason(db, make_run, record, action, released_by,
                                                     note, message):
    failed = failed_run(db, make_run, record)

    with pytest.raises(ValueError, match=message):
        release_run(failed, action, released_by=released_by, note=note, conn=db)
    assert promotable(db) == []
