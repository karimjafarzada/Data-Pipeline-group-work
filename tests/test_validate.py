"""Integration tests for validate(): statuses, quarantine rows and re-runs."""

from datetime import datetime

from pipeline.ingestion.bronze import CHICAGO, payload_hash, write_page
from pipeline.ingestion.client import DATASET_ID
from pipeline.quality import rules
from pipeline.quality.validate import MODEL_MISMATCH, check_payload, validate

NOW = datetime(2026, 11, 26, 12, 0, tzinfo=CHICAGO)


def variant(record, record_id, **changes):
    """A copy of `record` with its own id, plus changed (or, with None, removed) fields."""
    copy = {**record, "id": str(record_id)}
    for key, value in changes.items():
        if value is None:
            copy.pop(key, None)
        else:
            copy[key] = value
    return copy


def load_bronze(db, run_id, records):
    write_page(db, run_id, DATASET_ID, records)


def statuses(db):
    rows = db.execute(
        "SELECT source_id, validation_status FROM bronze.raw_records ORDER BY source_id"
    ).fetchall()
    return dict(rows)


def quarantine(db):
    return db.execute(
        "SELECT id, run_id, endpoint, source_id, payload_hash, rule_name, reason"
        " FROM ops.quarantine ORDER BY id"
    ).fetchall()


def test_marks_rows_and_writes_one_quarantine_row_per_broken_rule(db, make_run, crime_record):
    run_id = make_run()
    load_bronze(db, run_id, [
        variant(crime_record, 1),
        variant(crime_record, 2, year="2025"),  # year_mismatch
        variant(crime_record, 3, iucr=None, latitude="0"),  # two rules
    ])

    stats = validate(run_id, conn=db, now=NOW)

    assert statuses(db) == {"1": "valid", "2": "invalid", "3": "invalid"}
    assert sorted((row[3], row[5]) for row in quarantine(db)) == [
        ("2", rules.YEAR_MISMATCH),
        ("3", rules.COORDINATES_OUT_OF_RANGE),
        ("3", rules.MISSING_REQUIRED_FIELD),
    ]
    assert (stats.checked, stats.valid, stats.invalid) == (3, 1, 2)
    assert stats.rule_hits == {
        rules.COORDINATES_OUT_OF_RANGE: 1,
        rules.MISSING_REQUIRED_FIELD: 1,
        rules.YEAR_MISMATCH: 1,
    }


def test_quarantine_row_keeps_the_record_and_the_reason(db, make_run, crime_record):
    run_id = make_run()
    broken = variant(crime_record, 7, arrest="Y")
    load_bronze(db, run_id, [broken])

    validate(run_id, conn=db, now=NOW)

    row = db.execute(
        "SELECT run_id, endpoint, source_id, payload, payload_hash, rule_name, reason"
        " FROM ops.quarantine"
    ).fetchone()
    assert row == (
        run_id, DATASET_ID, "7", broken, payload_hash(broken),
        rules.INVALID_BOOLEAN, "arrest 'Y' is not true or false",
    )


def test_checks_pending_rows_left_by_an_earlier_run(db, make_run, crime_record):
    crashed_run = make_run()
    load_bronze(db, crashed_run, [variant(crime_record, 1), variant(crime_record, 2, ward="x")])
    this_run = make_run()

    stats = validate(this_run, conn=db, now=NOW)

    assert statuses(db) == {"1": "valid", "2": "invalid"}
    assert stats.checked == 2
    # The quarantine row belongs to the run that found the problem.
    assert [row[1] for row in quarantine(db)] == [this_run]


def test_records_which_run_checked_each_row(db, make_run, crime_record):
    fetching_run = make_run()
    load_bronze(db, fetching_run, [variant(crime_record, 1), variant(crime_record, 2, ward="x")])
    checking_run = make_run()

    validate(checking_run, conn=db, now=NOW)

    rows = db.execute(
        "SELECT source_id, run_id, validated_run_id FROM bronze.raw_records ORDER BY source_id"
    ).fetchall()
    # Valid and invalid rows alike: the run that checked them, not the one that fetched them.
    assert rows == [("1", fetching_run, checking_run), ("2", fetching_run, checking_run)]


def test_rows_already_validated_are_not_checked_again(db, make_run, crime_record):
    run_id = make_run()
    load_bronze(db, run_id, [variant(crime_record, 1, year="1999")])
    db.execute(
        "UPDATE bronze.raw_records SET validation_status = 'valid', validated_run_id = run_id"
    )

    stats = validate(run_id, conn=db, now=NOW)

    assert stats.checked == 0
    assert statuses(db) == {"1": "valid"}
    assert quarantine(db) == []


def test_running_twice_gives_the_same_database_state(db, make_run, crime_record):
    run_id = make_run()
    load_bronze(db, run_id, [
        variant(crime_record, 1),
        variant(crime_record, 2, date="2099-01-01T00:00:00.000", year="2099"),
        variant(crime_record, 3, longitude=None),
    ])
    first = validate(run_id, conn=db, now=NOW)
    state_after_first = (statuses(db), quarantine(db))

    second = validate(run_id, conn=db, now=NOW)

    assert (statuses(db), quarantine(db)) == state_after_first
    assert (first.checked, first.valid, first.invalid) == (3, 1, 2)
    assert (second.checked, second.valid, second.invalid, second.rule_hits) == (0, 0, 0, {})


def test_works_across_several_batches(db, make_run, crime_record):
    run_id = make_run()
    load_bronze(db, run_id, [
        variant(crime_record, n, ward="bad" if n % 2 else "18") for n in range(1, 8)
    ])

    stats = validate(run_id, conn=db, now=NOW, batch_size=2)

    assert (stats.checked, stats.valid, stats.invalid) == (7, 3, 4)
    assert set(statuses(db).values()) == {"valid", "invalid"}
    assert len(quarantine(db)) == 4


def test_empty_bronze_returns_zero_counts(db, make_run):
    stats = validate(make_run(), conn=db, now=NOW)
    assert (stats.checked, stats.valid, stats.invalid, stats.rule_hits) == (0, 0, 0, {})


def test_record_passing_rules_but_not_the_model_is_quarantined(db, make_run, crime_record):
    # No rule checks that iucr is a string, but silver needs text: the model catches it.
    run_id = make_run()
    load_bronze(db, run_id, [variant(crime_record, 1, iucr=1320)])

    stats = validate(run_id, conn=db, now=NOW)

    assert statuses(db) == {"1": "invalid"}
    assert stats.rule_hits == {MODEL_MISMATCH: 1}
    assert quarantine(db)[0][6].startswith("iucr: ")


def test_check_payload_of_a_valid_record_is_empty(crime_record):
    assert check_payload(crime_record, NOW) == []


def test_autocommit_setting_is_restored(db, make_run):
    run_id = make_run()
    db.autocommit = False
    validate(run_id, conn=db, now=NOW)
    assert db.autocommit is False
