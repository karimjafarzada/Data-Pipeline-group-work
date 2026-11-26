"""Tests for ingestion checkpoints."""

from datetime import date

import pytest

from pipeline.ingestion.checkpoint import (
    checkpoint_key,
    format_change_cursor,
    load_checkpoint,
    parse_change_cursor,
    save_checkpoint,
)

# --- keys and cursors (no database) -----------------------------------------


def test_incremental_key():
    assert checkpoint_key("incremental", "ijzp-q8t2") == "incremental:ijzp-q8t2"


def test_backfill_key_includes_the_date_range():
    key = checkpoint_key("backfill", "ijzp-q8t2", date(2024, 1, 1), date(2024, 1, 31))

    assert key == "backfill:ijzp-q8t2:2024-01-01:2024-01-31"


@pytest.mark.parametrize(
    ("mode", "start", "end", "message"),
    [
        ("backfill", date(2024, 1, 1), None, "needs both"),
        ("backfill", None, None, "needs both"),
        ("incremental", date(2024, 1, 1), None, "don't take"),
        ("daily", None, None, "mode must be"),
    ],
)
def test_bad_key_arguments_are_rejected(mode, start, end, message):
    with pytest.raises(ValueError, match=message):
        checkpoint_key(mode, "ijzp-q8t2", start, end)


def test_change_cursor_round_trip():
    cursor = format_change_cursor("2025-01-01T15:40:23.000", "28164")

    assert cursor == "2025-01-01T15:40:23.000|28164"
    assert parse_change_cursor(cursor) == ("2025-01-01T15:40:23.000", 28164)


@pytest.mark.parametrize("bad", ["12345", "|12", "2025-01-01T00:00:00|abc"])
def test_bad_change_cursor_is_rejected(bad):
    with pytest.raises(ValueError):
        parse_change_cursor(bad)


# --- database ---------------------------------------------------------------


def test_load_returns_none_without_a_checkpoint(db):
    assert load_checkpoint(db, "backfill:ijzp-q8t2:2024-01-01:2024-01-31") is None


def test_save_then_load(db, make_run):
    save_checkpoint(db, "incremental:ijzp-q8t2", "2025-01-01T15:40:23.000|28164", make_run())

    assert load_checkpoint(db, "incremental:ijzp-q8t2") == "2025-01-01T15:40:23.000|28164"


def test_saving_again_updates_the_same_row(db, make_run):
    first_run, second_run = make_run(), make_run()
    save_checkpoint(db, "backfill:ijzp-q8t2:2024-01-01:2024-01-31", "100", first_run)
    save_checkpoint(db, "backfill:ijzp-q8t2:2024-01-01:2024-01-31", "250", second_run)

    rows = db.execute(
        "SELECT cursor_value, last_run_id FROM ops.ingestion_checkpoint"
    ).fetchall()

    assert rows == [("250", second_run)]


def test_backfill_and_incremental_checkpoints_are_separate(db, make_run):
    run_id = make_run()
    save_checkpoint(db, "incremental:ijzp-q8t2", "2025-01-01T00:00:00.000|5", run_id)
    save_checkpoint(db, "backfill:ijzp-q8t2:2024-01-01:2024-01-31", "999", run_id)

    assert load_checkpoint(db, "incremental:ijzp-q8t2") == "2025-01-01T00:00:00.000|5"
    assert load_checkpoint(db, "backfill:ijzp-q8t2:2024-01-01:2024-01-31") == "999"
