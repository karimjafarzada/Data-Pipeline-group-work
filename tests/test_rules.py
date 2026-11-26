"""Unit tests for the validation rules: a passing and a failing case for each rule."""

from datetime import datetime

import pytest

from pipeline.ingestion.bronze import CHICAGO
from pipeline.quality import rules
from pipeline.quality.rules import Violation, check_record

# A fixed "now", so the future-date tests don't depend on the real clock.
NOW = datetime(2026, 11, 26, 12, 0, tzinfo=CHICAGO)


def rule_names(record):
    return [violation.rule_name for violation in check_record(record, NOW)]


# --- whole record -------------------------------------------------------------


def test_real_record_passes_every_rule(crime_record):
    assert check_record(crime_record, NOW) == []


def test_record_without_coordinates_passes(crime_record):
    # 3 of the 20 sampled records had no x/y, latitude, longitude or location at all.
    for key in ("x_coordinate", "y_coordinate", "latitude", "longitude", "location"):
        del crime_record[key]
    assert check_record(crime_record, NOW) == []


def test_missing_optional_fields_pass(crime_record):
    # The API leaves out null fields; only the required ones must be there.
    for key in ("block", "description", "location_description", "beat", "district",
                "ward", "community_area", "fbi_code"):
        del crime_record[key]
    assert check_record(crime_record, NOW) == []


def test_record_breaking_three_rules_gets_three_violations(crime_record):
    del crime_record["iucr"]  # missing_required_field
    crime_record["ward"] = "abc"  # invalid_number
    crime_record["latitude"] = "40.0"  # coordinates_out_of_range
    assert rule_names(crime_record) == [
        rules.MISSING_REQUIRED_FIELD,
        rules.INVALID_NUMBER,
        rules.COORDINATES_OUT_OF_RANGE,
    ]


def test_several_problems_for_one_rule_give_one_violation(crime_record):
    crime_record["ward"] = "abc"
    crime_record["community_area"] = "x"
    violations = check_record(crime_record, NOW)
    assert len(violations) == 1
    assert "ward 'abc'" in violations[0].reason
    assert "community_area 'x'" in violations[0].reason


def test_unreadable_date_is_reported_once(crime_record):
    # Without a readable date, year_mismatch and updated_before_date can't be checked.
    crime_record["date"] = "not a date"
    assert rule_names(crime_record) == [rules.INVALID_TIMESTAMP]


# --- missing_required_field -----------------------------------------------------


@pytest.mark.parametrize("field", rules.REQUIRED_FIELDS)
def test_missing_required_field_fails(crime_record, field):
    del crime_record[field]
    assert rules.check_required_fields(crime_record) == Violation(
        rules.MISSING_REQUIRED_FIELD, f"missing required field(s): {field}"
    )


def test_blank_required_field_counts_as_missing(crime_record):
    crime_record["case_number"] = "  "
    assert rules.check_required_fields(crime_record).rule_name == rules.MISSING_REQUIRED_FIELD


def test_missing_required_fields_are_listed_together(crime_record):
    del crime_record["iucr"]
    del crime_record["year"]
    assert rules.check_required_fields(crime_record).reason == (
        "missing required field(s): iucr, year"
    )


def test_required_fields_present_passes(crime_record):
    assert rules.check_required_fields(crime_record) is None


# --- invalid_timestamp ----------------------------------------------------------


@pytest.mark.parametrize("field", ["date", "updated_on"])
def test_unparseable_timestamp_fails(crime_record, field):
    crime_record[field] = "2026-13-45T99:00:00.000"
    violation = rules.check_timestamps(crime_record, NOW)
    assert violation.rule_name == rules.INVALID_TIMESTAMP
    assert field in violation.reason


def test_non_string_timestamp_fails(crime_record):
    crime_record["date"] = 1700000000
    assert rules.check_timestamps(crime_record, NOW).rule_name == rules.INVALID_TIMESTAMP


def test_future_date_fails(crime_record):
    crime_record["date"] = "2026-11-26T12:00:01.000"  # one second after NOW
    violation = rules.check_timestamps(crime_record, NOW)
    assert violation == Violation(
        rules.INVALID_TIMESTAMP, "date '2026-11-26T12:00:01.000' is in the future"
    )


def test_date_equal_to_now_passes(crime_record):
    crime_record["date"] = "2026-11-26T12:00:00.000"
    assert rules.check_timestamps(crime_record, NOW) is None


def test_valid_timestamps_pass(crime_record):
    assert rules.check_timestamps(crime_record, NOW) is None


# --- invalid_number -------------------------------------------------------------


@pytest.mark.parametrize("field", rules.INTEGER_FIELDS)
@pytest.mark.parametrize("value", ["abc", "18.5", "1e3", True])
def test_non_integer_fails(crime_record, field, value):
    crime_record[field] = value
    violation = rules.check_numbers(crime_record)
    assert violation.rule_name == rules.INVALID_NUMBER
    assert f"{field} {value!r} is not an integer" in violation.reason


@pytest.mark.parametrize("field", rules.COORDINATE_FIELDS)
@pytest.mark.parametrize("value", ["abc", "NaN", "inf", "1e999", "1_000"])
def test_non_numeric_coordinate_fails(crime_record, field, value):
    crime_record[field] = value
    assert rules.check_numbers(crime_record).rule_name == rules.INVALID_NUMBER


def test_blank_coordinate_counts_as_missing(crime_record):
    crime_record["x_coordinate"] = ""
    assert rules.check_numbers(crime_record) is None


@pytest.mark.parametrize("value", ["-87.69", "1159193", ".5", "4.1e1"])
def test_numeric_coordinate_strings_pass(crime_record, value):
    crime_record["x_coordinate"] = value
    assert rules.check_numbers(crime_record) is None


def test_numbers_as_strings_pass(crime_record):
    assert rules.check_numbers(crime_record) is None


# --- invalid_boolean ------------------------------------------------------------


@pytest.mark.parametrize("field", rules.BOOLEAN_FIELDS)
@pytest.mark.parametrize("value", ["true", "Y", 1, 0])
def test_non_boolean_fails(crime_record, field, value):
    crime_record[field] = value
    violation = rules.check_booleans(crime_record)
    assert violation == Violation(rules.INVALID_BOOLEAN, f"{field} {value!r} is not true or false")


@pytest.mark.parametrize("value", [True, False])
def test_real_booleans_pass(crime_record, value):
    crime_record["arrest"] = value
    crime_record["domestic"] = value
    assert rules.check_booleans(crime_record) is None


# --- year_mismatch --------------------------------------------------------------


def test_year_different_from_date_fails(crime_record):
    crime_record["year"] = "2025"
    assert rules.check_year_matches_date(crime_record) == Violation(
        rules.YEAR_MISMATCH, "year 2025 but date '2026-11-16T10:30:00.000' is in 2026"
    )


def test_year_matching_date_passes(crime_record):
    assert rules.check_year_matches_date(crime_record) is None


def test_year_mismatch_skipped_when_year_is_unreadable(crime_record):
    crime_record["year"] = "twenty"  # reported by invalid_number instead
    assert rules.check_year_matches_date(crime_record) is None


# --- coordinates_incomplete -----------------------------------------------------


@pytest.mark.parametrize(
    ("removed", "reason"),
    [
        ("longitude", "latitude present but longitude missing"),
        ("latitude", "longitude present but latitude missing"),
    ],
)
def test_one_coordinate_missing_fails(crime_record, removed, reason):
    del crime_record[removed]
    assert rules.check_coordinates_complete(crime_record) == Violation(
        rules.COORDINATES_INCOMPLETE, reason
    )


def test_both_coordinates_present_passes(crime_record):
    assert rules.check_coordinates_complete(crime_record) is None


def test_both_coordinates_missing_passes(crime_record):
    del crime_record["latitude"]
    del crime_record["longitude"]
    assert rules.check_coordinates_complete(crime_record) is None


# --- coordinates_out_of_range ---------------------------------------------------


@pytest.mark.parametrize(
    ("field", "value"),
    [("latitude", "41.59"), ("latitude", "42.11"), ("longitude", "-88.01"),
     ("longitude", "-87.49"), ("latitude", "0")],
)
def test_coordinate_outside_chicago_fails(crime_record, field, value):
    crime_record[field] = value
    violation = rules.check_coordinates_in_range(crime_record)
    assert violation.rule_name == rules.COORDINATES_OUT_OF_RANGE
    assert field in violation.reason


@pytest.mark.parametrize(("latitude", "longitude"), [("41.6", "-88.0"), ("42.1", "-87.5")])
def test_coordinates_on_the_edge_pass(crime_record, latitude, longitude):
    crime_record["latitude"] = latitude
    crime_record["longitude"] = longitude
    assert rules.check_coordinates_in_range(crime_record) is None


def test_coordinates_inside_chicago_pass(crime_record):
    assert rules.check_coordinates_in_range(crime_record) is None


# --- updated_before_date --------------------------------------------------------


def test_updated_before_date_fails(crime_record):
    crime_record["updated_on"] = "2026-11-16T10:29:59.000"
    assert rules.check_updated_after_date(crime_record) == Violation(
        rules.UPDATED_BEFORE_DATE,
        "updated_on '2026-11-16T10:29:59.000' is before date '2026-11-16T10:30:00.000'",
    )


def test_updated_at_same_moment_as_date_passes(crime_record):
    crime_record["updated_on"] = crime_record["date"]
    assert rules.check_updated_after_date(crime_record) is None


def test_updated_after_date_passes(crime_record):
    assert rules.check_updated_after_date(crime_record) is None
