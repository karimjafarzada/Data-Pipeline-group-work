"""Tests for CrimeRecord, the typed shape of a valid record."""

from datetime import datetime

import pytest
from pydantic import ValidationError

from pipeline.ingestion.bronze import CHICAGO
from pipeline.quality.model import CrimeRecord


def test_real_record_becomes_typed_values(crime_record):
    record = CrimeRecord.model_validate(crime_record)
    assert record.id == 14391668
    assert record.occurred_at == datetime(2026, 11, 16, 10, 30, tzinfo=CHICAGO)
    assert record.source_updated_at == datetime(2026, 11, 24, 15, 44, 55, tzinfo=CHICAGO)
    assert record.ward == 18
    assert record.community_area == 66
    assert record.year == 2026
    assert record.latitude == pytest.approx(41.760905907)
    assert record.arrest is False


def test_leading_zeros_are_kept(crime_record):
    record = CrimeRecord.model_validate(crime_record)
    assert record.beat == "0831"
    assert record.district == "008"


def test_fields_match_silver_columns(crime_record):
    # silver.crimes also has bronze_id and run_id, which come from bronze, not the payload.
    assert list(CrimeRecord.model_validate(crime_record).model_dump()) == [
        "id", "case_number", "occurred_at", "block", "iucr", "primary_type", "description",
        "location_description", "arrest", "domestic", "beat", "district", "ward",
        "community_area", "fbi_code", "x_coordinate", "y_coordinate", "year", "latitude",
        "longitude", "source_updated_at",
    ]


def test_location_and_unknown_keys_are_ignored(crime_record):
    crime_record["brand_new_column"] = "x"
    dumped = CrimeRecord.model_validate(crime_record).model_dump()
    assert "location" not in dumped
    assert "brand_new_column" not in dumped


def test_missing_optional_fields_become_none(crime_record):
    for key in ("latitude", "longitude", "x_coordinate", "y_coordinate", "ward", "block"):
        del crime_record[key]
    record = CrimeRecord.model_validate(crime_record)
    assert record.latitude is None
    assert record.ward is None
    assert record.block is None


@pytest.mark.parametrize("value", ["true", 1])
def test_boolean_must_be_a_real_boolean(crime_record, value):
    crime_record["arrest"] = value
    with pytest.raises(ValidationError):
        CrimeRecord.model_validate(crime_record)


@pytest.mark.parametrize("value", ["NaN", "inf"])
def test_coordinate_must_be_finite(crime_record, value):
    crime_record["latitude"] = value
    with pytest.raises(ValidationError):
        CrimeRecord.model_validate(crime_record)


def test_missing_required_field_is_rejected(crime_record):
    del crime_record["case_number"]
    with pytest.raises(ValidationError):
        CrimeRecord.model_validate(crime_record)


def test_unreadable_timestamp_is_rejected(crime_record):
    crime_record["updated_on"] = "yesterday"
    with pytest.raises(ValidationError, match="not a valid timestamp"):
        CrimeRecord.model_validate(crime_record)
