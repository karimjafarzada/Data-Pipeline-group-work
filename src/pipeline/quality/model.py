"""The shape of a valid crime record, field for field the same as silver.crimes.

The rules in rules.py decide whether a record is valid. This model turns a valid
payload into typed Python values: strings of digits become ints and floats, and
timestamps become Chicago-time datetimes. validate() builds it as a last check, and
build_silver() will use it to make silver rows.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, StrictBool, field_validator

from pipeline.ingestion.bronze import parse_source_timestamp


class CrimeRecord(BaseModel):
    model_config = ConfigDict(
        extra="ignore",  # e.g. `location` (duplicates latitude/longitude) and new API columns
        frozen=True,
        allow_inf_nan=False,  # 'NaN' and 'inf' are not coordinates
    )

    id: int
    case_number: str
    occurred_at: datetime = Field(validation_alias="date")
    block: str | None = None
    iucr: str
    primary_type: str
    description: str | None = None
    location_description: str | None = None
    arrest: StrictBool  # the API sends real JSON booleans; don't accept "true" or 1
    domestic: StrictBool
    beat: str | None = None  # text keeps leading zeros ('0831')
    district: str | None = None  # text keeps leading zeros ('008')
    ward: int | None = None
    community_area: int | None = None
    fbi_code: str | None = None
    x_coordinate: float | None = None
    y_coordinate: float | None = None
    year: int
    latitude: float | None = None
    longitude: float | None = None
    source_updated_at: datetime = Field(validation_alias="updated_on")

    @field_validator("occurred_at", "source_updated_at", mode="before")
    @classmethod
    def _as_chicago_time(cls, value: Any) -> datetime:
        """API timestamps have no timezone; they are Chicago local time."""
        parsed = parse_source_timestamp(value)
        if parsed is None:
            raise ValueError(f"{value!r} is not a valid timestamp")
        return parsed
