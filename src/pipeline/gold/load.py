"""Load the Gold star schema from the current Silver crime snapshot.

The loader is idempotent: dates and crime types are upserted by business key,
location attributes create SCD2 versions, and fact rows are updated only when
Silver's source_updated_at advances.
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from uuid import UUID

import psycopg

logger = logging.getLogger(__name__)
class GoldLoadError(RuntimeError):
    """Gold load could not acquire its transaction-scoped lock."""


@dataclass
class GoldStats:
    source_rows: int = 0
    dates: int = 0
    crime_types: int = 0
    locations_inserted: int = 0
    locations_expired: int = 0
    facts_inserted: int = 0
    facts_updated: int = 0
    facts_unchanged: int = 0
    seconds: float = 0.0


def load_gold(
    run_id: UUID,
    *,
    conn: psycopg.Connection | None = None,
) -> GoldStats:
    """Load Silver's current snapshot into Gold in one transaction."""
    owns_conn = conn is None
    if conn is None:
        settings = _connection_settings_from_env()
        conn = psycopg.connect(**settings)
    original_autocommit = conn.autocommit
    started = time.monotonic()
    try:
        conn.autocommit = True
        with conn.transaction():
            stats = _load(conn, run_id)
    finally:
        if owns_conn:
            conn.close()
        else:
            conn.autocommit = original_autocommit
    stats.seconds = round(time.monotonic() - started, 3)
    logger.info("Gold load finished: %s", stats)
    return stats


def _load(conn: psycopg.Connection, run_id: UUID) -> GoldStats:
    if not conn.execute(
        "SELECT pg_try_advisory_xact_lock(hashtext('load_gold'))"
    ).fetchone()[0]:
        raise GoldLoadError("Another load_gold() is already running")

    stats = GoldStats()
    conn.execute("""
        CREATE TEMP TABLE gold_source ON COMMIT DROP AS
        SELECT id, case_number, occurred_at, iucr, primary_type, description,
               arrest, domestic, beat, district, ward, community_area,
               x_coordinate, y_coordinate, latitude, longitude,
               source_updated_at, bronze_id, run_id AS silver_run_id,
               to_char(occurred_at AT TIME ZONE 'America/Chicago', 'YYYYMMDD')::integer AS date_key,
               CASE WHEN beat IS NOT NULL THEN 'beat:' || beat
                    WHEN district IS NOT NULL THEN 'district:' || district
                    ELSE 'area:' || coalesce(ward::text, '') || ':' ||
                         coalesce(community_area::text, '') END AS location_nk,
               md5(concat_ws('|', coalesce(beat, '<NULL>'),
                                  coalesce(district, '<NULL>'),
                                  coalesce(ward::text, '<NULL>'),
                                  coalesce(community_area::text, '<NULL>'))) AS attribute_hash
        FROM silver.crimes
    """)
    conn.execute("CREATE INDEX gold_source_location_idx ON gold_source(location_nk, source_updated_at)")
    conn.execute("ANALYZE gold_source")
    stats.source_rows = conn.execute("SELECT count(*) FROM gold_source").fetchone()[0]
    if not stats.source_rows:
        return stats

    conn.execute("""
        INSERT INTO gold.dim_date
            (date_key, full_date, year, quarter, month, month_name, day,
             day_of_week, day_name, is_weekend)
        SELECT DISTINCT
            date_key,
            (occurred_at AT TIME ZONE 'America/Chicago')::date,
            extract(year FROM occurred_at AT TIME ZONE 'America/Chicago')::smallint,
            extract(quarter FROM occurred_at AT TIME ZONE 'America/Chicago')::smallint,
            extract(month FROM occurred_at AT TIME ZONE 'America/Chicago')::smallint,
            to_char(occurred_at AT TIME ZONE 'America/Chicago', 'FMMonth'),
            extract(day FROM occurred_at AT TIME ZONE 'America/Chicago')::smallint,
            extract(isodow FROM occurred_at AT TIME ZONE 'America/Chicago')::smallint,
            to_char(occurred_at AT TIME ZONE 'America/Chicago', 'FMDay'),
            extract(isodow FROM occurred_at AT TIME ZONE 'America/Chicago') IN (6, 7)
        FROM gold_source
        ON CONFLICT (date_key) DO NOTHING
    """)
    stats.dates = conn.execute("SELECT count(DISTINCT date_key) FROM gold_source").fetchone()[0]

    conn.execute("""
        INSERT INTO gold.dim_crime_type (iucr, primary_type, description)
        SELECT DISTINCT ON (iucr) iucr, primary_type, description
        FROM gold_source
        ORDER BY iucr, source_updated_at DESC, id DESC
        ON CONFLICT (iucr) DO UPDATE SET
            primary_type = EXCLUDED.primary_type,
            description = EXCLUDED.description,
            updated_at = now()
        WHERE (gold.dim_crime_type.primary_type, gold.dim_crime_type.description)
              IS DISTINCT FROM (EXCLUDED.primary_type, EXCLUDED.description)
    """)
    stats.crime_types = conn.execute("SELECT count(DISTINCT iucr) FROM gold_source").fetchone()[0]

    # Collapse multiple crimes in a beat to its latest observed location attributes.
    conn.execute("""
        CREATE TEMP TABLE gold_location_latest ON COMMIT DROP AS
        SELECT DISTINCT ON (location_nk)
               location_nk, beat, district, ward, community_area,
               attribute_hash, source_updated_at AS valid_from
        FROM gold_source
        ORDER BY location_nk, source_updated_at DESC, id DESC
    """)
    conn.execute("CREATE UNIQUE INDEX gold_location_latest_nk ON gold_location_latest(location_nk)")

    # Expire only when the incoming observation is later than the current version.
    expired = conn.execute("""
        UPDATE gold.dim_location d
        SET valid_to = s.valid_from, is_current = false
        FROM gold_location_latest s
        WHERE d.location_nk = s.location_nk AND d.is_current
          AND d.attribute_hash <> s.attribute_hash
          AND s.valid_from > d.valid_from
    """).rowcount
    stats.locations_expired = expired

    inserted = conn.execute("""
        INSERT INTO gold.dim_location
            (location_nk, beat, district, ward, community_area, attribute_hash,
             valid_from, valid_to, is_current)
        SELECT s.location_nk, s.beat, s.district, s.ward, s.community_area,
               s.attribute_hash,
               CASE WHEN EXISTS (
                   SELECT 1 FROM gold.dim_location history
                   WHERE history.location_nk = s.location_nk
               ) THEN s.valid_from ELSE '-infinity'::timestamptz END,
               'infinity', true
        FROM gold_location_latest s
        WHERE NOT EXISTS (
            SELECT 1 FROM gold.dim_location d
            WHERE d.location_nk = s.location_nk AND d.is_current
        )
        ON CONFLICT (location_nk, valid_from) DO NOTHING
    """).rowcount
    stats.locations_inserted = inserted

    conn.execute("""
        CREATE TEMP TABLE gold_fact_stage ON COMMIT DROP AS
        SELECT s.id AS crime_id, s.date_key, t.crime_type_key, l.location_key,
               s.occurred_at, s.case_number, s.arrest, s.domestic,
               s.x_coordinate, s.y_coordinate, s.latitude, s.longitude,
               s.source_updated_at, s.bronze_id, s.silver_run_id
        FROM gold_source s
        JOIN gold.dim_crime_type t ON t.iucr = s.iucr
        JOIN gold.dim_location l
          ON l.location_nk = s.location_nk
         AND s.source_updated_at >= l.valid_from
         AND s.source_updated_at < l.valid_to
    """)
    conn.execute("CREATE UNIQUE INDEX gold_fact_stage_pk ON gold_fact_stage(crime_id)")
    conn.execute("ANALYZE gold_fact_stage")
    staged_facts = conn.execute("SELECT count(*) FROM gold_fact_stage").fetchone()[0]
    before = conn.execute("""
        SELECT count(*) FROM gold_fact_stage s
        JOIN gold.fact_crimes f USING (crime_id)
    """).fetchone()[0]
    inserted_facts = conn.execute("""
        INSERT INTO gold.fact_crimes
            (crime_id, date_key, crime_type_key, location_key, occurred_at,
             case_number, arrest, domestic, x_coordinate, y_coordinate,
             latitude, longitude, source_updated_at, bronze_id, silver_run_id, run_id)
        SELECT crime_id, date_key, crime_type_key, location_key, occurred_at,
               case_number, arrest, domestic, x_coordinate, y_coordinate,
               latitude, longitude, source_updated_at, bronze_id, silver_run_id, %s
        FROM gold_fact_stage
        ON CONFLICT (crime_id) DO UPDATE SET
            date_key = EXCLUDED.date_key,
            crime_type_key = EXCLUDED.crime_type_key,
            location_key = EXCLUDED.location_key,
            occurred_at = EXCLUDED.occurred_at,
            case_number = EXCLUDED.case_number,
            arrest = EXCLUDED.arrest,
            domestic = EXCLUDED.domestic,
            x_coordinate = EXCLUDED.x_coordinate,
            y_coordinate = EXCLUDED.y_coordinate,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            source_updated_at = EXCLUDED.source_updated_at,
            bronze_id = EXCLUDED.bronze_id,
            silver_run_id = EXCLUDED.silver_run_id,
            run_id = EXCLUDED.run_id,
            loaded_at = now()
        WHERE (gold.fact_crimes.source_updated_at, gold.fact_crimes.bronze_id)
              < (EXCLUDED.source_updated_at, EXCLUDED.bronze_id)
    """, (run_id,)).rowcount
    stats.facts_inserted = staged_facts - before
    stats.facts_updated = inserted_facts - stats.facts_inserted
    stats.facts_unchanged = staged_facts - inserted_facts
    return stats


def _connection_settings_from_env() -> dict[str, str]:
    missing = [key for key in ("POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER")
               if not os.environ.get(key)]
    if missing:
        raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")
    settings = {
        "host": os.environ["POSTGRES_HOST"],
        "port": os.environ.get("POSTGRES_PORT", "5432"),
        "dbname": os.environ["POSTGRES_DB"],
        "user": os.environ["POSTGRES_USER"],
    }
    if os.environ.get("POSTGRES_PASSWORD"):
        settings["password"] = os.environ["POSTGRES_PASSWORD"]
    return settings
