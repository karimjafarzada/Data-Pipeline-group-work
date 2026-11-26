"""Integration tests for the Gold loader and SCD2 location dimension."""
from pipeline.gold.load import load_gold


def insert_silver(db, run_id, *, description="TO VEHICLE", district="008",
                  ward=18, community_area=66, updated="2026-11-24 15:44:55+00"):
    bronze_id = db.execute("""
        INSERT INTO bronze.raw_records
            (endpoint, source_id, payload, payload_hash, run_id)
        VALUES ('ijzp-q8t2', '1001', '{}'::jsonb, %s, %s)
        RETURNING id
    """, (f"gold-fixture-{run_id}", run_id)).fetchone()[0]
    db.execute("""
        INSERT INTO silver.crimes
            (id, case_number, occurred_at, iucr, primary_type, description,
             arrest, domestic, beat, district, ward, community_area, year,
             source_updated_at, bronze_id, run_id)
        VALUES (1001, 'CASE-1', '2026-11-16 10:30:00-06', '1320',
                'CRIMINAL DAMAGE', %s, false, false, '0831', %s, %s, %s, 2026,
                %s::timestamptz, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            description = EXCLUDED.description,
            district = EXCLUDED.district,
            ward = EXCLUDED.ward,
            community_area = EXCLUDED.community_area,
            source_updated_at = EXCLUDED.source_updated_at,
            run_id = EXCLUDED.run_id
    """, (description, district, ward, community_area, updated, bronze_id, run_id))


def test_load_gold_creates_star_schema_rows(db, make_run):
    run_id = make_run()
    insert_silver(db, run_id)

    stats = load_gold(make_run(), conn=db)

    assert stats.source_rows == 1
    assert stats.facts_inserted == 1
    assert db.execute("SELECT count(*) FROM gold.fact_crimes").fetchone()[0] == 1
    assert db.execute("SELECT count(*) FROM gold.dim_date").fetchone()[0] == 1
    assert db.execute("SELECT count(*) FROM gold.dim_crime_type").fetchone()[0] == 1
    assert db.execute("SELECT count(*) FROM gold.dim_location WHERE is_current").fetchone()[0] == 1


def test_load_gold_is_idempotent(db, make_run):
    insert_silver(db, make_run())
    load_gold(make_run(), conn=db)
    before = db.execute("""
        SELECT crime_id, date_key, crime_type_key, location_key, source_updated_at
        FROM gold.fact_crimes
    """).fetchall()

    stats = load_gold(make_run(), conn=db)

    after = db.execute("""
        SELECT crime_id, date_key, crime_type_key, location_key, source_updated_at
        FROM gold.fact_crimes
    """).fetchall()
    assert after == before
    assert stats.facts_inserted == stats.facts_updated == 0
    assert stats.facts_unchanged == 1
    assert db.execute("SELECT count(*) FROM gold.dim_location").fetchone()[0] == 1


def test_location_change_expires_previous_scd2_version(db, make_run):
    first_run = make_run()
    insert_silver(db, first_run)
    load_gold(make_run(), conn=db)
    second_run = make_run()
    insert_silver(db, second_run, ward=19, updated="2026-11-25 09:00:00+00")

    stats = load_gold(make_run(), conn=db)

    versions = db.execute("""
        SELECT ward, is_current
        FROM gold.dim_location ORDER BY location_key
    """).fetchall()
    assert len(versions) == 2
    assert versions[0] == (18, False)
    assert versions[1] == (19, True)
    assert stats.locations_expired == 1
    assert db.execute("SELECT location_key FROM gold.fact_crimes").fetchone()[0] == db.execute(
        "SELECT location_key FROM gold.dim_location WHERE is_current"
    ).fetchone()[0]


def test_newer_silver_version_updates_fact_without_duplicate(db, make_run):
    first_run = make_run()
    insert_silver(db, first_run)
    load_gold(make_run(), conn=db)
    second_run = make_run()
    insert_silver(db, second_run, description="UPDATED", updated="2026-11-25 09:00:00+00")

    stats = load_gold(make_run(), conn=db)

    assert stats.facts_updated == 1
    assert db.execute("SELECT count(*) FROM gold.fact_crimes").fetchone()[0] == 1
    assert db.execute("SELECT case_number FROM gold.fact_crimes").fetchone()[0] == "CASE-1"


def test_empty_silver_is_a_noop(db, make_run):
    stats = load_gold(make_run(), conn=db)
    assert stats.source_rows == 0
    assert stats.facts_inserted == stats.facts_updated == stats.facts_unchanged == 0


def test_gold_loader_preserves_connection_autocommit(db, make_run):
    run_id = make_run()
    db.autocommit = False
    load_gold(run_id, conn=db)
    assert db.autocommit is False


def test_gold_schema_tables_exist(db):
    for table in ("gold.dim_date", "gold.dim_crime_type", "gold.dim_location", "gold.fact_crimes"):
        assert db.execute("SELECT to_regclass(%s) IS NOT NULL", (table,)).fetchone()[0], table
