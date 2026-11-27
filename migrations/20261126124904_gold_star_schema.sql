-- Gold warehouse star schema. Source of truth: silver.crimes.
-- Location attributes ward/community_area are tracked as SCD Type 2.
-- Crime type is Type 1 because IUCR is the stable business key and the current
-- Silver contract does not provide a separate effective-dated code catalogue.

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE gold.dim_date (
    date_key       INTEGER PRIMARY KEY, -- YYYYMMDD
    full_date      DATE NOT NULL UNIQUE,
    year           SMALLINT NOT NULL,
    quarter        SMALLINT NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month          SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),
    month_name     TEXT NOT NULL,
    day            SMALLINT NOT NULL CHECK (day BETWEEN 1 AND 31),
    day_of_week    SMALLINT NOT NULL CHECK (day_of_week BETWEEN 1 AND 7), -- ISO: Mon=1
    day_name       TEXT NOT NULL,
    is_weekend     BOOLEAN NOT NULL
);

CREATE TABLE gold.dim_crime_type (
    crime_type_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    iucr           TEXT NOT NULL UNIQUE,
    primary_type   TEXT NOT NULL,
    description    TEXT,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE gold.dim_location (
    location_key   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    location_nk    TEXT NOT NULL,
    beat           TEXT,
    district       TEXT,
    ward           INTEGER,
    community_area INTEGER,
    attribute_hash TEXT NOT NULL,
    valid_from     TIMESTAMPTZ NOT NULL,
    valid_to       TIMESTAMPTZ NOT NULL DEFAULT 'infinity',
    is_current     BOOLEAN NOT NULL DEFAULT TRUE,
    CHECK (valid_from < valid_to)
);

-- One current row per natural key; older versions remain queryable.
CREATE UNIQUE INDEX dim_location_one_current_idx
    ON gold.dim_location (location_nk) WHERE is_current;
CREATE UNIQUE INDEX dim_location_version_idx
    ON gold.dim_location (location_nk, valid_from);
CREATE INDEX dim_location_validity_idx
    ON gold.dim_location (location_nk, valid_from, valid_to);

CREATE TABLE gold.fact_crimes (
    crime_id         BIGINT PRIMARY KEY, -- one current analytical row per Silver crime
    date_key         INTEGER NOT NULL REFERENCES gold.dim_date (date_key),
    crime_type_key   BIGINT NOT NULL REFERENCES gold.dim_crime_type (crime_type_key),
    location_key     BIGINT NOT NULL REFERENCES gold.dim_location (location_key),
    occurred_at      TIMESTAMPTZ NOT NULL,
    case_number      TEXT NOT NULL,
    arrest           BOOLEAN NOT NULL,
    domestic         BOOLEAN NOT NULL,
    x_coordinate     DOUBLE PRECISION,
    y_coordinate     DOUBLE PRECISION,
    latitude         DOUBLE PRECISION,
    longitude        DOUBLE PRECISION,
    source_updated_at TIMESTAMPTZ NOT NULL,
    bronze_id        BIGINT NOT NULL REFERENCES bronze.raw_records (id),
    silver_run_id    UUID NOT NULL REFERENCES ops.pipeline_runs (run_id),
    run_id           UUID NOT NULL REFERENCES ops.pipeline_runs (run_id), -- Gold load run
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX fact_crimes_date_idx ON gold.fact_crimes (date_key);
CREATE INDEX fact_crimes_type_date_idx ON gold.fact_crimes (crime_type_key, date_key);
CREATE INDEX fact_crimes_location_date_idx ON gold.fact_crimes (location_key, date_key);
CREATE INDEX fact_crimes_run_idx ON gold.fact_crimes (silver_run_id);
