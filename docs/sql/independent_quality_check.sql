-- Independent check of validate(): the validation rules re-implemented in plain SQL,
-- run over every checked bronze payload, and compared with what the Python code decided.
--
-- Read-only (one SELECT; nothing is written). Needs PostgreSQL 16+ (pg_input_is_valid).
-- Run:  psql -d chicago_pipeline -f docs/sql/independent_quality_check.sql
--
-- Not covered: missing_source_id (checked at ingestion; such records never reach
-- bronze) and model_mismatch (needs the Pydantic model).
--
-- Conventions, as in pipeline/quality/rules.py:
-- - "present" = the key exists, isn't JSON null, and isn't a blank string;
-- - only missing_required_field cares about missing values;
-- - values are parsed as sent (no trimming), and rules comparing two fields skip
--   values that don't parse.
-- Timestamps are parsed by PostgreSQL here and by Python there, so this is an
-- independent second opinion, not a copy of the same code.
--
-- The two parsers don't accept the same strings. Python's datetime.fromisoformat()
-- takes only ISO 8601; PostgreSQL's ::timestamp also takes e.g. 'yesterday', 'now',
-- 'epoch', 'infinity', '06/09/2024' and 'June 9 2024'. On such values the verdicts can
-- differ: for date = 'yesterday', validate() reports invalid_timestamp and this check
-- accepts it. Agreement here means agreement on the data it was run on, not on every
-- possible input.

WITH parsed AS (
    SELECT
        r.id,
        r.validation_status,
        r.payload AS j,
        CASE WHEN jsonb_typeof(r.payload -> 'date') = 'string'
                  AND pg_input_is_valid(r.payload ->> 'date', 'timestamp')
             THEN (r.payload ->> 'date')::timestamp END                  AS occurred,
        CASE WHEN jsonb_typeof(r.payload -> 'updated_on') = 'string'
                  AND pg_input_is_valid(r.payload ->> 'updated_on', 'timestamp')
             THEN (r.payload ->> 'updated_on')::timestamp END            AS updated,
        CASE WHEN r.payload ->> 'year' ~ '^-?[0-9]+$'
             THEN (r.payload ->> 'year')::numeric END                     AS year_num,
        CASE WHEN r.payload ->> 'latitude' ~ '^-?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][-+]?[0-9]+)?$'
                  AND pg_input_is_valid(r.payload ->> 'latitude', 'double precision')
             THEN (r.payload ->> 'latitude')::double precision END        AS lat,
        CASE WHEN r.payload ->> 'longitude' ~ '^-?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][-+]?[0-9]+)?$'
                  AND pg_input_is_valid(r.payload ->> 'longitude', 'double precision')
             THEN (r.payload ->> 'longitude')::double precision END       AS lon
    FROM bronze.raw_records AS r
    WHERE r.validation_status <> 'pending'
),
rules AS (
    SELECT
        p.id,
        p.validation_status,
        -- missing_required_field: any required field not present
        EXISTS (
            SELECT 1 FROM unnest(ARRAY['case_number', 'date', 'iucr', 'primary_type', 'arrest',
                                       'domestic', 'year', 'updated_on']) AS k
            WHERE NULLIF(btrim(p.j ->> k), '') IS NULL
        ) AS missing_required_field,
        -- invalid_timestamp: present but unparseable, or date in the future (Chicago time)
        (NULLIF(btrim(p.j ->> 'date'), '') IS NOT NULL AND p.occurred IS NULL)
          OR (NULLIF(btrim(p.j ->> 'updated_on'), '') IS NOT NULL AND p.updated IS NULL)
          OR COALESCE(p.occurred > (now() AT TIME ZONE 'America/Chicago'), false)
          AS invalid_timestamp,
        -- invalid_number: present integers that aren't integers, present coordinates
        -- that aren't finite numbers
        EXISTS (
            SELECT 1 FROM unnest(ARRAY['id', 'ward', 'community_area', 'year']) AS k
            WHERE NULLIF(btrim(p.j ->> k), '') IS NOT NULL
              AND p.j ->> k !~ '^-?[0-9]+$'
        ) OR EXISTS (
            SELECT 1 FROM unnest(ARRAY['x_coordinate', 'y_coordinate', 'latitude', 'longitude']) AS k
            WHERE NULLIF(btrim(p.j ->> k), '') IS NOT NULL
              AND NOT (p.j ->> k ~ '^-?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][-+]?[0-9]+)?$'
                       AND pg_input_is_valid(p.j ->> k, 'double precision'))
        ) AS invalid_number,
        -- invalid_boolean: present but not a JSON true/false
        EXISTS (
            SELECT 1 FROM unnest(ARRAY['arrest', 'domestic']) AS k
            WHERE NULLIF(btrim(p.j ->> k), '') IS NOT NULL
              AND jsonb_typeof(p.j -> k) <> 'boolean'
        ) AS invalid_boolean,
        -- year_mismatch (both must parse)
        COALESCE(p.year_num <> extract(year FROM p.occurred), false) AS year_mismatch,
        -- coordinates_incomplete: exactly one of latitude / longitude present
        (NULLIF(btrim(p.j ->> 'latitude'), '') IS NULL)
          <> (NULLIF(btrim(p.j ->> 'longitude'), '') IS NULL) AS coordinates_incomplete,
        -- coordinates_out_of_range (when present and numeric)
        COALESCE(p.lat NOT BETWEEN 41.6 AND 42.1, false)
          OR COALESCE(p.lon NOT BETWEEN -88.0 AND -87.5, false) AS coordinates_out_of_range,
        -- updated_before_date (both must parse)
        COALESCE(p.updated < p.occurred, false) AS updated_before_date
    FROM parsed AS p
),
verdict AS (
    SELECT
        r.*,
        missing_required_field OR invalid_timestamp OR invalid_number OR invalid_boolean
          OR year_mismatch OR coordinates_incomplete OR coordinates_out_of_range
          OR updated_before_date AS sql_invalid
    FROM rules AS r
),
sql_hits AS (
    SELECT rule_name, hits
    FROM (
        SELECT count(*) FILTER (WHERE missing_required_field)   AS missing_required_field,
               count(*) FILTER (WHERE invalid_timestamp)        AS invalid_timestamp,
               count(*) FILTER (WHERE invalid_number)           AS invalid_number,
               count(*) FILTER (WHERE invalid_boolean)          AS invalid_boolean,
               count(*) FILTER (WHERE year_mismatch)            AS year_mismatch,
               count(*) FILTER (WHERE coordinates_incomplete)   AS coordinates_incomplete,
               count(*) FILTER (WHERE coordinates_out_of_range) AS coordinates_out_of_range,
               count(*) FILTER (WHERE updated_before_date)      AS updated_before_date
        FROM verdict
    ) AS totals
    CROSS JOIN LATERAL (VALUES
        ('missing_required_field',   totals.missing_required_field),
        ('invalid_timestamp',        totals.invalid_timestamp),
        ('invalid_number',           totals.invalid_number),
        ('invalid_boolean',          totals.invalid_boolean),
        ('year_mismatch',            totals.year_mismatch),
        ('coordinates_incomplete',   totals.coordinates_incomplete),
        ('coordinates_out_of_range', totals.coordinates_out_of_range),
        ('updated_before_date',      totals.updated_before_date)
    ) AS unpivot(rule_name, hits)
),
python_hits AS (
    SELECT rule_name, count(*) AS hits FROM ops.quarantine GROUP BY rule_name
)
-- Per rule: records SQL flags vs quarantine rows Python wrote. Then, record by record:
-- the SQL verdict vs bronze.validation_status.
SELECT 'rule ' || s.rule_name                          AS check_item,
       s.hits                                         AS sql_count,
       COALESCE(py.hits, 0)                           AS python_count,
       s.hits = COALESCE(py.hits, 0)                  AS agrees
FROM sql_hits AS s LEFT JOIN python_hits AS py USING (rule_name)
UNION ALL
SELECT 'records SQL ' || CASE WHEN sql_invalid THEN 'invalid' ELSE 'valid' END
         || ' / validate() ' || validation_status,
       count(*), count(*),
       sql_invalid = (validation_status = 'invalid')
FROM verdict
GROUP BY sql_invalid, validation_status
ORDER BY 1;
