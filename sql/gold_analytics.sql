-- Starter analytical queries for the Gold star schema.
-- 1. Monthly crime volume and month-over-month change.
WITH monthly AS (
    SELECT d.year, d.month, d.month_name, count(*) AS crime_count
    FROM gold.fact_crimes f
    JOIN gold.dim_date d USING (date_key)
    GROUP BY d.year, d.month, d.month_name
),
with_previous AS (
    SELECT *,
           lag(crime_count) OVER (ORDER BY year, month) AS previous_month_count
    FROM monthly
)
SELECT year, month, month_name, crime_count, previous_month_count,
       crime_count - previous_month_count AS month_over_month_delta
FROM with_previous
ORDER BY year, month;

-- 2. Rolling seven-calendar-day crime count, including days with zero crimes.
WITH bounds AS (
    SELECT min(full_date) AS first_day, max(full_date) AS last_day
    FROM gold.dim_date
),
calendar AS (
    SELECT day::date AS full_date
    FROM bounds,
         generate_series(first_day::timestamp, last_day::timestamp, interval '1 day') AS days(day)
),
daily AS (
    SELECT c.full_date, count(f.crime_id) AS crime_count
    FROM calendar c
    LEFT JOIN gold.dim_date d ON d.full_date = c.full_date
    LEFT JOIN gold.fact_crimes f ON f.date_key = d.date_key
    GROUP BY c.full_date
)
SELECT full_date, crime_count,
       sum(crime_count) OVER (
           ORDER BY full_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
       ) AS rolling_7_day_count
FROM daily
ORDER BY full_date;

-- 3. District ranking by crime volume and arrest rate.
WITH district_stats AS (
    SELECT l.district, count(*) AS crime_count,
           count(*) FILTER (WHERE f.arrest) AS arrests
    FROM gold.fact_crimes f
    JOIN gold.dim_location l USING (location_key)
    GROUP BY l.district
)
SELECT district, crime_count, arrests,
       round(100.0 * arrests / nullif(crime_count, 0), 2) AS arrest_pct,
       dense_rank() OVER (ORDER BY crime_count DESC) AS volume_rank
FROM district_stats
ORDER BY volume_rank, district;

-- 4. Crime types contributing most to each month (window ranking).
WITH type_month AS (
    SELECT d.year, d.month, t.primary_type, count(*) AS crime_count
    FROM gold.fact_crimes f
    JOIN gold.dim_date d USING (date_key)
    JOIN gold.dim_crime_type t USING (crime_type_key)
    GROUP BY d.year, d.month, t.primary_type
),
ranked AS (
    SELECT *, row_number() OVER (
        PARTITION BY year, month ORDER BY crime_count DESC, primary_type
    ) AS type_rank
    FROM type_month
)
SELECT year, month, primary_type, crime_count
FROM ranked
WHERE type_rank <= 5
ORDER BY year, month, type_rank;
