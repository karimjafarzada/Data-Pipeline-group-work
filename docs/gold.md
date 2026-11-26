# Gold layer

Gold is a dimensional warehouse built exclusively from `silver.crimes`.

## Model

- `gold.fact_crimes`: one current analytical row per Silver crime ID.
- `gold.dim_date`: calendar attributes keyed by `YYYYMMDD`.
- `gold.dim_crime_type`: IUCR business key with current primary type and description (Type 1).
- `gold.dim_location`: SCD Type 2 history for beat/district/ward/community-area attributes.
  Beat is the natural key when present; otherwise district, then ward/community area.

For the first observed snapshot, a location version starts at `-infinity` because Silver is a current-state table and its crime `source_updated_at` values are not the effective start date of a whole beat. Subsequent attribute changes use the newest observed Silver `source_updated_at` as the SCD2 boundary. Each fact joins the location version valid at its own Silver `source_updated_at`. This captures changes observed between
Gold loads; it cannot reconstruct versions that Silver overwrote before Gold first observed them.
Crime type descriptions are Type 1 because the Silver contract contains no effective-dated IUCR
reference catalogue.

## Run

Apply migrations with `python -m pipeline.migrate`, then call `pipeline.gold.load.load_gold(run_id)`
after the quality gate and Silver build. The caller supplies the run UUID created by
`start_run()`. The loader uses POSTGRES_* environment variables when no connection is injected.

## Analytics

See `sql/gold_analytics.sql` for monthly trends, rolling seven-day counts, district arrest rates,
and top crime types per month. These queries use CTEs and window functions.
