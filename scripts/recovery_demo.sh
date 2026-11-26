#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Separate Compose project and disposable databases for this demonstration.
project="recovery-demo-$$"
startup="$project-startup"
worker="$project-worker"
export POSTGRES_PASSWORD
POSTGRES_PASSWORD="$(openssl rand -hex 24)"
export POSTGRES_USER=warehouse POSTGRES_DB=warehouse POSTGRES_HOST_PORT=5434 POSTGRES_PORT=5434
compose=(docker compose --project-name "$project")
script_mount="$PWD/scripts/recovery_demo.py:/tmp/recovery_demo.py:ro"
fixture_mount="$PWD/tests/fixtures/malformed_batch.json:/tmp/malformed_batch.json:ro"

cleanup() {
    docker rm -f "$startup" "$worker" >/dev/null 2>&1 || true
    "${compose[@]}" down --volumes
}
trap cleanup EXIT

wait_for_log() {
    local container="$1" message="$2"
    for ((attempt=0; attempt<30; attempt++)); do
        if docker logs "$container" 2>&1 | grep -F "$message" >/dev/null; then
            docker logs "$container" 2>&1 | grep -F "$message"
            return
        fi
        sleep 1
    done
    docker logs "$container"
    return 1
}

sql() {
    "${compose[@]}" exec -T db psql -U warehouse -d "$1" -v ON_ERROR_STOP=1 -qAtc "$2"
}

"${compose[@]}" build pipeline
"${compose[@]}" run -d --no-deps --name "$startup" pipeline python -m pipeline.migrate
wait_for_log "$startup" "Retrying"
"${compose[@]}" up -d --wait --wait-timeout 60 db
test "$(docker wait "$startup")" = 0
test "$(sql warehouse "SELECT to_regclass('ops.pipeline_runs') IS NOT NULL")" = t
echo "STARTUP RECOVERY: migrations succeeded once PostgreSQL was ready"

"${compose[@]}" run -d --no-deps --name "$worker" -v "$script_mount" pipeline \
    python /tmp/recovery_demo.py pause
wait_for_log "$worker" "CHECKPOINT_COMMITTED: 1"
test "$(sql warehouse "SELECT count(*) FROM bronze.raw_records")" = 1
docker kill "$worker"
test "$(docker wait "$worker")" = 137
"${compose[@]}" run --rm --no-deps -v "$script_mount" pipeline \
    python /tmp/recovery_demo.py resume
test "$(sql warehouse "SELECT string_agg(source_id, ',' ORDER BY source_id) FROM bronze.raw_records")" = 1,2
test "$(sql warehouse "SELECT cursor_value FROM ops.ingestion_checkpoint")" = 2
echo "CHECKPOINT RECOVERY: ids 1 and 2 each exist once; checkpoint is 2"

sql warehouse "CREATE DATABASE malformed_demo"
set +e
"${compose[@]}" run --rm --no-deps -e POSTGRES_DB=malformed_demo \
    -e QUALITY_MAX_FAILURE_RATE=0.05 -v "$script_mount" -v "$fixture_mount" \
    pipeline python /tmp/recovery_demo.py malformed
result=$?
set -e
test "$result" = 2
test "$(sql malformed_demo "SELECT status || ':' || rows_fetched || ':' || rows_valid || ':' || rows_quarantined FROM ops.pipeline_runs")" = failed:20:13:7
test "$(sql malformed_demo "SELECT count(*) FROM bronze.promotable_records")" = 0
test "$(sql malformed_demo "SELECT count(*) FROM ops.quarantine")" = 8
echo "QUALITY GATE: failed:20:13:7; 8 rule violations; 0 promotable rows"
