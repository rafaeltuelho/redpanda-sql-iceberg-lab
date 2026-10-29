#!/usr/bin/env bash
# Create topics, register schemas and map the SQL tables for the Taxi Time Machine.
# Uses the current rpk profile (rpk profile create --from-cloud <cluster-id>).
# Safe to re-run: existing topics/schemas are left as they are.
set -euo pipefail
cd "$(dirname "$0")/.."

RETENTION_MS=${RETENTION_MS:-3600000}        # 1h: older rows can only come from Iceberg
ICEBERG_LAG_MS=${ICEBERG_LAG_MS:-60000}      # rows newer than this can only come from the topic
ICEBERG=(-c redpanda.iceberg.mode=value_schema_id_prefix -c redpanda.iceberg.target.lag.ms="$ICEBERG_LAG_MS")

create_topic() {
  local name=$1; shift
  if rpk topic describe "$name" >/dev/null 2>&1; then
    echo "topic $name exists"
  else
    rpk topic create "$name" -c retention.ms="$RETENTION_MS" "$@"
  fi
}

create_topic trips_raw        -p 6
create_topic trips_enriched   -p 6 "${ICEBERG[@]}"
create_topic trips_quarantine -p 1 "${ICEBERG[@]}"
create_topic weather_hourly   -p 1 "${ICEBERG[@]}"

for pair in trips_raw:trip_raw trips_enriched:trip_enriched \
            trips_quarantine:trip_quarantine weather_hourly:weather_hourly; do
  topic=${pair%%:*}; file=schemas/${pair##*:}.avsc
  rpk registry schema create "$topic-value" --schema "$file" --type avro \
    || echo "schema $topic-value not created (already registered?)"
done

echo "Mapping SQL tables ..."
python scripts/run_sql.py scripts/sql/create_tables.sql
