-- Map the Iceberg-enabled topics as Redpanda SQL tables. Each table returns the live
-- records still in the topic plus the Iceberg-committed history in one result.
CREATE TABLE default_redpanda_catalog=>trips_enriched WITH (
  topic = 'trips_enriched', schema_subject = 'trips_enriched-value');

CREATE TABLE default_redpanda_catalog=>trips_quarantine WITH (
  topic = 'trips_quarantine', schema_subject = 'trips_quarantine-value');

CREATE TABLE default_redpanda_catalog=>weather_hourly WITH (
  topic = 'weather_hourly', schema_subject = 'weather_hourly-value');
