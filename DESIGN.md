# NYC Taxi Time Machine: design

A demo web app for **Redpanda Iceberg topics + Redpanda SQL + Redpanda Connect** on a
Redpanda BYOC cluster (AWS). One map replays NYC taxi trips. One SQL table backs both the
historical playback (Iceberg) and the live view (records still in the topic).

## 1. Goals

- Show that **a single SQL table** returns live topic records and Iceberg-committed
  history together, with no `UNION ALL`, no ETL and no duplicate rows at the boundary.
- Show **Redpanda Connect** doing real pipeline work: generating, enriching, routing and
  loading data.
- Make the demo **visual and controllable** (playback, live mode, a surge button) so it
  holds up in front of an audience.
- Keep it small: one app container, three Connect pipelines and two public data sources.

Out of scope: auth on the web app, multi-tenant use, production hardening.

## 2. Architecture

```
                         ┌───────────────── Redpanda Connect (managed pipelines, BYOC data plane) ─────────────────┐
 Redpanda SQL ──sql_raw─▶│ ① replay-shift  every 5s: read trips from the same weekday/time-of-day in 2022-23,     │
 (trips_enriched,        │                 restamp to "now" (origin=replay)                                       │──┐
  origin=history)        │                                                                                         │  │
                         │ ③ weather-loader  Open-Meteo archive (once) + last full hour (hourly)                   │──┼─▶ weather_hourly (Iceberg)
                         └─────────────────────────────────────────────────────────────────────────────────────────┘  │
 produce.py (parquet backfill, origin=history) ───────────────────────────────────────────────────────────────────────┤
 FastAPI POST /api/surge (origin=surge) ──────────────────────────────────────────────────────────────────────────────┤
                                                                                                                       ▼
                                                                                                           trips_raw (Avro, 1h retention)
                                                                                                                       │
                         ┌─────────────────────────────────────────────────────────────────────────────────────────┐   │
                         │ ② trips-enrich  decode → zone/borough, duration, mph, tip %, airport, payment label     │◀──┘
                         │                 switch → valid rows / quarantined rows                                  │──▶ trips_enriched (Iceberg)
                         └─────────────────────────────────────────────────────────────────────────────────────────┘──▶ trips_quarantine (Iceberg)

 Browser (React + deck.gl) ◀──HTTP/SSE── FastAPI ◀──pg wire── Redpanda SQL
                                                               trips_enriched ⋈ weather_hourly   (live + Iceberg in one query)
```

The existing `trips` topic from the first lab stays as it is. It isn't used by the app,
so its Glue table (kept because `iceberg_delete=false`) is left alone.

## 3. Key decisions

| # | Decision | Why | Alternatives considered |
|---|---|---|---|
| D1 | Python 3.13 + **FastAPI** backend | Matches the existing scripts (`prep.py`, `produce.py`). | Node/TS, Go |
| D2 | **Vite + React + TypeScript + deck.gl + MapLibre** frontend, built to static files and served by FastAPI | One container. deck.gl handles thousands of animated arcs. MapLibre with Carto tiles needs no token. | Plain JS page, Mapbox |
| D3 | **Replay-shift** live data, built as a Connect pipeline (`generate` → `sql_raw` → restamp) | The live stream follows real NYC patterns (airports, rush hours). It needs no custom producer and uses SQL from inside Connect. | Python producer, pure random generator |
| D4 | Every producer writes to **`trips_raw`**. Connect enriches it into **`trips_enriched`** | History and live data share one schema and one enrichment path. | Enrich only live data and join zones in SQL |
| D5 | History is **reloaded** through `trips_raw` into the new `trips_enriched` topic | Keeps one schema. Avoids dropping `trips`, which would leave an orphaned Glue table. | Evolve the `trips` schema in place |
| D6 | **Open-Meteo** hourly weather in its own Iceberg topic, joined in SQL | Free, no key, about 2,200 rows. Gives the "weather vs. demand" story and shows SQL `JOIN` across two Iceberg topics. | Enrich each trip with weather in Connect (many HTTP lookups, and duplicated data) |
| D7 | TLC **taxi zone lookup** (265 zones) inlined into the enrich pipeline as a Bloblang map. GeoJSON is served to the browser | Cloud pipelines have no file system. The data is static and small. | A compacted `zones` topic joined in SQL |
| D8 | **Quarantine routing** in Connect (`switch` on data-quality rules) | The TLC data really has bad rows. Gives a "data quality" counter with no extra system. | Filtering in SQL |
| D9 | **Time convention: NYC wall clock stored as UTC** (§5) | TLC timestamps have no time zone, and `prep.py` already stores them as if they were UTC. Keeping this rule for live data and weather means joins and "same time of day" just work. | Converting everything to real UTC (would mean reloading and shifting every query) |
| D10 | **Shared live poller** in the backend (one SQL query every 2 s, fanned out over SSE) | SQL load stays constant however many browser tabs are open. | One query per client |
| D11 | Deploy on **EC2 + Docker Compose** or **ECS Fargate (CDK, Python)** from the same image | EC2 is quickest for a demo. Fargate is managed. Both run in the VPC peered with the cluster. | EKS |
| D12 | **Serverless** only where it fits: Connect pipelines (already managed). The API isn't on Lambda | Lambda inside a VPC adds cold starts and a new SQL connection per call, and can't hold the live SSE stream. See §10. | Lambda + API Gateway, with polling |
| D13 | **SCRAM password** auth to Redpanda SQL for the backend and Connect `sql_raw`; OIDC client credentials optional for the backend | Works with every Postgres driver (psycopg, Go `postgres`/`pgx`) with no token refresh. | Bearer token (expires), client credentials everywhere |

## 4. Data sources

| Source | What | Size | Used for |
|---|---|---|---|
| NYC TLC Yellow Taxi, Dec 2022 to Feb 2023 (existing `prep.py`) | Trips | ~9M rows | History and the replay source |
| TLC `taxi_zone_lookup.csv` + `taxi_zones.zip` (shapefile, EPSG:2263) | 265 zones: name, borough, polygons | <1 MB after converting to WGS84 GeoJSON and rounding coordinates | Map polygons, arc end points (zone centroids), enrichment |
| Open-Meteo **archive** API (`archive-api.open-meteo.com/v1/archive`) | Hourly temperature, precipitation, rain, snow, wind, WMO weather code for NYC, Dec 2022 to Feb 2023 | ~2,160 rows | Weather strip, rain vs. dry |
| Open-Meteo **forecast** API (`api.open-meteo.com/v1/forecast`) | The same fields for the last full hour | 1 row per hour | Live weather |

Both Open-Meteo calls use `latitude=40.7128&longitude=-74.0060&timezone=America/New_York`.

## 5. Time convention (D9)

The TLC `tpep_*_datetime` values are New York wall-clock times with no zone. `prep.py`
stores them with `epoch_us()`, which treats them as UTC. Redpanda SQL shows them as
`timestamp with time zone`, so `2023-01-01 00:05:00+00` means **12:05 AM in New York**.

The same rule applies everywhere:

- **Replay / surge:** "now" = current New York wall-clock time, labeled as UTC.
- **Weather:** Open-Meteo is called with `timezone=America/New_York`. Its local hour
  strings are parsed as UTC.
- **Backend:** `ny_now()` returns New York wall-clock time as a UTC `datetime`. It's used
  for live windows and freshness.
- **UI:** shows the stored value as is ("NYC time"). It never converts to the browser's
  time zone.

The history window (Dec to Feb) has no DST change. Live mode follows the current New York
offset automatically.

`redpanda.timestamp` (record time) is **not** event time: for history it's the load time.
The app always uses `pickup_datetime` as event time.

## 6. Topics and schemas

All values are Avro with the Schema Registry wire format. Iceberg topics use
`redpanda.iceberg.mode=value_schema_id_prefix`.

| Topic | Partitions | Retention | Iceberg | Schema (subject) |
|---|---|---|---|---|
| `trips_raw` | 6 | 1 h | no | `schemas/trip_raw.avsc` (`trips_raw-value`) |
| `trips_enriched` | 6 | 1 h | yes, target lag 60 s | `schemas/trip_enriched.avsc` (`trips_enriched-value`) |
| `trips_quarantine` | 1 | 1 h | yes | `schemas/trip_quarantine.avsc` (`trips_quarantine-value`) |
| `weather_hourly` | 1 | 1 h | yes | `schemas/weather_hourly.avsc` (`weather_hourly-value`) |

Short retention is on purpose: anything older than about an hour **can only** come from
Iceberg, and anything newer than about 60 s **can only** come from the topic. A single
query that returns both proves the union.

**`trip_raw`** has the 10 existing fields plus `origin` (string: `history | replay | surge`,
default `history`).

**`trip_enriched`** is `trip_raw` plus:

| Field | Type | Rule |
|---|---|---|
| `pickup_zone`, `pickup_borough`, `dropoff_zone`, `dropoff_borough` | string | zone lookup. Unknown IDs (264/265, 0) → `"Unknown"` |
| `duration_min` | double | (dropoff − pickup) / 60 s |
| `speed_mph` | double, nullable | `trip_distance / (duration_min/60)`. Null when duration ≤ 0 |
| `tip_pct` | double, nullable | `tip_amount / fare_amount`. Null when fare ≤ 0 |
| `is_airport` | boolean | pickup or dropoff in zone 1 (EWR), 132 (JFK) or 138 (LGA) |
| `payment_label` | string | 1 credit card, 2 cash, 3 no charge, 4 dispute, 5 unknown, 6 voided, other "other" |

**`trip_quarantine`** is `trip_enriched` plus `reason` (string, the first rule that fails).

Quarantine rules, in order: `duration_min <= 0`, `duration_min > 360`,
`speed_mph > 100`, `trip_distance > 200`, `fare_amount < 0`, `total_amount > 1000`.

**`weather_hourly`**: `obs_hour` (timestamp-micros), `temp_c`, `precip_mm`, `rain_mm`,
`snow_cm`, `wind_kmh` (double, nullable), `weather_code` (int, nullable), `kind`
(string: `archive | live`).

Timestamps are Avro `long` / `timestamp-micros`. Redpanda SQL shows them as
`timestamp with time zone`, which was confirmed against the existing `trips` table.

SQL tables (`scripts/sql/create_tables.sql`):

```sql
CREATE TABLE default_redpanda_catalog=>trips_enriched   WITH (topic = 'trips_enriched',   schema_subject = 'trips_enriched-value');
CREATE TABLE default_redpanda_catalog=>trips_quarantine WITH (topic = 'trips_quarantine', schema_subject = 'trips_quarantine-value');
CREATE TABLE default_redpanda_catalog=>weather_hourly   WITH (topic = 'weather_hourly',   schema_subject = 'weather_hourly-value');
```

## 7. Redpanda Connect pipelines (`connect/`)

All three run as managed pipelines in the BYOC data plane. They use the contextual
variables `${REDPANDA_BROKERS}` and `${REDPANDA_SCHEMA_REGISTRY_URL}` and the Cloud
secrets `${secrets.RP_USER}`, `${secrets.RP_PASS}` and `${secrets.RP_SQL_DSN}`.
`connect/render.py` produces the deployable YAML. It inlines the zone map, and with
`--local` it rewrites `${secrets.X}` → `${X}` so a pipeline can also run self-hosted with
`rpk connect run`.

### ① `replay-shift`

1. `generate` (interval 5 s) computes the windows in Bloblang:
   - `now_wall` = New York wall-clock time labeled as UTC, floored to 5 s.
   - `CYCLE` = 84 days (12 weeks) and `ANCHOR` = Monday 2022-12-05 00:00. Then
     `hist = ANCHOR + ((now_wall − ANCHOR) mod CYCLE)`, which keeps **weekday and time
     of day**.
   - Emits `REPLAY_WEEKS` windows (default 4) at `hist + k·7d mod CYCLE`, so the volume
     looks like a busy day. Each window is `[h − 5s, h)` with
     `shift = now_wall − h`.
   - Redpanda SQL's `extract()` has no day-of-week field, so this math lives in Connect,
     not in SQL.
2. `unarchive` (json_array) → one message per window.
3. `sql_raw` (`postgres` driver, `${secrets.RP_SQL_DSN}`):
   `SELECT … FROM default_redpanda_catalog=>trips_enriched WHERE origin = 'history' AND pickup_datetime >= $1::timestamptz AND pickup_datetime < $2::timestamptz`.
   The sorted Iceberg files mean each query only reads a narrow slice.
4. `unarchive` → one message per trip. Bloblang adds `shift` to both timestamps and sets
   `origin = "replay"`.
5. `schema_registry_encode` (`trips_raw-value`) → `redpanda` output `trips_raw`.

### ② `trips-enrich`

1. `redpanda` input from `trips_raw`, consumer group `trips-enrich`.
2. `schema_registry_decode`.
3. Bloblang: zone lookup (inlined map), derived fields, quarantine rules.
   Sets `@route` to `trips_enriched` or `trips_quarantine`.
4. `schema_registry_encode` with `subject: ${! @route }-value`.
5. `redpanda` output with `topic: ${! @route }`.

Surge and replay trips go through the same path as history.

### ③ `weather-loader`

- `broker` input with two `generate` inputs:
  - `count: 1`: one call to the archive API for 2022-12-01 to 2023-02-28 (`kind=archive`).
  - `interval: "0 5 * * * *"` (cron, at :05 past each hour): forecast API with
    `past_days=1`. Keeps only the hour that just ended (`kind=live`), so the table has one
    row per hour.
- `http` processor → Bloblang turns `hourly.time[i]` and the matching value arrays into
  one record per hour → `unarchive` → `schema_registry_encode` (`weather_hourly-value`)
  → `redpanda` output `weather_hourly`, key `obs_hour`.

### Deployment of pipelines

First runs go through the Cloud UI (Connect → Create pipeline, paste the rendered YAML,
attach secrets). `scripts/deploy_pipelines.sh` does the same through the Data Plane API
(`/v1/redpanda-connect/pipelines`) for repeat deploys. Terraform `redpanda_pipeline` is an
alternative if the cluster is already managed with Terraform.

## 8. Backend (FastAPI, `app/`)

Connection: psycopg 3 async pool to `host=<sql-host> port=5432 dbname=oxla sslmode=require`,
using SCRAM user/password, or `options=auth_method=client_secret` with a service account's
client ID and secret. Server-side prepared statements are turned off
(`prepare_threshold=None`) for compatibility. Parameters are still bound, never formatted
into the SQL string.

Every query endpoint returns `{ data, sql, ms, rows }`. The SQL panel in the UI shows them.

| Endpoint | Purpose | Query outline |
|---|---|---|
| `GET /api/meta` | History bounds, server "NYC now", config | `min/max(pickup_datetime) WHERE origin='history'` (cached) |
| `GET /api/volume?from&to&bucket_s` | Timeline sparkline | count per bucket |
| `GET /api/frames?from&to&bucket_s` | Map choropleth frames | count, avg fare per (bucket, pickup zone) |
| `GET /api/arcs?from&to&limit` | Trips to animate | pickup/dropoff time, zones, fare, origin, `LIMIT` |
| `GET /api/weather?from&to` | Weather strip | `weather_hourly` rows in range |
| `GET /api/insights/rain` | Rain vs. dry card | `trips_enriched ⋈ weather_hourly` on hour |
| `GET /api/insights/then-vs-now` | Last hour vs. the same hour in 2023 | one query, `OR` on two ranges |
| `GET /api/insights/quality` | Quarantine counter | count by `reason` |
| `GET /api/live` (SSE) | Live mode | shared poller every 2 s: trips with `pickup_datetime > ny_now() − 20s`, deduplicated by `(redpanda).partition, (redpanda).offset`, plus 15-minute zone counts and freshness |
| `POST /api/surge {zone_id, count}` | Surge button | produces `count` trips from `zone_id` to random zones into `trips_raw` (`origin=surge`) |
| `GET /*` | Static frontend | `web/dist` |

Buckets of any size use `timestamp_micros(unix_micros(ts) / $b * $b)`, because
`date_trunc` only truncates to whole units. Every query has a `pickup_datetime` range
predicate so Iceberg can skip files using their min/max stats.

**Freshness** = `ny_now() − max(pickup_datetime)` over the last minute. Below the Iceberg
target lag (60 s), the UI labels those rows "still in the topic, not yet in Iceberg".

## 9. Frontend (`web/`)

- **Map:** `GeoJsonLayer` of zones, colored by pickups in the rolling window
  (sequential scale). `ArcLayer` for trips: history in teal, replay in amber, surge in
  red. Arcs fade in at pickup time.
- **Timeline:** History/Live toggle. Scrubber over Dec 1, 2022 to Mar 1, 2023. Play/pause
  and speed (60×, 300×, 900×, 3600×). Volume sparkline. Weather strip (precipitation bars,
  snow marker).
- **Playback:** the client fetches frames and arcs in 2-hour chunks of history (1-minute
  buckets), keeps one chunk ahead, and computes the rolling window locally. Each chunk is
  one SQL round trip.
- **Side panel:**
  - Freshness badge
  - Then vs. now
  - Rain vs. dry
  - Quarantine counter
  - Surge control (zone picker + button)
  - SQL log (latest queries with ms and rows)

## 10. Deployment

One image (`Dockerfile`, multi-stage: Node builds `web/`, then a Python slim runtime).
Config comes from environment variables (`.env.example`).

- **EC2 + Compose** (`deploy/compose/`): `docker compose up -d` on an instance in the
  peered VPC. Port 8080. Use an SSM port-forward or a security group rule for access.
- **ECS Fargate** (`deploy/cdk/`, CDK Python): imports the existing peered VPC by ID.
  `ApplicationLoadBalancedFargateService` (ALB internal or public, chosen by a context
  flag), image from a local Docker asset, credentials from one Secrets Manager JSON
  secret, health check on `/api/health`. The ALB idle timeout is raised so SSE stays open.
- **Serverless fit:**
  - Connect pipelines: yes, already managed by Redpanda.
  - Frontend on S3 + CloudFront: possible, not needed.
  - API on Lambda: not the default. A Lambda in the VPC has to open a new SQL connection on
    each call and has cold starts, and live mode would need polling instead of SSE. Possible
    later variant.

## 11. Demo script (about 8 min)

1. **History:** play New Year's Eve 2022, 6 PM to 2 AM at 900×. Midtown lights up and
   airport arcs appear. The SQL panel shows the queries and their latency.
2. **Weather:** jump to a rainy day. The weather strip and the rain vs. dry card show
   demand rising and tips changing.
3. **Live:** switch to Live. Replay trips arrive with a freshness of about 2 s, labeled
   "still in topic".
4. **Surge:** pick Madison Square Garden (zone 186, Penn Station/Madison Sq West), press
   Surge, and watch it appear on the map within seconds, enriched by Connect.
5. **Then vs. now:** one SQL query covers rows from 2023 (Iceberg) and from seconds ago
   (topic).
6. **Quality:** the quarantine counter shows bad TLC rows that Connect routed away.

## 12. Repository layout

```
DESIGN.md  README.md  Dockerfile  .env.example
prep.py                     # existing: download + sort TLC parquet
produce.py                  # updated: → trips_raw, origin=history, schema from schemas/
schemas/                    # Avro schemas
scripts/setup_cluster.sh    # topics, schema registration, SQL tables
scripts/sql/create_tables.sql
scripts/prep_zones.py       # TLC zones → web/public/taxi_zones.geojson + app/zones.json
scripts/deploy_pipelines.sh
connect/                    # pipeline templates + render.py
app/                        # FastAPI backend
web/                        # Vite + React + deck.gl
deploy/compose/  deploy/cdk/
```

## 13. Risks and open questions

| Item | Status | Mitigation |
|---|---|---|
| Connect `sql_raw` (`postgres` driver) against the Redpanda SQL endpoint with SCRAM + TLS | Not tested | Fall back to the `pgx` driver. If parameter binding fails, set `unsafe_dynamic_query`; the values are computed in the pipeline, never user input |
| Bound parameters (`$1::timestamptz`) from psycopg and Go drivers | Not tested | Same as above, plus a startup self-test in the backend |
| How `schema_registry_decode` represents Avro logical types and nullable unions | To confirm with the first pipeline run | Normalize in Bloblang (`ts_unix_micro()`, unwrap `{"int": …}`) |
| Composite field access `(redpanda).offset` in SQL | Documented, not tested here | Dedupe by `(pickup_datetime, pickup/dropoff zone, total_amount)` instead |
| Replay depends on the history backfill being finished | By design | `setup_cluster.sh` → `produce.py` → start `replay-shift` |
| `REPLAY_WEEKS` × 1 query every 5 s against SQL | ~0.8 queries/s | Tunable. Each query reads one narrow slice of Iceberg files |
| Duplicate or missing replay rows when a tick is late | Acceptable for a demo | Windows are aligned to 5 s floors |
