# Build notes: questions and answers

Notes taken while building and first deploying the NYC Taxi Time Machine demo, kept as raw
material for blog posts. Each entry is a question that came up and the answer we arrived
at. The formal design (decisions D1 to D13, architecture, schemas) is in
[DESIGN.md](../DESIGN.md); this file covers what we learned along the way.

Answers marked *(not measured)* come from reading the code or the docs, not from timing the
real cluster.

## Contents

1. [Design starting points](#1-design-starting-points)
2. [Hosting and deployment](#2-hosting-and-deployment)
3. [Redpanda SQL authentication](#3-redpanda-sql-authentication)
4. [Redpanda Connect in Cloud: secrets and configuration](#4-redpanda-connect-in-cloud-secrets-and-configuration)
5. [Backfill and the enrich pipeline](#5-backfill-and-the-enrich-pipeline)
6. [Live data: replay-shift and surge](#6-live-data-replay-shift-and-surge)
7. [Redpanda SQL quirks](#7-redpanda-sql-quirks)
8. [Frontend behavior](#8-frontend-behavior)
9. [Load, performance and architecture](#9-load-performance-and-architecture)
10. [Open items](#10-open-items)

---

## 1. Design starting points

The full rationale is in [DESIGN.md §3](../DESIGN.md#3-key-decisions). The short version of
the questions behind it:

**What makes this a good Redpanda SQL demo?**
One SQL table over an Iceberg-enabled topic returns both the rows still in the topic and
the rows already committed to Iceberg, with no `UNION ALL`. The app shows that by playing
2022–23 history (only in Iceberg, because topic retention is 1 hour) next to a live stream
(newer than the last Iceberg commit, so only in the topic), from the same table.

**Where does the "live" data come from?**
*Replay-shift*: a Redpanda Connect pipeline reads trips from the same weekday and time of
day in the history, through Redpanda SQL, and produces them again with today's
timestamps. The live stream follows real NYC patterns (rush hours, airports) without a
custom generator (D3).

**Why add a Connect pipeline in the middle?**
Every producer (backfill, replay, surge button) writes to `trips_raw`. The `trips-enrich`
pipeline adds zone names, derived fields (duration, speed, tip %), and routes rows that
fail quality rules to `trips_quarantine`. History and live data share one schema and one
enrichment path (D4, D8).

**Which extra public data source, without overdoing it?**
Open-Meteo hourly weather for NYC: free, no API key, about 2,200 rows for the three months.
It lands in its own Iceberg topic and is joined in SQL ("weather vs. demand"), which shows
a `JOIN` across two Iceberg-backed topics (D6).

**Is serverless a fit?**
Partly. The Connect pipelines are already managed. The API is not a good Lambda fit: a
Lambda in the VPC opens a new SQL connection per call, has cold starts, and can't hold the
live SSE stream (D12, [DESIGN.md §10](../DESIGN.md#10-deployment)).

**How are time zones handled?**
TLC timestamps have no time zone. Everything (history, live, weather) stores NYC wall-clock
time labelled as UTC, so joins and "same time of day" comparisons just work
([DESIGN.md §5](../DESIGN.md#5-time-convention-d9)).

---

## 2. Hosting and deployment

**What EC2 instance size runs the backend and serves the frontend?**
A t3.medium (2 vCPU, 4 GiB) is plenty. The app is one uvicorn process with a small SQL
connection pool, a Kafka producer and one shared live poller; it runs at a few hundred MB
of RAM, because Redpanda SQL does the scanning and aggregation. The frontend is static
files once built. The peak is the image build (`npm install` plus the Vite build of
deck.gl/MapLibre, about 1–2 GB for a minute or two). A t3.small (2 GiB) would be tight for
the build. Keep t3 credits on `unlimited`, use 20 GB+ of gp3, and open egress to the
cluster on 5432 (SQL), 9092 (Kafka) and 30081 (Schema Registry). A t4g.medium (Graviton)
should also work since all base images and wheels have arm64 builds, but we didn't test it.

**What's the rollout plan?**
1. Clone the repo on an EC2 host in the VPC peered with the cluster.
2. Run in dev mode (uvicorn + Vite) and validate against the real cluster.
3. Run the same image with Compose on that host.
4. Deploy to Fargate.

Step 4 doesn't need a registry of your own: the CDK stack uses
`ContainerImage.from_asset`, so `cdk deploy` builds the linux/amd64 image and pushes it to
the CDK bootstrap ECR repository.

**What tools does an Ubuntu host need?**
git, curl and unzip; Node 22 (NodeSource) for the frontend; rpk for topic creation, schema
registration and `rpk cloud auth token`; uv, which installs Python 3.13 itself. All Python
dependencies (psycopg[binary], confluent-kafka, duckdb, pyarrow) ship as prebuilt x86_64
wheels, so no compilers or `libpq-dev`. The host has no browser, so rpk logs in with a
service account's client ID and secret.

**How do you open the dev UI on a remote host?**
The Vite dev server only listens on `localhost:5173`. Either SSH-tunnel both ports
(`ssh -L 5173:localhost:5173 -L 8080:localhost:8080 …`), or run `npm run build` once and
let uvicorn serve the UI and API together on `:8080`.

**Does Compose work with Podman?**
Yes, with one change: fully qualified base images (`docker.io/library/node:22-slim`).
Ubuntu's Podman often has no unqualified-search registry configured, so short names fail
with "short-name did not resolve". Two rootless-Podman differences from Docker:
`restart: unless-stopped` doesn't survive a reboot unless you enable lingering and
`podman-restart.service`, and it's worth checking that containers resolve the cluster's
private hostnames.

**Do the dev servers reload after `git pull`?**
The frontend does: Vite watches `web/src` and hot-reloads the open tab. uvicorn only
reloads if started with `--reload`; otherwise restart it after backend changes.

---

## 3. Redpanda SQL authentication

**Can the Kafka SASL user also sign in to Redpanda SQL?**
No. SQL users are Redpanda Cloud identities (org users or service accounts) that hold a
data-plane role with a SQL permission. Kafka SCRAM users are a separate, cluster-level
list. Assigning a role with **SQL: Access** or **SQL: Manage** to an identity
automatically creates the matching user in the SQL engine; no `CREATE USER`.

**SQL: Manage or SQL: Access?**
- **SQL: Manage** is the SQL engine's superuser: it can read every topic, create catalogs
  and tables, and grant access. Simplest for a demo.
- **SQL: Access** can connect but can't read anything until a SQL: Manage user runs
  `GRANT SELECT` on specific tables. Least privilege.

**Do Kafka ACLs apply to SQL queries?**
No. Redpanda SQL reaches the topics with its own internal SASL credential, whoever runs the
query. Access control for SQL is the SQL-level `GRANT`/`REVOKE`.

**How do you create the SQL identity?**
In the Cloud Console: **Organization IAM → Service accounts → Create service account**,
with a role binding scoped to the cluster and a role that has a SQL permission (roles are
defined under **Organization IAM → Roles → Data Plane**). Copy the client ID and secret.

**Which auth mode should the app use?**
Redpanda SQL supports three, chosen by the connection's `options` parameter:

| Mode | `options` | User / password |
|---|---|---|
| Client credentials | `auth_method=client_secret` | client ID / client secret |
| Bearer token | `auth_method=bearer` | any / a Cloud-issued JWT |
| SCRAM | (none) | service account email / password set with `ALTER USER` |

A service account works with both client credentials and SCRAM; the app's `RP_SQL_AUTH`
just says which credentials are in `.env`. We use `client_secret` for the app because it
needs no password setup. The `replay-shift` pipeline still needs SCRAM, because its DSN is
a plain Postgres URL.

**The Console shows only a client ID and secret. What's the SCRAM username?**
The SQL username is the service account's email (it looks like
`<id>@iam.serviceaccount.redpanda.com`), and the Console doesn't show it. Sign in with the
client secret and ask the engine: `SELECT current_user`.

**Why did `ALTER USER … WITH PASSWORD …` keep failing with syntax errors?**
The username is an identifier and needs **double** quotes; the password is a string
literal and needs **single** quotes:

```sql
ALTER USER "<service-account-email>" WITH PASSWORD '<password>';
```

Getting that through a shell `-c "…"` argument is error-prone, so write the statement to a
file with a quoted heredoc and run the file. A SQL: Manage service account can set its own
password. The password survives cluster operations; it's only revoked if the account loses
its SQL role.

**How does that go into a Postgres URL?**
URL-encode the email (`@` → `%40`) and any `@ : / ? #` in the password:
`postgres://<id>%40iam.serviceaccount.redpanda.com:<password>@<sql-host>:5432/oxla?sslmode=require`.

**Is the database `oxla` or `default`?**
Both connect, so the engine apparently ignores the name. We use `oxla`, as the docs do.
The `default` in the Console is a schema/namespace, not a database. Queries name tables in
full (`default_redpanda_catalog=>trips_enriched`) anyway.

---

## 4. Redpanda Connect in Cloud: secrets and configuration

**Which secrets do the pipelines need?**
Three, in the cluster's **Secrets Store** with scope **Redpanda Connect**: `RP_USER`,
`RP_PASS` (Kafka SASL, used for Schema Registry and the `redpanda` input/output) and
`RP_SQL_DSN` (only `replay-shift`). Broker and Schema Registry addresses come from Cloud's
built-in `${REDPANDA_BROKERS}` and `${REDPANDA_SCHEMA_REGISTRY_URL}`.

**Can one secret reference another?**
No, values are stored as entered. But a pipeline field can combine several secrets
(every `${secrets.X}` in a string is substituted), so a DSN could also be built in YAML
from separate user/password secrets, using the key=value DSN form to avoid URL encoding.
We kept a single prebuilt `RP_SQL_DSN`.

**How do the same pipeline files run in Cloud and locally?**
The templates use `${secrets.X}`. `connect/render.py` writes the Cloud version as is;
`--local` rewrites `${secrets.X}` to `${X}` so `rpk connect run` (or the Compose `connect`
profile) reads the same values from the environment.

**Is there an API instead of pasting YAML into the Console?**
Yes, the Data Plane API's `/v1/redpanda-connect/pipelines`. `scripts/deploy_pipelines.py`
creates or updates each pipeline by display name. It also creates or updates the secrets
the selected pipelines reference (`/v1/secrets`, Base64 values, scope
`SCOPE_REDPANDA_CONNECT`), reading the values from `.env`; it finds the names by scanning
the YAML for `${secrets.X}`. A pipeline reads a secret's value only when it starts.

**How do you make the SASL mechanism configurable?**
The templates use `${RP_SASL_MECHANISM:SCRAM-SHA-256}`. Locally, Connect reads it from the
environment. The catch: **managed Cloud pipelines don't see your environment**, so a
`${VAR:default}` always falls back to its default there. That had silently pinned
`REPLAY_WEEKS` to 4 in Cloud. `render.py` now writes the `.env` value (or the default) of
every `${NAME:default}` into the Cloud YAML at render time.

---

## 5. Backfill and the enrich pipeline

**`produce.py` wrote 9,302,651 rows in 187 s, but SQL counted only about 1M. Where's the rest?**
Still in `trips_raw`, waiting for `trips-enrich`. The producer writes at about 50k rows/s;
the pipeline decodes, enriches, routes and re-encodes each row at roughly 5k rows/s on its
200m CPU *(rate measured locally, not on the cluster)*. `rpk group describe trips-enrich`
showed `TOTAL-LAG` of about 7.7M, which drains in roughly 25–30 minutes.

**What's the risk while it catches up?**
`trips_raw` has 1-hour retention, counted from when rows were produced, not read. If the
backlog takes longer than that, unread rows are deleted. Raise retention for the backfill
(`rpk topic alter-config trips_raw --set retention.ms=86400000`) and lower it again after
the lag reaches 0. Or give the pipeline more CPU (`cpu_shares` in `deploy_pipelines.py`).

**When does a trip go to `trips_quarantine`?**
When it fails one of these rules (the first failure becomes its `reason`):

| `reason` | Rule |
|---|---|
| `duration<=0` | dropoff at or before pickup |
| `duration>6h` | trip longer than 360 minutes |
| `speed>100mph` | average speed above 100 mph |
| `distance>200mi` | `trip_distance` above 200 miles |
| `fare<0` | negative fare (refunds, adjustments) |
| `total>1000` | `total_amount` above $1,000 |

The pipeline sets the destination topic and its Schema Registry subject per message from
`@route`. Quarantined rows keep all enriched fields plus `reason`. They never appear on
the map; only the Quality card reads `trips_quarantine`.

---

## 6. Live data: replay-shift and surge

**What does the Surge card do, and how is it different from replay-shift?**

| | `replay-shift` | Surge |
|---|---|---|
| Where it runs | Connect pipeline, every 5 s | FastAPI app, on a button press |
| Trip data | real 2022–23 trips from the same weekday and time, restamped to now | made up: random distance, fare, tip and drop-off |
| Pickup zones | spread across the city like real demand | all from the selected zone |
| Volume | steady (4 historical weeks by default) | one burst of up to 1,000 |
| `origin` | `replay` (orange) | `surge` (red) |
| Purpose | realistic background traffic | a visible spike on demand |

Both write to `trips_raw` and follow the same path through `trips-enrich` into
`trips_enriched`, so the spike shows up in SQL seconds later: that's the freshness story.

**The live clock shows a time with no date. Which day is it?**
Today, now, in New York: Live mode is current trips (replayed history restamped to the
present). The clock now shows the date above the time to make that obvious.

---

## 7. Redpanda SQL quirks

**Why did Then vs now fail with `syntax error, unexpected PERIOD`?**
`period` is a reserved word in Redpanda SQL, though PostgreSQL accepts it as a column
alias. Renaming the alias (to `era`) fixed it. Other aliases (`zone`, `hours`, `sky`, `t`)
worked.

Other behavior worth knowing (from building the queries; see DESIGN.md):
- An Iceberg-enabled topic gets an automatic `redpanda` struct column with partition,
  offset and timestamp; `(redpanda).partition` and `(redpanda).offset` identify a record.
- Useful functions: `date_trunc`, `unix_micros`/`unix_millis`, `timestamp_micros`.
  `extract()` has no day-of-week.

---

## 8. Frontend behavior

**Do the arcs disappear when the trip reaches its destination?**
Originally no: each arc appeared whole at pickup and faded over a fixed window, regardless
of the dropoff time; a bright-to-dim color gradient was the only direction cue.

**How do arcs grow from pickup to dropoff now?**
deck.gl's `ArcLayer` can't draw a partial arc, so arcs are computed as 3D paths (a
parabola over the straight line between zone centroids, peaking at a fifth of the
distance) and drawn with a `PathLayer`, plus a `ScatterplotLayer` dot at the leading end.
- History: each arc grows over the trip's real duration at playback speed (a 15-minute
  trip takes 1 s at 900×), then fades over about 1.2 s.
- Live: real trips take minutes, too slow to watch, so arcs grow over 3 s and fade over the
  rest of a 45 s window.

Trade-off: arcs stay on screen longer, so the map is busier (about 2× at 900×).

**Smaller UI touches**
- Timeline presets stay highlighted until you click the track.
- Hovering the timeline shows the date and time under the cursor.
- Hovering a card shows, briefly, how that card gets its data (see §9).

---

## 9. Load, performance and architecture

**How often does the frontend query the backend?** *(not measured)*

| What | When | SQL queries | Scales with |
|---|---|---|---|
| Meta, zones, timeline volume and weather | once per page load | 1 each for volume/weather, cached | — |
| History playback (`/api/chunk`) | about one chunk every 4 s of playback; the chunk grows with speed (1 h at 900×, 4 h at 3600×) | 2 per chunk | each viewer (now cached, see below) |
| Live (shared poller) | every 2 s while anyone is connected | 2 | **shared** across viewers |
| Then vs now | every 15 s in Live mode | 1 | shared via a 10 s cache |
| Quality | every 30 s | 1, cached 30 s | — |
| Weather vs demand | every 10 min | 1, cached 10 min | — |

About 0.5 queries/s per viewer during playback and about 1 query/s in Live mode however
many people watch. That's light for Redpanda SQL; the cost is what each query scans.

**Is it polling or push?**
A mix. Live mode is **SSE**: one `EventSource` on `/api/live`, with the server pushing
every 2 s. History playback is **on demand**: chunks are fetched as the playhead moves,
nothing while paused. The insight cards **poll** on timers. Behind SSE, the backend itself
**polls Redpanda SQL** with one shared loop. The app reads only through SQL, never by
consuming the topic, because the point is that one SQL table returns both live and
Iceberg rows.

**What works well in the current design?**
- The shared live poller: live load is flat in the number of viewers; the
  `(redpanda).partition/offset` seen-set sends each arc once.
- Bounded prefetch: chunk size follows playback speed, so there's always about 4 s of
  buffer, and playback pauses ("loading from Iceberg…") rather than queueing requests.
- Server-side TTL caches on aggregates, and an SQL log that shows exactly what ran.

**What did we change after reviewing it?**
1. **Cache playback chunks**: 30 min on the server (LRU, 256 entries), 10 min in the
   browser. History is immutable once the backfill is done; the TTLs are short on purpose
   while it isn't.
2. **Cache Then vs now for 10 s**, so tabs share one query.
3. **Coalesce concurrent misses**: identical requests arriving together share one query
   (5 parallel chunk requests ran 2 SQL queries instead of 10 in a test).
4. **Label cache hits** as "cached" in the SQL log instead of repeating the old latency.
5. **Debounce timeline drags** (150 ms), so a drag only loads the chunks where it stops.

---

## 10. Open items

Ideas from the review that we haven't done yet:

- **Iceberg partitioning.** By default Redpanda partitions an Iceberg table by
  `hour(redpanda.timestamp)`, the produce time, not the pickup time. The whole backfill
  went in within minutes, so partitions don't help a pickup-time filter; only file-level
  min/max statistics on `pickup_datetime` can skip files. They should be tight because
  `prep.py` sorted the data by pickup, but check the query times in the SQL log. If scans
  are slow, set `redpanda.iceberg.partition.spec=(day(pickup_datetime))` on
  `trips_enriched` and backfill again.
- **Live re-reads.** Each 2 s poll reads the last 30 s, so every row is read about 15 times
  and deduplicated. A 6–10 s window, or reading only offsets above the last one seen, would
  cut that.
- **One Fargate task.** The poller and caches are in-process; more tasks would multiply
  live load. Fine for a demo; a public deployment would need a shared cache and a single
  poller.
- **Not yet verified on the real cluster** ([DESIGN.md §13](../DESIGN.md#13-risks-and-open-questions)):
  `sql_raw` with bound parameters in `replay-shift`, and the key=value DSN form in Connect.
