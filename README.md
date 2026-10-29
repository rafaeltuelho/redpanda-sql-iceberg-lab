# NYC Taxi Time Machine: Redpanda Iceberg topics + Redpanda SQL + Redpanda Connect

A demo web app that replays 9M NYC taxi trips on a map. The historical playback is served
from Iceberg, and the live view shows synthetic trips that are still in the topic. One SQL
table returns both. Redpanda Connect generates the live stream, enriches every trip, routes
bad rows to quarantine and loads NYC weather for a SQL join.

Design and decisions: [DESIGN.md](DESIGN.md).

```
replay-shift (Connect) ─┐
produce.py backfill ────┼─▶ trips_raw ─▶ trips-enrich (Connect) ─▶ trips_enriched (Iceberg) ─┐
surge button (app) ─────┘                                     └──▶ trips_quarantine (Iceberg) ├─▶ Redpanda SQL ─▶ app
weather-loader (Connect) ─────────────────────────────────────────▶ weather_hourly (Iceberg) ─┘
```

## 1. Enable Iceberg on the cluster

* Reference: https://docs.redpanda.com/cloud-data-platform/manage/iceberg/iceberg-topics-aws-glue/

```shell
# Glue requires Redpanda Iceberg tables to be manually deleted
# so iceberg_delete is set to false.
rpk cloud login

rpk profile create --from-cloud <cluster-id>

rpk cluster config set \
  iceberg_enabled=true \
  iceberg_delete=false \
  iceberg_default_catalog_namespace='["<custom-namespace>"]' \
  iceberg_catalog_type=rest \
  iceberg_rest_catalog_endpoint=https://glue.<glue-region>.amazonaws.com/iceberg \
  iceberg_rest_catalog_authentication_mode=aws_sigv4 \
  iceberg_rest_catalog_credentials_source=sts \
  iceberg_rest_catalog_aws_region=<glue-region> \
  iceberg_rest_catalog_base_location=s3://<cluster-storage-bucket-name>/<warehouse-path>
```

## 2. Local environment

The scripts and the app talk to the cluster's private endpoints, so this usually runs on an
EC2 host in the VPC peered with the cluster. A t3.medium (2 vCPU, 4 GiB, `unlimited`
credits, 20 GB gp3) is enough to build and run everything. Its security group needs egress to
the cluster on 5432 (SQL), 9092 (Kafka) and 30081 (Schema Registry), and ingress on 8080
from your IP only.

On Ubuntu, install git, Node 22 (for the frontend) and rpk (used by `setup_cluster.sh`
and `deploy_pipelines.py`):

```shell
sudo apt-get update && sudo apt-get install -y git curl unzip
curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash - && sudo apt-get install -y nodejs
curl -LO https://github.com/redpanda-data/redpanda/releases/latest/download/rpk-linux-amd64.zip
mkdir -p ~/.local/bin && unzip -o rpk-linux-amd64.zip -d ~/.local/bin/
```

The host has no browser, so log rpk in with a service account
(`rpk cloud login --client-id … --client-secret …`) or set `DATAPLANE_TOKEN` for
`deploy_pipelines.py`.

uv installs Python 3.13 itself. All Python dependencies come as prebuilt x86_64 wheels, so
no compiler or `libpq-dev` is needed:

```shell
curl -LsSf https://astral.sh/uv/install.sh | sh
uv venv --python 3.13 .venv
uv pip install -r requirements-tools.txt
cp .env.example .env    # fill in Kafka, Schema Registry and Redpanda SQL settings
```

Credentials you need:

* **Kafka user** (`RP_USER` / `RP_PASS`, SASL/SCRAM) with produce and consume on the four
  topics, and with Schema Registry access.
* **Redpanda SQL service account.** Kafka SASL users can't sign in to Redpanda SQL. SQL
  users are Redpanda Cloud identities (org users or service accounts) that hold a data-plane
  role with a SQL permission. See the steps below.

**Create the SQL service account:**

1. In the Cloud Console, go to **Organization IAM → Service accounts → Create service
   account**, and give it a name (for example `nyc-taxi-lab-user`).
2. Under **Role binding**, choose scope **Cluster**, your cluster as the resource, and a role
   with a SQL permission. Roles are defined under **Organization IAM → Roles → Data Plane**.
   * **SQL: Manage** is the simplest choice for a demo. It can read every topic, create
     the tables in `scripts/sql/create_tables.sql` and set passwords, so no grants are
     needed.
   * **SQL: Access** is least privilege. It can only read what it's granted, so a
     SQL: Manage user has to run `GRANT SELECT` on the three tables after step 3 below.
3. Copy the client ID and secret. Assigning the role creates the matching user in the SQL
   engine; you don't need `CREATE USER`.
4. Choose how the app signs in (`.env`):
   * `RP_SQL_AUTH=client_secret`, with `RP_SQL_USER` set to the client ID and
     `RP_SQL_PASSWORD` set to the client secret. No password setup is needed.
   * `RP_SQL_AUTH=scram`, with `RP_SQL_USER` set to the service account's email and
     `RP_SQL_PASSWORD` set to the password from step 5.
5. Set a SCRAM password. The `replay-shift` pipeline always needs one, because its
   `RP_SQL_DSN` secret is a plain Postgres URL.

   The Console shows a service account's client ID and secret, but not the email that is
   its SQL username. To find the email, sign in with the client secret (step 4) and ask the
   engine:

   ```shell
   python scripts/run_sql.py -c "SELECT current_user"
   # <service-account-id>@iam.serviceaccount.redpanda.com
   ```

   Then set the password as a SQL: Manage user. If the service account has SQL: Manage,
   it can set its own password. The username is an identifier, so it takes double
   quotes; the password is a string literal, so it takes single quotes. Write the statement
   to a file so the shell doesn't mangle either:

   ```shell
   cat > /tmp/pw.sql <<'EOF'
   ALTER USER "<service-account-email>" WITH PASSWORD '<password>';
   EOF
   python scripts/run_sql.py /tmp/pw.sql && rm /tmp/pw.sql
   ```

   In `RP_SQL_DSN`, URL-encode the email (`@` becomes `%40`), along with any
   `@ : / ? #` in the password:
   `postgres://<sa-id>%40iam.serviceaccount.redpanda.com:<password>@<sql-host>:5432/oxla?sslmode=require`

If you also use this service account for
`rpk cloud login --client-id … --client-secret …` (for `deploy_pipelines.py`), its role
also needs permission to manage Redpanda Connect pipelines.

## 3. Topics, schemas and SQL tables

```shell
./scripts/setup_cluster.sh
```

This creates `trips_raw` (1 h retention), plus `trips_enriched`, `trips_quarantine` and
`weather_hourly` (Iceberg, 1 h retention, 60 s target lag). It registers the Avro schemas
in `schemas/` and maps the SQL tables (`scripts/sql/create_tables.sql`).

## 4. Connect pipelines

Create these secrets in the Cloud Console (**Secrets Store**, scope Redpanda Connect):
`RP_USER`, `RP_PASS` and `RP_SQL_DSN` (`postgres://<user>:<password>@<sql-host>:5432/oxla?sslmode=require`).

```shell
python connect/render.py                       # -> connect/dist/*.yaml (zone map inlined)
export DATAPLANE_URL=https://api-....byoc.prd.cloud.redpanda.com
python scripts/deploy_pipelines.py trips-enrich weather-loader
```

You can also paste `connect/dist/<name>.yaml` into **Connect → Create pipeline** in the
Cloud Console. To run the pipelines yourself instead, use `python connect/render.py --local`
and `rpk connect run connect/dist/local/<name>.yaml` (or the Compose `connect` profile).

## 5. Backfill history, then start live replay

```shell
python scripts/prep.py                                  # downloads and sorts Dec 2022 - Feb 2023 (~9M rows)
python scripts/produce.py trips_sorted.parquet trips_raw
python scripts/run_sql.py -c "SELECT count(*) FROM default_redpanda_catalog=>trips_enriched"
python scripts/deploy_pipelines.py replay-shift # once the backfill is enriched
```

`replay-shift` reads history through Redpanda SQL, so start it after `trips_enriched` holds
the backfill.

## 6. Run the app

| Where | How |
|---|---|
| Laptop / VPC host (dev) | `.venv/bin/uvicorn app.backend.main:app --port 8080`, and `cd web && npm install && npm run dev` |
| EC2 + Compose | `docker compose -f deploy/compose/docker-compose.yml --env-file .env up -d --build` → `:8080` (`podman compose` takes the same arguments) |
| ECS Fargate | Store the `.env` keys as a JSON secret named `taxi-time-machine` in Secrets Manager, then `cd deploy/cdk && pip install -r requirements.txt && cdk deploy -c vpc_id=vpc-…` (add `-c public=true` for an internet-facing ALB) |

The Fargate tasks run in private subnets with egress, so they need a NAT gateway or VPC
endpoints to pull the image from ECR. `cdk deploy` builds the image (linux/amd64) and pushes
it to the CDK bootstrap ECR repository, so you don't need a registry of your own.

**Dev mode on a remote host.** The Vite dev server only listens on `localhost:5173`. Tunnel
both ports from your laptop and open http://localhost:5173:

```shell
ssh -L 5173:localhost:5173 -L 8080:localhost:8080 ubuntu@<ec2-host>
```

Or skip Vite: run `cd web && npm ci && npm run build`, and uvicorn serves the UI and the
API on `:8080`.

**Podman.** The Compose file works with `podman compose` (`sudo apt-get install -y podman
podman-compose`). Rootless Podman doesn't apply `restart: unless-stopped` after a reboot
unless you enable it once:

```shell
loginctl enable-linger $USER && systemctl --user enable --now podman-restart.service
```

If the container can't reach the cluster, check that it resolves the private hostnames:
`podman exec -it <app-container> getent hosts <sql-host>`.

**First-run checks**, in this order. Each one exercises a part that has only been tested
against a local Redpanda ([DESIGN.md §13](DESIGN.md#13-risks-and-open-questions)):

1. `curl localhost:8080/api/health`: the SQL pool and auth.
2. `curl "localhost:8080/api/volume?from=1669852800000&to=1669939200000"` (Dec 1, 2022): bound parameters and the `catalog=>table` mapping.
3. **Live** mode in the UI: `(redpanda).partition/offset` access (there's a fallback query if
   it fails).
4. `replay-shift` (start it last): `sql_raw` against Redpanda SQL. Check its logs in the
   Connect pipeline view.

## 7. Demo

The demo script is in [DESIGN.md §11](DESIGN.md#11-demo-script-about-8-min):
1. Play New Year's Eve.
2. Jump to the rainiest day.
3. Switch to **Live** and watch the freshness badge.
4. **Surge** Penn Station / MSG.
5. Show then vs. now.
6. Show quarantined rows.

Every view has its SQL in the **SQL log** panel.

## Repository

| Path | What |
|---|---|
| `scripts/prep.py`, `scripts/produce.py` | Download and sort the TLC data, backfill into `trips_raw` |
| `schemas/` | Avro schemas for the four topics |
| `scripts/` | Data prep and backfill, cluster setup, SQL runner, zone asset prep, pipeline deploy |
| `connect/` | Pipeline templates and `render.py` |
| `app/backend/` | FastAPI API (`/api/*`) and shared live poller |
| `web/` | React + deck.gl + MapLibre frontend |
| `deploy/` | Docker Compose and CDK (Fargate) |
