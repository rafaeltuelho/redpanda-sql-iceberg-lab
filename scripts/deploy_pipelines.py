"""
Create or update the three Connect pipelines on the BYOC cluster via the Data Plane API.

Before deploying, every ${secrets.X} the selected pipelines reference is created or updated
in the cluster's Secrets Store (scope Redpanda Connect), with its value read from the
environment variable X (the repo's .env is loaded). Pass --no-secrets to skip this.

Prerequisites:
  - python connect/render.py           (writes connect/dist/*.yaml)
  - RP_USER, RP_PASS (and RP_SQL_DSN for replay-shift) set in .env
  - DATAPLANE_URL   e.g. https://api-xxxx.<cluster-id>.byoc.prd.cloud.redpanda.com
  - DATAPLANE_TOKEN optional; defaults to `rpk cloud auth token`

Usage:
    python scripts/deploy_pipelines.py [--no-secrets] [replay-shift trips-enrich weather-loader]
"""
import base64
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request

from dotenv import load_dotenv

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DESCRIPTIONS = {
    "trips-enrich": "trips_raw -> zones, derived fields, quality routing -> trips_enriched / trips_quarantine",
    "weather-loader": "Open-Meteo hourly NYC weather -> weather_hourly",
    "replay-shift": "Replays same weekday/time-of-day trips from history into trips_raw as live data",
}
SCOPE = "SCOPE_REDPANDA_CONNECT"

load_dotenv(os.path.join(ROOT, ".env"))
args = [a for a in sys.argv[1:] if not a.startswith("--")]
sync_secrets = "--no-secrets" not in sys.argv
names = args or list(DESCRIPTIONS)

base = os.environ["DATAPLANE_URL"].rstrip("/")
token = os.environ.get("DATAPLANE_TOKEN") or subprocess.check_output(
    ["rpk", "cloud", "auth", "token"], text=True).strip()


def call(method: str, path: str, body: dict | None = None) -> dict:
    req = urllib.request.Request(
        f"{base}{path}", method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        sys.exit(f"{method} {path}: HTTP {e.code} {e.read().decode(errors='replace')}")


configs = {name: open(os.path.join(ROOT, "connect", "dist", f"{name}.yaml")).read() for name in names}

if sync_secrets:
    needed = sorted({s for cfg in configs.values() for s in re.findall(r"\$\{secrets\.([A-Z][A-Z0-9_]*)\}", cfg)})
    missing = [s for s in needed if not os.environ.get(s)]
    if missing:
        sys.exit(f"set {', '.join(missing)} in .env (or pass --no-secrets to manage secrets yourself)")
    have = {s["id"] for s in call("GET", "/v1/secrets?page_size=100").get("secrets", [])}
    for sid in needed:
        body = {"scopes": [SCOPE], "secret_data": base64.b64encode(os.environ[sid].encode()).decode()}
        if sid in have:
            call("PUT", f"/v1/secrets/{sid}", body)
            print(f"updated secret {sid}")
        else:
            call("POST", "/v1/secrets", {"id": sid, **body})
            print(f"created secret {sid}")

existing = {p["display_name"]: p["id"]
            for p in call("GET", "/v1/redpanda-connect/pipelines?page_size=100").get("pipelines", [])}

for name in names:
    body = {
        "display_name": name,
        "description": DESCRIPTIONS[name],
        "config_yaml": configs[name],
        "resources": {"memory_shares": "256Mi", "cpu_shares": "200m"},
    }
    if name in existing:
        call("PUT", f"/v1/redpanda-connect/pipelines/{existing[name]}", body)
        print(f"updated {name} ({existing[name]})")
    else:
        created = call("POST", "/v1/redpanda-connect/pipelines", body)
        print(f"created {name} ({created.get('pipeline', created).get('id')})")
