"""
Create or update the three Connect pipelines on the BYOC cluster via the Data Plane API.

Prerequisites:
  - python connect/render.py           (writes connect/dist/*.yaml)
  - Cloud secrets RP_USER, RP_PASS, RP_SQL_DSN exist (Secrets Store, scope Redpanda Connect)
  - DATAPLANE_URL   e.g. https://api-xxxx.<cluster-id>.byoc.prd.cloud.redpanda.com
  - DATAPLANE_TOKEN optional; defaults to `rpk cloud auth token`

Usage:
    python scripts/deploy_pipelines.py [replay-shift trips-enrich weather-loader]
"""
import json
import os
import subprocess
import sys
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DESCRIPTIONS = {
    "trips-enrich": "trips_raw -> zones, derived fields, quality routing -> trips_enriched / trips_quarantine",
    "weather-loader": "Open-Meteo hourly NYC weather -> weather_hourly",
    "replay-shift": "Replays same weekday/time-of-day trips from history into trips_raw as live data",
}

base = os.environ["DATAPLANE_URL"].rstrip("/")
token = os.environ.get("DATAPLANE_TOKEN") or subprocess.check_output(
    ["rpk", "cloud", "auth", "token"], text=True).strip()


def call(method: str, path: str, body: dict | None = None) -> dict:
    req = urllib.request.Request(
        f"{base}{path}", method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read() or b"{}")


existing = {p["display_name"]: p["id"]
            for p in call("GET", "/v1/redpanda-connect/pipelines?page_size=100").get("pipelines", [])}

for name in sys.argv[1:] or list(DESCRIPTIONS):
    body = {
        "display_name": name,
        "description": DESCRIPTIONS[name],
        "config_yaml": open(os.path.join(ROOT, "connect", "dist", f"{name}.yaml")).read(),
        "resources": {"memory_shares": "256Mi", "cpu_shares": "200m"},
    }
    if name in existing:
        call("PUT", f"/v1/redpanda-connect/pipelines/{existing[name]}", body)
        print(f"updated {name} ({existing[name]})")
    else:
        created = call("POST", "/v1/redpanda-connect/pipelines", body)
        print(f"created {name} ({created.get('pipeline', created).get('id')})")
