"""
Render the pipeline templates in connect/ into deployable YAML under connect/dist/.

  - Inlines the TLC zone map (app/backend/zones.json) into trips-enrich.
  - Cloud output (default) keeps ${secrets.X} references for Redpanda Cloud secrets. Managed
    pipelines don't see your environment, so every ${NAME:default} (RP_SASL_MECHANISM,
    REPLAY_WEEKS) is replaced with its value from .env / the environment, or the default.
  - --local rewrites ${secrets.X} -> ${X} so `rpk connect run` resolves them from the
    environment (set REDPANDA_BROKERS and REDPANDA_SCHEMA_REGISTRY_URL yourself).

Usage:
    python connect/render.py            # -> connect/dist/*.yaml
    python connect/render.py --local    # -> connect/dist/local/*.yaml
"""
import json
import os
import re
import sys

from dotenv import load_dotenv

HERE = os.path.dirname(os.path.abspath(__file__))
ZONES = os.path.join(HERE, "..", "app", "backend", "zones.json")
PIPELINES = ["replay-shift", "trips-enrich", "weather-loader"]

load_dotenv(os.path.join(HERE, "..", ".env"))
local = "--local" in sys.argv
out_dir = os.path.join(HERE, "dist", "local" if local else "")
os.makedirs(out_dir, exist_ok=True)

zones = {k: {"zone": v["zone"], "borough": v["borough"]} for k, v in json.load(open(ZONES)).items()}
zones_literal = json.dumps(zones, separators=(",", ":"))

for name in PIPELINES:
    text = open(os.path.join(HERE, f"{name}.yaml")).read()
    text = text.replace("__ZONES__", zones_literal)
    if local:
        text = re.sub(r"\$\{secrets\.([A-Z0-9_]+)\}", r"${\1}", text)
    else:
        text = re.sub(r"\$\{([A-Z][A-Z0-9_]*):([^}]*)\}", lambda m: os.environ.get(m[1]) or m[2], text)
    path = os.path.join(out_dir, f"{name}.yaml")
    with open(path, "w") as f:
        f.write(text)
    print(path)
