"""
Build the NYC taxi zone assets used by the app and the Connect pipelines.

Downloads the TLC zone lookup CSV and zone shapefile (NY State Plane, EPSG:2263),
reprojects to WGS84 and writes:
  - web/public/taxi_zones.geojson  polygons for the map (properties: id, zone, borough)
  - app/backend/zones.json         id -> {zone, borough, lon, lat} (centroid), used by the
                                   backend and inlined into the trips-enrich pipeline

Usage:
    uv run --python 3.13 --with pyshp --with pyproj scripts/prep_zones.py
"""
import csv
import io
import json
import os
import urllib.request
import zipfile

import shapefile  # pyshp
from pyproj import Transformer

LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
SHAPES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GEOJSON_OUT = os.path.join(ROOT, "web/public/taxi_zones.geojson")
ZONES_OUT = os.path.join(ROOT, "app/backend/zones.json")


def fetch(url: str) -> bytes:
    with urllib.request.urlopen(url) as r:
        return r.read()


def ring_area_centroid(ring):
    """Signed area and centroid of a ring (shoelace), in the ring's own units."""
    a = cx = cy = 0.0
    for (x0, y0), (x1, y1) in zip(ring, ring[1:] + ring[:1]):
        cross = x0 * y1 - x1 * y0
        a += cross
        cx += (x0 + x1) * cross
        cy += (y0 + y1) * cross
    a /= 2.0
    if a == 0:
        return 0.0, ring[0]
    return a, (cx / (6 * a), cy / (6 * a))


lookup = {}
for row in csv.DictReader(io.StringIO(fetch(LOOKUP_URL).decode())):
    lookup[int(row["LocationID"])] = {"zone": row["Zone"], "borough": row["Borough"]}

z = zipfile.ZipFile(io.BytesIO(fetch(SHAPES_URL)))
names = {os.path.splitext(n)[1].lower(): n for n in z.namelist() if not n.startswith("__MACOSX")}
reader = shapefile.Reader(shp=io.BytesIO(z.read(names[".shp"])),
                          shx=io.BytesIO(z.read(names[".shx"])),
                          dbf=io.BytesIO(z.read(names[".dbf"])))
to_wgs84 = Transformer.from_crs("EPSG:2263", "EPSG:4326", always_xy=True)

# Some LocationIDs appear in more than one shape record: merge them into one MultiPolygon.
polygons: dict[int, list] = {}
weights: dict[int, list] = {}
for rec in reader.iterShapeRecords():
    loc_id = int(rec.record["LocationID"])
    geo = rec.shape.__geo_interface__
    polys = [geo["coordinates"]] if geo["type"] == "Polygon" else geo["coordinates"]
    for poly in polys:
        outer = [tuple(p) for p in poly[0]]
        area, (cx, cy) = ring_area_centroid(outer)
        weights.setdefault(loc_id, []).append((abs(area), cx, cy))
        polygons.setdefault(loc_id, []).append([
            [[round(v, 5) for v in to_wgs84.transform(x, y)] for x, y in ring] for ring in poly
        ])

features, zones = [], {}
for loc_id in sorted(polygons):
    info = lookup.get(loc_id, {"zone": "Unknown", "borough": "Unknown"})
    total = sum(w for w, _, _ in weights[loc_id])
    cx = sum(w * x for w, x, _ in weights[loc_id]) / total
    cy = sum(w * y for w, _, y in weights[loc_id]) / total
    lon, lat = to_wgs84.transform(cx, cy)
    zones[loc_id] = {**info, "lon": round(lon, 5), "lat": round(lat, 5)}
    features.append({
        "type": "Feature",
        "properties": {"id": loc_id, **info},
        "geometry": {"type": "MultiPolygon", "coordinates": polygons[loc_id]},
    })

# Zones present in the lookup but without a polygon (264 "NV", 265 "Outside of NYC").
for loc_id, info in lookup.items():
    zones.setdefault(loc_id, {**info, "lon": None, "lat": None})

with open(GEOJSON_OUT, "w") as f:
    json.dump({"type": "FeatureCollection", "features": features}, f, separators=(",", ":"))
with open(ZONES_OUT, "w") as f:
    json.dump({str(k): zones[k] for k in sorted(zones)}, f, indent=1)

print(f"{len(features)} zone polygons -> {GEOJSON_OUT} ({os.path.getsize(GEOJSON_OUT) / 1e6:.1f} MB)")
print(f"{len(zones)} zones -> {ZONES_OUT}")
