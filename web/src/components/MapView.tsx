import { useEffect, useMemo, useState } from "react";
import DeckGL from "@deck.gl/react";
import { GeoJsonLayer, PathLayer, ScatterplotLayer } from "@deck.gl/layers";
import { Map } from "react-map-gl/maplibre";
import "maplibre-gl/dist/maplibre-gl.css";
import { ArcView, ORIGIN_COLORS, Zones } from "../api";

const BASEMAP = "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json";
const VIEW = { longitude: -73.94, latitude: 40.72, zoom: 10.4, pitch: 40, bearing: -15 };

// Sequential scale: deep blue -> teal -> amber -> red, on a log scale of pickups
const STOPS: [number, number, number][] = [[20, 32, 64], [32, 120, 140], [64, 196, 190], [255, 176, 59], [255, 72, 72]];
function colorFor(n: number, max: number): [number, number, number, number] {
  if (!n) return [0, 0, 0, 0];
  const x = Math.min(1, Math.log1p(n) / Math.log1p(Math.max(max, 1))) * (STOPS.length - 1);
  const i = Math.min(STOPS.length - 2, Math.floor(x));
  const f = x - i;
  const [a, b] = [STOPS[i], STOPS[i + 1]];
  return [a[0] + (b[0] - a[0]) * f, a[1] + (b[1] - a[1]) * f, a[2] + (b[2] - a[2]) * f, 150];
}

// An arc is a parabola above the straight line between the zone centroids, peaking at a
// fifth of the distance. Only the first `progress` of it is returned, so it grows over time.
const SEGMENTS = 24;
function arcPath(src: [number, number], dst: [number, number], progress: number): [number, number, number][] {
  const mPerLon = 111_320 * Math.cos((src[1] * Math.PI) / 180);
  const dist = Math.hypot((dst[0] - src[0]) * mPerLon, (dst[1] - src[1]) * 110_540);
  const at = (t: number): [number, number, number] =>
    [src[0] + (dst[0] - src[0]) * t, src[1] + (dst[1] - src[1]) * t, 0.8 * dist * t * (1 - t)];
  const out = [];
  const n = Math.ceil(progress * SEGMENTS);
  for (let k = 0; k < n; k++) out.push(at(k / SEGMENTS));
  out.push(at(progress));
  return out;
}

type Props = {
  zones: Zones;
  zoneCounts: Map<number, number>;
  arcs: ArcView[];
  selectedZone: number | null;
  onZoneClick: (id: number) => void;
};

export default function MapView({ zones, zoneCounts, arcs, selectedZone, onZoneClick }: Props) {
  const [geo, setGeo] = useState<GeoJSON.FeatureCollection | null>(null);
  useEffect(() => { fetch("/taxi_zones.geojson").then((r) => r.json()).then(setGeo); }, []);

  const max = useMemo(() => Math.max(1, ...zoneCounts.values()), [zoneCounts]);

  const arcData = useMemo(() => arcs.flatMap(({ arc, progress, age }) => {
    const s = zones[arc[2]], d = zones[arc[3]];
    if (s?.lon == null || d?.lon == null || arc[2] === arc[3]) return [];
    const path = arcPath([s.lon, s.lat!], [d.lon, d.lat!], progress);
    return [{ path, head: path[path.length - 1], progress, age, origin: arc[5] }];
  }), [arcs, zones]);

  const layers = [
    new GeoJsonLayer({
      id: "zones",
      data: geo ?? { type: "FeatureCollection", features: [] },
      pickable: true,
      stroked: true,
      filled: true,
      getFillColor: (f: any) => colorFor(zoneCounts.get(f.properties.id) ?? 0, max),
      getLineColor: (f: any) => (f.properties.id === selectedZone ? [255, 72, 72, 255] : [120, 140, 170, 60]),
      getLineWidth: (f: any) => (f.properties.id === selectedZone ? 60 : 12),
      lineWidthMinPixels: 0.5,
      onClick: (info: any) => info.object && onZoneClick(info.object.properties.id),
      updateTriggers: { getFillColor: [zoneCounts, max], getLineColor: selectedZone, getLineWidth: selectedZone },
      transitions: { getFillColor: 300 },
    }),
    new PathLayer({
      id: "arcs",
      data: arcData,
      getPath: (d: any) => d.path,
      getColor: (d: any) => [...ORIGIN_COLORS[d.origin as keyof typeof ORIGIN_COLORS], 160 * (1 - d.age)],
      getWidth: (d: any) => (d.origin === "history" ? 1 : 2),
      widthUnits: "pixels",
      capRounded: true,
      jointRounded: true,
    }),
    // The leading point of each arc that is still travelling
    new ScatterplotLayer({
      id: "arc-heads",
      data: arcData.filter((d) => d.progress < 1),
      getPosition: (d: any) => d.head,
      getFillColor: (d: any) => [...ORIGIN_COLORS[d.origin as keyof typeof ORIGIN_COLORS], 230],
      getRadius: 2.5,
      radiusUnits: "pixels",
    }),
  ];

  return (
    <DeckGL
      initialViewState={VIEW}
      controller
      layers={layers}
      getTooltip={({ object }: any) => {
        if (!object?.properties) return null;
        const p = object.properties;
        return `${p.zone} · ${p.borough}\n${zoneCounts.get(p.id) ?? 0} pickups (15 min) · #${p.id}`;
      }}
    >
      <Map mapStyle={BASEMAP} />
    </DeckGL>
  );
}
