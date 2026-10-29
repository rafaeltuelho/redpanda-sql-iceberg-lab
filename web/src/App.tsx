import { useCallback, useEffect, useState } from "react";
import { api, Meta, ORIGIN_COLORS, QueryInfo, Zones } from "./api";
import MapView from "./components/MapView";
import Timeline from "./components/Timeline";
import { QualityCard, RainCard, SourceCard, SqlLog, SurgeCard, ThenVsNowCard } from "./components/Panel";
import { usePlayback } from "./usePlayback";
import { useLive } from "./useLive";

type Mode = "history" | "live";
type Weather = [number, number | null, number | null, number | null, number | null, number | null];

export default function App() {
  const [meta, setMeta] = useState<Meta | null>(null);
  const [zones, setZones] = useState<Zones>({});
  const [mode, setMode] = useState<Mode>("history");
  const [speed, setSpeed] = useState(900);
  const [log, setLog] = useState<QueryInfo[]>([]);
  const [volume, setVolume] = useState<[number, number][]>([]);
  const [weather, setWeather] = useState<Weather[]>([]);
  const [surgeZone, setSurgeZone] = useState(186);
  const [bootError, setBootError] = useState<string | null>(null);

  // Newest first; repeated live queries replace their previous entry
  const onQueries = useCallback((qs: QueryInfo[]) => setLog((l) => {
    const names = new Set(qs.filter((q) => q.name.startsWith("live")).map((q) => q.name));
    return [...qs.map((q) => ({ ...q, at: Date.now() })), ...l.filter((q) => !names.has(q.name))].slice(0, 30);
  }), []);

  useEffect(() => {
    api<Meta>("/api/meta").then(setMeta).catch((e: Error) => setBootError(e.message));
    api<Zones>("/api/zones").then(setZones);
  }, []);

  useEffect(() => {
    if (!meta) return;
    const range = `from=${meta.history_from}&to=${meta.history_to}`;
    api<{ data: [number, number][]; queries: QueryInfo[] }>(`/api/volume?${range}&bucket_s=3600`)
      .then((r) => { setVolume(r.data); onQueries(r.queries); }).catch((e: Error) => setBootError(e.message));
    api<{ data: Weather[]; queries: QueryInfo[] }>(`/api/weather?${range}`)
      .then((r) => { setWeather(r.data); onQueries(r.queries); }).catch(() => undefined);
  }, [meta, onQueries]);

  const history = usePlayback({
    enabled: mode === "history" && !!meta,
    from: meta?.history_from ?? 0, to: meta?.history_to ?? 1, speed, onQueries,
  });
  const live = useLive(mode === "live", onQueries);

  const view = mode === "history" ? history : live;
  const error = bootError ?? (mode === "history" ? history.error : live.error);

  return (
    <div className="app">
      <header>
        <h1>NYC Taxi Time Machine</h1>
        <div className="seg mode">
          <button className={mode === "history" ? "on" : ""} onClick={() => setMode("history")}>History · Iceberg</button>
          <button className={mode === "live" ? "on" : ""} onClick={() => { history.setPlaying(false); setMode("live"); }}>
            <span className="dot" /> Live · topic + Iceberg
          </button>
        </div>
        <div className="legend">
          {Object.entries(ORIGIN_COLORS).map(([k, c]) => (
            <span key={k}><i style={{ background: `rgb(${c.join(",")})` }} />{k}</span>
          ))}
        </div>
      </header>

      <main>
        <div className="map">
          <MapView zones={zones} zoneCounts={view.zoneCounts} arcs={view.arcs}
            selectedZone={surgeZone} onZoneClick={setSurgeZone} />
          {error && <div className="toast">{error}</div>}
          {mode === "live" && (
            <div className="live-clock">
              <span className="dot" /> {live.clock ? new Date(live.clock).toISOString().slice(11, 19) : "connecting…"} NYC
              <small>{live.last ? `${live.arcs.length} trips in the last 45 s` : ""}</small>
            </div>
          )}
        </div>
        {mode === "history" && meta && (
          <Timeline from={meta.history_from} to={meta.history_to} playhead={history.playhead}
            volume={volume} weather={weather} playing={history.playing} ready={history.ready} speed={speed}
            onSeek={history.seek} onPlay={history.setPlaying} onSpeed={setSpeed} />
        )}
      </main>

      <aside>
        <SourceCard mode={mode} meta={meta} freshness={live.last?.freshness_ms ?? null} />
        {mode === "live" && <ThenVsNowCard onQueries={onQueries} weeks={meta?.replay_weeks ?? 4} />}
        <SurgeCard zones={zones} zone={surgeZone} setZone={setSurgeZone} onSent={() => setMode("live")} />
        <RainCard onQueries={onQueries} />
        <QualityCard onQueries={onQueries} />
        <SqlLog log={log} />
      </aside>
    </div>
  );
}
