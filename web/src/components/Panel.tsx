import { useEffect, useState } from "react";
import { api, fmtDate, fmtNum, fmtTime, Meta, QueryInfo, Zones } from "../api";

type Mode = "history" | "live";

/** `via` says how the card gets its data; it shows briefly when the card is hovered. */
function Card({ title, children, note, via }: {
  title: string; children: React.ReactNode; note?: React.ReactNode; via?: string;
}) {
  return (
    <section className="card">
      {via && <span className="via">{via}</span>}
      <h3>{title}</h3>
      {children}
      {note && <p className="note">{note}</p>}
    </section>
  );
}

/** Polls an insight endpoint every `everyMs` and reports its SQL to the log. */
function useInsight<T>(path: string, everyMs: number, onQueries: (q: QueryInfo[]) => void) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    let alive = true;
    const load = () => api<T & { queries: QueryInfo[] }>(path)
      .then((d) => { if (alive) { setData(d); setError(null); onQueries(d.queries); } })
      .catch((e: Error) => alive && setError(e.message));
    load();
    const id = setInterval(load, everyMs);
    return () => { alive = false; clearInterval(id); };
  }, [path, everyMs, onQueries]);
  return { data, error };
}

export function SourceCard({ mode, meta, freshness }: { mode: Mode; meta: Meta | null; freshness: number | null }) {
  const lag = meta?.iceberg_lag_ms ?? 60_000;
  if (mode === "history") {
    return (
      <Card title="Where these rows come from" via="On demand · /api/chunk as the playhead moves, one chunk ahead (cached 30 min)" note={<>Topic retention is 1 h, so 2022-23 rows can only be served from <b>Iceberg</b>. The same table also returns live rows.</>}>
        <div className="big iceberg">Iceberg</div>
        <code className="table">{meta?.tables.trips}</code>
      </Card>
    );
  }
  const inTopic = freshness != null && freshness < lag;
  return (
    <Card title="Freshness" via="SSE · /api/live pushes every ~2 s from one shared SQL poller" note={<>Iceberg commits every ~{lag / 1000}s. Rows newer than that can only be coming from the <b>topic</b>, and the same SQL table returns them with the Iceberg history.</>}>
      <div className={`big ${inTopic ? "topic" : ""}`}>{freshness == null ? "–" : `${(freshness / 1000).toFixed(1)} s`}</div>
      <div className="sub">{freshness == null ? "waiting for live rows…" : inTopic ? "newest row: still in the topic, not in Iceberg yet" : "newest row may already be in Iceberg"}</div>
    </Card>
  );
}

type TvN = { data: { period: string; origin: string; trips: number; avg_total: number; avg_tip_pct: number | null; airport_trips: number }[]; now: number[]; then: number[] };
export function ThenVsNowCard({ onQueries, weeks }: { onQueries: (q: QueryInfo[]) => void; weeks: number }) {
  const { data, error } = useInsight<TvN>("/api/insights/then-vs-now", 15_000, onQueries);
  const sum = (period: string) => {
    const rows = data?.data.filter((r) => r.period === period) ?? [];
    const trips = rows.reduce((a, r) => a + r.trips, 0);
    const avg = (k: "avg_total" | "avg_tip_pct") => trips ? rows.reduce((a, r) => a + (r[k] ?? 0) * r.trips, 0) / trips : null;
    return { trips, total: avg("avg_total"), tip: avg("avg_tip_pct"), airport: rows.reduce((a, r) => a + r.airport_trips, 0) };
  };
  const now = sum("now"), then = sum("then");
  return (
    <Card title="Then vs now: one query" via="Polling · every 15 s (server cache 10 s)" note={<>Last hour (topic + Iceberg) vs the same weekday and hour in the history (Iceberg). “Now” replays {weeks} historical weeks, so expect about {weeks}× the volume.</>}>
      {error ? <p className="error">{error}</p> : (
        <table className="kv">
          <thead><tr><th /><th>then {data && <small>{fmtDate(data.then[0])}</small>}</th><th>now {data && <small>last hour from {fmtTime(data.now[0]).slice(0, 5)}</small>}</th></tr></thead>
          <tbody>
            <tr><td>trips</td><td>{fmtNum(then.trips)}</td><td>{fmtNum(now.trips)}</td></tr>
            <tr><td>avg total</td><td>${fmtNum(then.total, 2)}</td><td>${fmtNum(now.total, 2)}</td></tr>
            <tr><td>avg tip</td><td>{fmtNum(then.tip == null ? null : then.tip * 100, 1)}%</td><td>{fmtNum(now.tip == null ? null : now.tip * 100, 1)}%</td></tr>
            <tr><td>airport trips</td><td>{fmtNum(then.airport)}</td><td>{fmtNum(now.airport)}</td></tr>
          </tbody>
        </table>
      )}
    </Card>
  );
}

type Rain = { data: { sky: string; trips: number; hours: number; avg_tip_pct: number | null; avg_duration: number; avg_total: number }[] };
export function RainCard({ onQueries }: { onQueries: (q: QueryInfo[]) => void }) {
  const { data, error } = useInsight<Rain>("/api/insights/rain", 600_000, onQueries);
  const order = ["dry", "rain", "snow"];
  const rows = [...(data?.data ?? [])].sort((a, b) => order.indexOf(a.sky) - order.indexOf(b.sky));
  return (
    <Card title="Weather vs demand" via="Polling · every 10 min (server cache 10 min)" note={<><code>trips_enriched ⋈ weather_hourly</code> on the pickup hour, both Iceberg-backed topics.</>}>
      {error ? <p className="error">{error}</p> : !data ? <p className="sub">running join…</p> : (
        <table className="kv">
          <thead><tr><th /><th>trips/h</th><th>tip</th><th>ride</th></tr></thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.sky}>
                <td>{r.sky}</td>
                <td>{fmtNum(r.trips / Math.max(1, r.hours))}</td>
                <td>{fmtNum(r.avg_tip_pct == null ? null : r.avg_tip_pct * 100, 1)}%</td>
                <td>{fmtNum(r.avg_duration, 1)} min</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Card>
  );
}

type Quality = { data: { reason: string; origin: string; trips: number }[] };
export function QualityCard({ onQueries }: { onQueries: (q: QueryInfo[]) => void }) {
  const { data, error } = useInsight<Quality>("/api/insights/quality", 30_000, onQueries);
  const byReason = new Map<string, number>();
  for (const r of data?.data ?? []) byReason.set(r.reason, (byReason.get(r.reason) ?? 0) + r.trips);
  const total = [...byReason.values()].reduce((a, b) => a + b, 0);
  return (
    <Card title="Quarantined by Connect" via="Polling · every 30 s (server cache 30 s)" note={<>The <code>trips-enrich</code> pipeline routes rows that fail a quality rule to <code>trips_quarantine</code>.</>}>
      {error ? <p className="error">{error}</p> : (
        <>
          <div className="big">{fmtNum(total)}</div>
          <ul className="bars">
            {[...byReason.entries()].map(([reason, n]) => (
              <li key={reason}><span>{reason}</span><i style={{ width: `${(n / Math.max(1, total)) * 100}%` }} /><b>{fmtNum(n)}</b></li>
            ))}
          </ul>
        </>
      )}
    </Card>
  );
}

const SURGE_ZONES = [186, 230, 161, 132, 138, 79, 255, 48];
export function SurgeCard({ zones, zone, setZone, onSent }: {
  zones: Zones; zone: number; setZone: (z: number) => void; onSent: () => void;
}) {
  const [count, setCount] = useState(200);
  const [status, setStatus] = useState<string | null>(null);
  const fire = async () => {
    setStatus("producing…");
    try {
      const r = await api<{ sent: number; topic: string }>("/api/surge", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ zone_id: zone, count }),
      });
      setStatus(`${r.sent} trips → ${r.topic} → trips-enrich → trips_enriched`);
      onSent();
    } catch (e) {
      setStatus((e as Error).message);
    }
  };
  const options = Array.from(new Set([zone, ...SURGE_ZONES]));
  return (
    <Card title="Surge" via="On click · POST /api/surge produces to trips_raw" note="Produces trips into trips_raw; Connect enriches them and SQL picks them up within seconds. Tip: click a zone on the map.">
      <div className="row">
        <select value={zone} onChange={(e) => setZone(Number(e.target.value))}>
          {options.map((z) => <option key={z} value={z}>{zones[z]?.zone ?? z}</option>)}
        </select>
        <input type="number" min={1} max={1000} value={count} onChange={(e) => setCount(Number(e.target.value))} />
        <button className="danger" onClick={fire}>Surge</button>
      </div>
      {status && <p className="sub">{status}</p>}
    </Card>
  );
}

export function SqlLog({ log }: { log: QueryInfo[] }) {
  return (
    <Card title="SQL log" via="Collected from every response above">
      <ol className="sqllog">
        {log.map((q, i) => (
          <li key={`${q.at}-${i}`}>
            <details>
              <summary><span className="qname">{q.name}</span><span className="qms">{q.cached ? "cached" : `${fmtNum(q.ms)} ms`} · {fmtNum(q.rows)} rows</span></summary>
              <pre>{q.sql}</pre>
            </details>
          </li>
        ))}
      </ol>
    </Card>
  );
}
