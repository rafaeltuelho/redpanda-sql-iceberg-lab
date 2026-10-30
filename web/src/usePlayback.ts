import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { api, ArcView, Chunk, HOUR, MIN, QueryInfo } from "./api";

const ZONE_WINDOW = 15 * MIN;   // map colors = pickups in the last 15 history-minutes

/** History playback: advances the playhead at `speed`× and loads data in aligned chunks,
 *  one chunk ahead, each one SQL round trip per query (frames + arcs). */
export function usePlayback(opts: {
  enabled: boolean; from: number; to: number; speed: number;
  onQueries: (q: QueryInfo[]) => void;
}) {
  const { enabled, from, to, speed, onQueries } = opts;
  const [playhead, setPlayhead] = useState(Date.parse("2022-12-31T18:00:00Z"));
  const [playing, setPlaying] = useState(false);
  const [version, setVersion] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const chunks = useRef(new Map<number, Chunk | "loading">());
  const playheadRef = useRef(playhead);
  playheadRef.current = playhead;

  // Bigger chunks at higher speeds: ~4 s of playback each, at least 30 min of history
  const chunkMs = Math.max(30 * MIN, Math.ceil((speed * 4000) / (15 * MIN)) * 15 * MIN);
  const sample = Math.max(1, Math.round(chunkMs / (30 * MIN)));
  useEffect(() => { chunks.current.clear(); setVersion((v) => v + 1); }, [chunkMs]);

  const keyOf = useCallback((t: number) => Math.floor(t / chunkMs) * chunkMs, [chunkMs]);

  const ensure = useCallback((key: number) => {
    if (key < from - chunkMs || key >= to || chunks.current.has(key)) return;
    chunks.current.set(key, "loading");
    api<Chunk>(`/api/chunk?from=${key}&to=${key + chunkMs}&bucket_s=60&sample=${sample}`)
      .then((c) => { chunks.current.set(key, c); onQueries(c.queries); setError(null); })
      .catch((e: Error) => { chunks.current.delete(key); setError(e.message); setPlaying(false); })
      .finally(() => setVersion((v) => v + 1));
  }, [chunkMs, from, to, sample, onQueries]);

  const current = keyOf(playhead);
  // Debounced so dragging across the timeline only loads the chunks where it stops
  useEffect(() => {
    if (!enabled) return;
    const id = setTimeout(() => {
      ensure(current - chunkMs);   // rolling windows reach into the previous chunk
      ensure(current);
      ensure(current + chunkMs);
    }, 150);
    return () => clearTimeout(id);
  }, [enabled, current, chunkMs, ensure, version]);

  const ready = typeof chunks.current.get(current) === "object";

  // Animation loop: advance only while the current chunk is loaded (buffering otherwise)
  useEffect(() => {
    if (!enabled || !playing) return;
    let raf = 0;
    let last = performance.now();
    const step = (now: number) => {
      const dt = now - last;
      last = now;
      if (typeof chunks.current.get(keyOf(playheadRef.current)) === "object") {
        setPlayhead((p) => {
          const next = p + dt * speed;
          if (next >= to) { setPlaying(false); return to; }
          return next;
        });
      }
      raf = requestAnimationFrame(step);
    };
    raf = requestAnimationFrame(step);
    return () => cancelAnimationFrame(raf);
  }, [enabled, playing, speed, to, keyOf]);
  const minute = Math.floor(playhead / MIN);
  const zoneCounts = useMemo(() => {
    const counts = new Map<number, number>();
    const lo = playhead - ZONE_WINDOW;
    for (const key of [current - chunkMs, current]) {
      const c = chunks.current.get(key);
      if (typeof c !== "object") continue;
      for (const [t, zone, n] of c.frames) {
        if (t > lo && t <= playhead) counts.set(zone, (counts.get(zone) ?? 0) + n);
      }
    }
    return counts;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [minute, version, current, chunkMs]);

  // Each arc grows from pickup to dropoff over the trip's own duration, then fades out over
  // ~1.2 s of real time
  const fadeWindow = Math.min(30 * MIN, Math.max(2 * MIN, speed * 1200));
  const arcs = useMemo(() => {
    const out: ArcView[] = [];
    for (const key of [current - chunkMs, current]) {
      const c = chunks.current.get(key);
      if (typeof c !== "object") continue;
      for (const a of c.arcs) {
        const [pickup, dropoff] = a;
        if (pickup > playhead || playhead >= dropoff + fadeWindow) continue;
        out.push({
          arc: a,
          progress: dropoff > pickup ? Math.min(1, (playhead - pickup) / (dropoff - pickup)) : 1,
          age: Math.max(0, (playhead - dropoff) / fadeWindow),
        });
      }
    }
    return out;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [playhead, version, current, chunkMs, fadeWindow]);

  const seek = useCallback((t: number) => setPlayhead(Math.min(to, Math.max(from, t))), [from, to]);

  return { playhead, seek, playing, setPlaying, ready, zoneCounts, arcs, error, chunkMs, sample };
}

export const PRESETS: { label: string; at: string }[] = [
  { label: "New Year's Eve", at: "2022-12-31T18:00:00" },
  { label: "Pre-Christmas storm", at: "2022-12-23T06:00:00" },
  { label: "Rainiest day", at: "2023-01-19T06:00:00" },
  { label: "Morning rush", at: "2023-01-10T06:30:00" },
  { label: "Snow night", at: "2023-02-27T20:00:00" },
];
export { HOUR };
