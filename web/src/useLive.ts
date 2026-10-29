import { useEffect, useRef, useState } from "react";
import { Arc, LiveEvent, QueryInfo } from "./api";

const ARC_TTL = 45_000; // live arcs fade over 45 s of real time

/** Live mode: subscribes to the backend's shared SQL poller over SSE. */
export function useLive(enabled: boolean, onQueries: (q: QueryInfo[]) => void) {
  const [last, setLast] = useState<LiveEvent | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [tick, setTick] = useState(0);
  const arcs = useRef<{ arc: Arc; at: number }[]>([]);
  const lastAt = useRef(performance.now());

  useEffect(() => {
    if (!enabled) return;
    const es = new EventSource("/api/live");
    es.onmessage = (m) => {
      const ev: LiveEvent = JSON.parse(m.data);
      if (ev.error) { setError(ev.error); return; }
      setError(null);
      const now = performance.now();
      lastAt.current = now;
      arcs.current = arcs.current.filter((a) => now - a.at < ARC_TTL)
        .concat(ev.new.map((arc) => ({ arc, at: now })));
      setLast(ev);
      onQueries(ev.queries);
    };
    es.onerror = () => setError("live stream disconnected, retrying…");
    return () => es.close();
  }, [enabled, onQueries]);

  // Re-render ~10×/s so the clock runs and arcs fade between events
  useEffect(() => {
    if (!enabled) return;
    const id = setInterval(() => setTick((t) => t + 1), 100);
    return () => clearInterval(id);
  }, [enabled]);

  const now = performance.now();
  const clock = last ? last.now + (now - lastAt.current) : null;
  const visible = arcs.current
    .filter((a) => now - a.at < ARC_TTL)
    .map((a) => ({ arc: a.arc, age: (now - a.at) / ARC_TTL }));
  const zoneCounts = new Map<number, number>(last?.zones ?? []);
  return { last, clock, arcs: visible, zoneCounts, error, tick };
}
