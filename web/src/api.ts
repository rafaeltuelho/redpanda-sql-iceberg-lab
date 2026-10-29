// Times are epoch ms of NYC wall clock stored as UTC (DESIGN.md §5): always format in UTC.
export type QueryInfo = { name: string; sql: string; ms: number; rows: number; at?: number };
export type Meta = {
  history_from: number; history_to: number; ny_now: number;
  iceberg_lag_ms: number; replay_weeks: number; tables: Record<string, string>;
};
export type Zone = { zone: string; borough: string; lon: number | null; lat: number | null };
export type Zones = Record<string, Zone>;
export type Origin = "history" | "replay" | "surge";
/** [pickup_ms, dropoff_ms, pickup_zone, dropoff_zone, total_amount, origin] */
export type Arc = [number, number, number, number, number, Origin];
/** [bucket_ms, zone, trips, avg_total] */
export type Frame = [number, number, number, number];
export type Chunk = { frames: Frame[]; arcs: Arc[]; queries: QueryInfo[] };
export type LiveEvent = {
  now: number; new: Arc[]; zones: [number, number][]; freshness_ms: number | null;
  iceberg_lag_ms: number; queries: QueryInfo[]; error?: string;
};

export async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, init);
  if (!res.ok) {
    let detail = res.statusText;
    try { detail = (await res.json()).detail ?? detail; } catch { /* not JSON */ }
    throw new Error(detail);
  }
  return res.json() as Promise<T>;
}

export const MIN = 60_000;
export const HOUR = 60 * MIN;

const dateFmt = new Intl.DateTimeFormat("en-US", {
  timeZone: "UTC", weekday: "short", month: "short", day: "numeric", year: "numeric",
});
const timeFmt = new Intl.DateTimeFormat("en-US", {
  timeZone: "UTC", hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false,
});
export const fmtDate = (ms: number) => dateFmt.format(ms);
export const fmtTime = (ms: number) => timeFmt.format(ms);
export const fmtNum = (n: number | null | undefined, digits = 0) =>
  n == null ? "–" : n.toLocaleString("en-US", { maximumFractionDigits: digits, minimumFractionDigits: digits });
export const utc = (iso: string) => Date.parse(iso + "Z");

export const ORIGIN_COLORS: Record<Origin, [number, number, number]> = {
  history: [64, 196, 190],
  replay: [255, 176, 59],
  surge: [255, 72, 72],
};
