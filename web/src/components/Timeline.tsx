import { useMemo, useRef } from "react";
import { fmtDate, fmtTime, utc } from "../api";
import { PRESETS } from "../usePlayback";

type Props = {
  from: number; to: number; playhead: number;
  volume: [number, number][];
  weather: [number, number | null, number | null, number | null, number | null, number | null][];
  playing: boolean; ready: boolean; speed: number;
  onSeek: (t: number) => void; onPlay: (p: boolean) => void; onSpeed: (s: number) => void;
};

const SPEEDS = [60, 300, 900, 3600];
const W = 1000;

export default function Timeline(p: Props) {
  const svg = useRef<SVGSVGElement>(null);
  const span = p.to - p.from;
  const x = (t: number) => ((t - p.from) / span) * W;

  const area = useMemo(() => {
    if (!p.volume.length) return "";
    const max = Math.max(...p.volume.map((v) => v[1]));
    const pts = p.volume.map(([t, n]) => `${x(t).toFixed(1)},${(40 - (n / max) * 38).toFixed(1)}`);
    return `M${x(p.volume[0][0])},40 L${pts.join(" L")} L${x(p.volume[p.volume.length - 1][0])},40 Z`;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [p.volume, p.from, p.to]);

  const seekFrom = (e: React.PointerEvent) => {
    const r = svg.current!.getBoundingClientRect();
    p.onSeek(p.from + ((e.clientX - r.left) / r.width) * span);
  };

  return (
    <div className="timeline">
      <div className="timeline-controls">
        <button className="primary" onClick={() => p.onPlay(!p.playing)}>{p.playing ? "Pause" : "Play"}</button>
        <div className="seg">
          {SPEEDS.map((s) => (
            <button key={s} className={s === p.speed ? "on" : ""} onClick={() => p.onSpeed(s)}>{s}×</button>
          ))}
        </div>
        <div className="clock">
          <span className="date">{fmtDate(p.playhead)}</span>
          <span className="time">{fmtTime(p.playhead)}</span>
          <span className="tz">NYC</span>
          {p.playing && !p.ready && <span className="buffering">loading from Iceberg…</span>}
        </div>
        <div className="presets">
          {PRESETS.map((pr) => (
            <button key={pr.label} onClick={() => p.onSeek(utc(pr.at))}>{pr.label}</button>
          ))}
        </div>
      </div>
      <svg ref={svg} className="track" viewBox={`0 0 ${W} 60`} preserveAspectRatio="none"
        onPointerDown={(e) => { (e.target as Element).setPointerCapture(e.pointerId); seekFrom(e); }}
        onPointerMove={(e) => e.buttons === 1 && seekFrom(e)}>
        <path d={area} className="volume" />
        {p.weather.map(([t, , precip, snow]) => (
          <rect key={t} x={x(t)} width={W / (span / 3_600_000)} y={44}
            height={Math.min(16, (precip ?? 0) * 4)} className={snow ? "snow" : "rain"} />
        ))}
        <line x1={x(p.playhead)} x2={x(p.playhead)} y1={0} y2={60} className="playhead" />
      </svg>
      <div className="track-legend">
        <span>{fmtDate(p.from)}</span>
        <span><i className="sw vol" /> trips per hour <i className="sw rain" /> rain <i className="sw snow" /> snow</span>
        <span>{fmtDate(p.to - 1)}</span>
      </div>
    </div>
  );
}
