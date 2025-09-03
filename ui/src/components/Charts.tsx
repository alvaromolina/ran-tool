import React from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, Legend, ResponsiveContainer, CartesianGrid,
  BarChart, Bar, ReferenceLine, ReferenceArea
} from 'recharts';

function colorForSeries(key: string, idx: number): string {
  const k = (key || '').toLowerCase();
  // 3G / UMTS
  if (k.includes('3g') || k.includes('umts')) return '#ff3b30';
  // 4G / LTE / VoLTE
  if (k.includes('4g') || k.includes('lte') || k.includes('volte')) return '#34c759';
  // 5G / NR
  if (k.includes('5g') || k.includes('nr')) return '#007aff';
  // Fallback palette
  const fallback = ["#007aff", "#ff3b30", "#34c759", "#ff9f0a", "#5856d6"];
  return fallback[idx % fallback.length];
}

function pickXKey(rows: any[]): string | null {
  if (!rows || rows.length === 0) return null;
  const candidateKeys = ['date', 'time', 'day', 'timestamp'];
  const row = rows[0];
  for (const k of candidateKeys) {
    if (k in row) return k;
  }
  // fallback: first string key
  const keys = Object.keys(row);
  const s = keys.find(k => typeof row[k] === 'string');
  return s || null;
}

function numericKeys(rows: any[], exclude: string[]): string[] {
  if (!rows || rows.length === 0) return [];
  const keys = Object.keys(rows[0]);
  // choose columns that are numbers on at least one row
  const nums = keys.filter(k => !exclude.includes(k) && rows.some(r => typeof r[k] === 'number'));
  return nums.slice(0, 5); // limit to avoid clutter
}

type VLine = { x: string; stroke?: string; strokeDasharray?: string; strokeWidth?: number; label?: string };
type Region = { from: string; to: string; stroke?: string; strokeDasharray?: string; strokeWidth?: number; fill?: string; fillOpacity?: number };

function withinWindow(rows: any[], xKey: string, min?: string, max?: string): any[] {
  if (!rows || rows.length === 0) return rows;
  if (!min && !max) return rows;
  const minT = min ? Date.parse(min) : Number.NEGATIVE_INFINITY;
  const maxT = max ? Date.parse(max) : Number.POSITIVE_INFINITY;
  return rows.filter(r => {
    const v = r?.[xKey];
    const t = typeof v === 'string' ? Date.parse(v) : (typeof v === 'number' ? v : NaN);
    if (Number.isNaN(t)) return true;
    return t >= minT && t <= maxT;
  });
}

export const SimpleLineChart: React.FC<{ data: any[]; xKey?: string; height?: number; title?: string; loading?: boolean; xMin?: string; xMax?: string; vLines?: VLine[]; regions?: Region[] }>
  = ({ data, xKey, height = 260, title, loading, xMin, xMax, vLines = [], regions = [] }) => {
  const x = xKey || pickXKey(data) || '';
  const series = numericKeys(data, [x]);
  const wdata = withinWindow(data, x, xMin, xMax);
  // Build a sorted list of available x values in-window for snapping
  const xvals = (wdata || []).map(r => r?.[x]).filter(Boolean) as string[];
  const toT = (s?: string) => (s ? Date.parse(s) : NaN);
  const uniqSorted = Array.from(new Set(xvals)).sort();
  function snapToRange(from?: string, to?: string): {x1?: string; x2?: string} {
    if (!uniqSorted.length) return {};
    const minX = uniqSorted[0];
    const maxX = uniqSorted[uniqSorted.length - 1];
    const fT = toT(from); const tT = toT(to);
    let x1: string | undefined; let x2: string | undefined;
    if (!Number.isNaN(fT)) {
      // if window starts after all data, clamp to last x; if before all, clamp to first matching >=
      if (fT > toT(maxX)) {
        x1 = maxX;
      } else {
        x1 = uniqSorted.find(s => toT(s) >= fT) || minX;
      }
    }
    if (!Number.isNaN(tT)) {
      // if window ends before all data, clamp to first x; else pick last <= to
      if (tT < toT(minX)) {
        x2 = minX;
      } else {
        for (let i = uniqSorted.length - 1; i >= 0; i--) {
          if (toT(uniqSorted[i]) <= tT) { x2 = uniqSorted[i]; break; }
        }
        x2 = x2 || maxX;
      }
    }
    // If clamped outside range, ensure x1 <= x2 by collapsing to nearest edge
    if (x1 && x2 && toT(x1) > toT(x2)) {
      if (toT(x1) > toT(maxX)) { x1 = maxX; x2 = maxX; }
      else if (toT(x2) < toT(minX)) { x1 = minX; x2 = minX; }
      else { x1 = x2; }
    }
    return { x1, x2 };
  }
  function snapX(xv?: string): string | undefined {
    if (!xv || !uniqSorted.length) return xv;
    const target = toT(xv);
    if (Number.isNaN(target)) return xv;
    let best = uniqSorted[0];
    let bestDiff = Math.abs(toT(best) - target);
    for (const s of uniqSorted) {
      const d = Math.abs(toT(s) - target);
      if (d < bestDiff) { best = s; bestDiff = d; }
    }
    return best;
  }
  const snappedRegions = regions.map(r => {
    const { x1, x2 } = snapToRange(r.from, r.to);
    let from = x1 || r.from; let to = x2 || r.to;
    // If region collapsed to a single tick, expand to adjacent tick so it's visible
    if (from && to && from === to && uniqSorted.length > 1) {
      const idx = Math.max(0, uniqSorted.indexOf(from));
      if (idx < uniqSorted.length - 1) {
        to = uniqSorted[idx + 1];
      } else {
        // last tick: expand backwards
        if (idx > 0) from = uniqSorted[idx - 1];
      }
    }
    return { ...r, from, to };
  });
  const snappedVLines = vLines.map(l => ({ ...l, x: snapX(l.x) || l.x }));
  if (loading) return (
    <div className="chart-block">
      {title && <div className="chart-title">{title}</div>}
      <div className="chart-loading" style={{ height }} />
    </div>
  );
  if (!x || series.length === 0) return (
    <div className="chart-block">
      <div className="chart-empty">No numeric series to chart. Choose a valid site and date.</div>
    </div>
  );
  return (
    <div className="chart-block">
      {title && <div className="chart-title">{title}</div>}
      <ResponsiveContainer width="100%" height={height}>
        <LineChart data={wdata} margin={{ top: 8, right: 8, bottom: 8, left: 8 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey={x} minTickGap={24} />
          <YAxis />
          <Tooltip />
          <Legend />
          {snappedRegions.map((r, idx) => (
            <ReferenceArea key={`reg-${idx}`} x1={r.from} x2={r.to} stroke={r.stroke || '#0a84ff'} strokeDasharray={r.strokeDasharray || '6 6'} strokeWidth={r.strokeWidth} fill={r.fill || '#0a84ff'} fillOpacity={r.fillOpacity ?? 0.06} />
          ))}
          {snappedVLines.map((l, idx) => (
            <ReferenceLine key={`vl-${idx}`} x={l.x} stroke={l.stroke || '#111'} strokeDasharray={l.strokeDasharray || '6 6'} strokeWidth={l.strokeWidth || 3} label={l.label} />
          ))}
          {series.map((s, i) => (
            <Line key={s} type="monotone" dataKey={s} stroke={colorForSeries(s, i)} dot={false} strokeWidth={2} />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
};

export const SimpleStackedBar: React.FC<{ data: any[]; xKey?: string; height?: number; title?: string; loading?: boolean; xMin?: string; xMax?: string; vLines?: VLine[]; regions?: Region[] }>
  = ({ data, xKey, height = 260, title, loading, xMin, xMax, vLines = [], regions = [] }) => {
  const x = xKey || pickXKey(data) || '';
  const series = numericKeys(data, [x]);
  const wdata = withinWindow(data, x, xMin, xMax);
  const xvals = (wdata || []).map(r => r?.[x]).filter(Boolean) as string[];
  const toT = (s?: string) => (s ? Date.parse(s) : NaN);
  const uniqSorted = Array.from(new Set(xvals)).sort();
  function snapToRange(from?: string, to?: string): {x1?: string; x2?: string} {
    if (!uniqSorted.length) return {};
    const minX = uniqSorted[0];
    const maxX = uniqSorted[uniqSorted.length - 1];
    const fT = toT(from); const tT = toT(to);
    let x1: string | undefined; let x2: string | undefined;
    if (!Number.isNaN(fT)) {
      if (fT > toT(maxX)) {
        x1 = maxX;
      } else {
        x1 = uniqSorted.find(s => toT(s) >= fT) || minX;
      }
    }
    if (!Number.isNaN(tT)) {
      if (tT < toT(minX)) {
        x2 = minX;
      } else {
        for (let i = uniqSorted.length - 1; i >= 0; i--) {
          if (toT(uniqSorted[i]) <= tT) { x2 = uniqSorted[i]; break; }
        }
        x2 = x2 || maxX;
      }
    }
    if (x1 && x2 && toT(x1) > toT(x2)) {
      if (toT(x1) > toT(maxX)) { x1 = maxX; x2 = maxX; }
      else if (toT(x2) < toT(minX)) { x1 = minX; x2 = minX; }
      else { x1 = x2; }
    }
    return { x1, x2 };
  }
  function snapX(xv?: string): string | undefined {
    if (!xv || !uniqSorted.length) return xv;
    const target = toT(xv);
    if (Number.isNaN(target)) return xv;
    let best = uniqSorted[0];
    let bestDiff = Math.abs(toT(best) - target);
    for (const s of uniqSorted) {
      const d = Math.abs(toT(s) - target);
      if (d < bestDiff) { best = s; bestDiff = d; }
    }
    return best;
  }
  const snappedRegions = regions.map(r => {
    const { x1, x2 } = snapToRange(r.from, r.to);
    let from = x1 || r.from; let to = x2 || r.to;
    if (from && to && from === to && uniqSorted.length > 1) {
      const idx = Math.max(0, uniqSorted.indexOf(from));
      if (idx < uniqSorted.length - 1) {
        to = uniqSorted[idx + 1];
      } else {
        if (idx > 0) from = uniqSorted[idx - 1];
      }
    }
    return { ...r, from, to };
  });
  const snappedVLines = vLines.map(l => ({ ...l, x: snapX(l.x) || l.x }));
  if (loading) return (
    <div className="chart-block">
      {title && <div className="chart-title">{title}</div>}
      <div className="chart-loading" style={{ height }} />
    </div>
  );
  if (!x || series.length === 0) return (
    <div className="chart-block">
      <div className="chart-empty">No numeric series to chart. Choose a valid site and date.</div>
    </div>
  );
  return (
    <div className="chart-block">
      {title && <div className="chart-title">{title}</div>}
      <ResponsiveContainer width="100%" height={height}>
        <BarChart data={wdata} margin={{ top: 8, right: 8, bottom: 8, left: 8 }} barCategoryGap="0%" barGap={0}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey={x} minTickGap={24} />
          <YAxis />
          <Tooltip />
          <Legend />
          {series.map((s, i) => (
            <Bar key={s} dataKey={s} stackId="a" fill={colorForSeries(s, i)} stroke="none" />
          ))}
          {snappedRegions.map((r, idx) => (
            <ReferenceArea
              key={`reg-${idx}`}
              x1={r.from}
              x2={r.to}
              stroke={r.stroke || '#0a84ff'}
              strokeDasharray={r.strokeDasharray || '6 6'}
              strokeWidth={r.strokeWidth || 2}
              fill={r.fill || '#0a84ff'}
              fillOpacity={r.fillOpacity ?? 0.06}
            />
          ))}
          {snappedVLines.map((l, idx) => (
            <ReferenceLine key={`vl-${idx}`} x={l.x} stroke={l.stroke || '#111'} strokeDasharray={l.strokeDasharray || '6 6'} strokeWidth={l.strokeWidth || 3} label={l.label} />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
;
