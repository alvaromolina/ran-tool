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
  // Build union of keys across all rows to avoid missing series that start later (e.g., 5G CQI with no 'before').
  const keySet = new Set<string>();
  for (const r of rows) {
    for (const k of Object.keys(r)) keySet.add(k);
  }
  const keys = Array.from(keySet);
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

export const SimpleLineChart: React.FC<{ data: any[]; xKey?: string; height?: number; title?: string; loading?: boolean; xMin?: string; xMax?: string; vLines?: VLine[]; regions?: Region[]; missingAsZero?: boolean }>
  = ({ data, xKey, height = 260, title, loading, xMin, xMax, vLines = [], regions = [], missingAsZero = false }) => {
  const x = xKey || pickXKey(data) || '';
  const series = numericKeys(data, [x]);
  // Expand xMax to at least the day after the furthest region 'to' so ReferenceArea covers the last day fully
  const plus1 = (s?: string) => (s ? new Date(Date.parse(s) + 24*3600*1000).toISOString().slice(0,10) : undefined);
  const regionMaxToPlus1 = regions
    .map(r => plus1(r.to))
    .filter(Boolean)
    .sort()
    .slice(-1)[0] as string | undefined;
  const effXMax = [xMax, regionMaxToPlus1].filter(Boolean).sort().slice(-1)[0] as string | undefined;
  const wdataBase = withinWindow(data, x, xMin, effXMax);
  // Build a daily UTC x-grid from xMin..xMax (or data bounds) and merge rows to ensure consistent buckets
  function gridDailyUTC(rows: any[], xk: string, cols: string[], min?: string, max?: string): any[] {
    if (!rows || rows.length === 0) return rows;
    const parseUTC = (s: string) => new Date(`${s}T00:00:00Z`).getTime();
    const fmtUTC = (t: number) => new Date(t).toISOString().slice(0, 10);
    const map = new Map<string, any>();
    for (const r of rows) { const key = r?.[xk]; if (key) map.set(String(key), r); }
    const existing = Array.from(map.keys()).sort();
    // Do NOT start grid before first available data point to avoid leading empty space
    const startStr = (min && min > existing[0]) ? min : existing[0];
    const endStr = max || existing[existing.length - 1];
    if (!startStr || !endStr) return rows;
    const DAY = 24 * 3600 * 1000;
    let t = parseUTC(startStr);
    const endT = parseUTC(endStr);
    const out: any[] = [];
    while (t <= endT) {
      const key = fmtUTC(t);
      const row = map.get(key) || { [xk]: key };
      for (const c of cols) if (!(c in row)) row[c] = (missingAsZero ? 0 : null);
      out.push(row);
      t += DAY;
    }
    return out;
  }
  const wdata = gridDailyUTC(wdataBase, x, series, xMin, effXMax);
  // Snap regions and vLines to the nearest available x-values to ensure the shaded window covers exact data points.
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
    // If both ends snap to the same x, widen by one bucket like the bar chart
    if (from && to && from === to && uniqSorted.length > 1) {
      const idx = Math.max(0, uniqSorted.indexOf(from));
      if (idx < uniqSorted.length - 1) {
        to = uniqSorted[idx + 1];
      } else if (idx > 0) {
        from = uniqSorted[idx - 1];
      }
    }
    // For LineChart on categorical X, ReferenceArea's x2 is the RIGHT boundary.
    // To cover days [from..to] inclusive, set x2 to the boundary of the NEXT day after original r.to.
    if (r.to) {
      const nextBoundary = new Date(Date.parse(r.to) + 24*3600*1000).toISOString().slice(0,10);
      const snappedBoundary = snapX(nextBoundary);
      if (snappedBoundary) to = snappedBoundary;
    }
    if (r.from) {
      const snappedFrom = snapX(r.from);
      if (snappedFrom) from = snappedFrom;
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
          <XAxis
            dataKey={x}
            type="category"
            minTickGap={24}
            allowDuplicatedCategory={false}
            allowDataOverflow={true}
            padding={{ left: 0, right: 0 }}
          />
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
            <Line key={s} type="monotone" dataKey={s} stroke={colorForSeries(s, i)} dot={false} strokeWidth={2} connectNulls={false} />
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
