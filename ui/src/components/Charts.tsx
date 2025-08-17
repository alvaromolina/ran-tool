import React from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, Legend, ResponsiveContainer, CartesianGrid,
  BarChart, Bar
} from 'recharts';

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

export const SimpleLineChart: React.FC<{ data: any[]; xKey?: string; height?: number; title?: string; loading?: boolean }>
  = ({ data, xKey, height = 260, title, loading }) => {
  const x = xKey || pickXKey(data) || '';
  const series = numericKeys(data, [x]);
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
        <LineChart data={data} margin={{ top: 8, right: 8, bottom: 8, left: 8 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey={x} minTickGap={24} />
          <YAxis />
          <Tooltip />
          <Legend />
          {series.map((s, i) => (
            <Line key={s} type="monotone" dataKey={s} stroke={["#007aff", "#ff3b30", "#34c759", "#ff9f0a", "#5856d6"][i % 5]} dot={false} strokeWidth={2} />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
};

export const SimpleStackedBar: React.FC<{ data: any[]; xKey?: string; height?: number; title?: string; loading?: boolean }>
  = ({ data, xKey, height = 260, title, loading }) => {
  const x = xKey || pickXKey(data) || '';
  const series = numericKeys(data, [x]);
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
        <BarChart data={data} margin={{ top: 8, right: 8, bottom: 8, left: 8 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey={x} minTickGap={24} />
          <YAxis />
          <Tooltip />
          <Legend />
          {series.map((s, i) => (
            <Bar key={s} dataKey={s} stackId="a" fill={["#0a84ff", "#ff375f", "#32d74b", "#ffd60a", "#5e5ce6"][i % 5]} />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
};
