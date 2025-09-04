export type Technology = '3G' | '4G' | '5G';

const BASE_URL = (import.meta as any).env.VITE_API_BASE_URL || 'http://localhost:8000';

async function http<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

async function httpPost<T>(path: string, body: any): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export const api = {
  health: () => http<{ status: string }>(`/api/health`),
  searchSites: (q: string, limit = 10) => {
    const params = new URLSearchParams();
    params.set('q', q);
    params.set('limit', String(limit));
    return http<string[]>(`/api/sites/search?${params.toString()}`);
  },
  neighborsList: (site: string, params?: { radius_km?: number, vecinos?: string }) => {
    const q = new URLSearchParams();
    if (params?.radius_km != null) q.set('radius_km', String(params.radius_km));
    if (params?.vecinos != null) q.set('vecinos', String(params.vecinos.replaceAll("\n",",").replaceAll(" ",",").replaceAll(",,",",")));
    const qs = q.toString();
    return http<Array<{ site_name: string; region: string | null; province: string | null; municipality: string | null; vendor: string | null }>>(
      `/api/sites/${encodeURIComponent(site)}/neighbors/list${qs ? `?${qs}` : ''}`
    );
  },
  ranges: (site: string) => http<{ site_att: string; input_date: string | null; max_date: string | null }>(`/api/sites/${encodeURIComponent(site)}/ranges`),
  cellChanges: (site: string, params?: { group_by?: 'network' | 'region' | 'province' | 'municipality'; technologies?: string[]; vendors?: string[] }) => {
    const q = new URLSearchParams();
    if (params?.group_by) q.set('group_by', params.group_by);
    params?.technologies?.forEach(t => q.append('technologies', t));
    params?.vendors?.forEach(v => q.append('vendors', v));
    const qs = q.toString();
    return http<any[]>(`/api/sites/${encodeURIComponent(site)}/cell-changes${qs ? `?${qs}` : ''}`);
  },
  cqi: (site: string, params?: { from_date?: string; to_date?: string; technology?: Technology }) => {
    const q = new URLSearchParams();
    if (params?.from_date) q.set('from_date', params.from_date);
    if (params?.to_date) q.set('to_date', params.to_date);
    if (params?.technology) q.set('technology', params.technology);
    const qs = q.toString();
    return http<any[]>(`/api/sites/${encodeURIComponent(site)}/cqi${qs ? `?${qs}` : ''}`);
  },
  traffic: (site: string, params?: { from_date?: string; to_date?: string; technology?: Technology; vendor?: string }) => {
    const q = new URLSearchParams();
    if (params?.from_date) q.set('from_date', params.from_date);
    if (params?.to_date) q.set('to_date', params.to_date);
    if (params?.technology) q.set('technology', params.technology);
    if (params?.vendor) q.set('vendor', params.vendor);
    const qs = q.toString();
    return http<any[]>(`/api/sites/${encodeURIComponent(site)}/traffic${qs ? `?${qs}` : ''}`);
  },
  voice: (site: string, params?: { from_date?: string; to_date?: string; technology?: Exclude<Technology, '5G'>; vendor?: string }) => {
    const q = new URLSearchParams();
    if (params?.from_date) q.set('from_date', params.from_date);
    if (params?.to_date) q.set('to_date', params.to_date);
    if (params?.technology) q.set('technology', params.technology);
    if (params?.vendor) q.set('vendor', params.vendor);
    const qs = q.toString();
    return http<any[]>(`/api/sites/${encodeURIComponent(site)}/traffic/voice${qs ? `?${qs}` : ''}`);
  },
  neighbors: (site: string, params?: { radius_km?: number }) => {
    const q = new URLSearchParams();
    if (params?.radius_km != null) q.set('radius_km', String(params.radius_km));
    const qs = q.toString();
    return http<{ site_att: string; radius_km: number; neighbors: string[] }>(`/api/sites/${encodeURIComponent(site)}/neighbors${qs ? `?${qs}` : ''}`);
  },
  neighborsGeo: (site: string, params?: { radius_km?: number }) => {
    const q = new URLSearchParams();
    if (params?.radius_km != null) q.set('radius_km', String(params.radius_km));
    const qs = q.toString();
    return http<Array<{ role: 'center' | 'neighbor'; att_name: string; latitude: number; longitude: number }>>(`/api/sites/${encodeURIComponent(site)}/neighbors/geo${qs ? `?${qs}` : ''}`);
  },
  neighborsCqi: (site: string, params?: { from_date?: string; to_date?: string; technology?: Technology; radius_km?: number }) => {
    const q = new URLSearchParams();
    if (params?.from_date) q.set('from_date', params.from_date);
    if (params?.to_date) q.set('to_date', params.to_date);
    if (params?.technology) q.set('technology', params.technology);
    if (params?.radius_km != null) q.set('radius_km', String(params.radius_km));
    const qs = q.toString();
    return http<any[]>(`/api/sites/${encodeURIComponent(site)}/neighbors/cqi${qs ? `?${qs}` : ''}`);
  },
  neighborsTraffic: (site: string, params?: { from_date?: string; to_date?: string; technology?: Technology; vendor?: string; radius_km?: number }) => {
    const q = new URLSearchParams();
    if (params?.from_date) q.set('from_date', params.from_date);
    if (params?.to_date) q.set('to_date', params.to_date);
    if (params?.technology) q.set('technology', params.technology);
    if (params?.vendor) q.set('vendor', params.vendor);
    if (params?.radius_km != null) q.set('radius_km', String(params.radius_km));
    const qs = q.toString();
    return http<any[]>(`/api/sites/${encodeURIComponent(site)}/neighbors/traffic${qs ? `?${qs}` : ''}`);
  },
  neighborsVoice: (site: string, params?: { from_date?: string; to_date?: string; technology?: Exclude<Technology, '5G'>; vendor?: string; radius_km?: number }) => {
    const q = new URLSearchParams();
    if (params?.from_date) q.set('from_date', params.from_date);
    if (params?.to_date) q.set('to_date', params.to_date);
    if (params?.technology) q.set('technology', params.technology);
    if (params?.vendor) q.set('vendor', params.vendor);
    if (params?.radius_km != null) q.set('radius_km', String(params.radius_km));
    const qs = q.toString();
    return http<any[]>(`/api/sites/${encodeURIComponent(site)}/neighbors/traffic/voice${qs ? `?${qs}` : ''}`);
  },
  eventDates: (site: string, params?: { limit?: number; offset?: number }) => {
    const q = new URLSearchParams();
    if (params?.limit != null) q.set('limit', String(params.limit));
    if (params?.offset != null) q.set('offset', String(params.offset));
    const qs = q.toString();
    return http<Array<{ tech: '3G'|'4G'; date: string; add_cell: number|null; delete_cell: number|null; total_cell: number|null; remark: string|null }>>(
      `/api/sites/${encodeURIComponent(site)}/event-dates${qs ? `?${qs}` : ''}`
    );
  },
  evaluate: (args: { site_att: string; input_date: string; threshold?: number; period?: number; guard?: number; radius_km?: number ; vecinos?: string}) => {
    args.vecinos = args.vecinos.replaceAll("\n",",").replaceAll(" ",",").replaceAll(",,",",");
    return httpPost<{ site_att: string; input_date: string; options: any; overall: 'Pass'|'Fail'|'Restored'|'Inconclusive'|null; metrics: Array<any>; data?: any }>(
      `/api/evaluate`,
      { ...args, debug: true },
    )
    },
  reportPdf: async (args: { site_att: string; input_date: string; threshold?: number; period?: number; guard?: number; radius_km?: number; include_debug?: boolean }): Promise<Blob> => {
    const res = await fetch(`${BASE_URL}/api/report`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        site_att: args.site_att,
        input_date: args.input_date,
        threshold: args.threshold,
        period: args.period,
        guard: args.guard,
        radius_km: args.radius_km,
        debug: false,
        include_debug: !!args.include_debug,
      }),
    });
    if (!res.ok) {
      // Try to parse JSON error from server
      try {
        const j = await res.json();
        throw new Error(j?.error || `HTTP ${res.status}`);
      } catch {
        throw new Error(`HTTP ${res.status}`);
      }
    }
    return res.blob();
  },
};
