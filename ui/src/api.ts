export type Technology = '3G' | '4G' | '5G';

const BASE_URL = (import.meta as any).env.VITE_API_BASE_URL || 'http://localhost:8000';

async function http<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`);
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
};
