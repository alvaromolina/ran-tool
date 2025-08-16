import { useEffect, useMemo, useState } from 'react'
import './App.css'
import { api } from './api'
import { MapContainer, TileLayer, CircleMarker, Popup, Circle } from 'react-leaflet'
import { useRef, useEffect as useEffectReact } from 'react'

function App() {
  const [health, setHealth] = useState<string>('checking...')
  const [site, setSite] = useState<string>('TESTSITE')
  const [siteSuggestions, setSiteSuggestions] = useState<string[]>([])
  const [siteLoading, setSiteLoading] = useState(false)
  const [radiusKm, setRadiusKm] = useState<number>(5)
  const [result, setResult] = useState<any>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [mapGeo, setMapGeo] = useState<Array<{ role: 'center'|'neighbor', att_name: string, latitude: number, longitude: number }>>([])

  const mapRef = useRef<any>(null)

  const mapCenter = useMemo(() => {
    const center = mapGeo.find((r) => r.role === 'center')
    if (center) return [center.latitude, center.longitude] as [number, number]
    return [19.4326, -99.1332] as [number, number] // fallback (CDMX)
  }, [mapGeo])

  // Fit map to markers when data changes
  useEffectReact(() => {
    if (!mapRef.current || mapGeo.length === 0) return
    const latlngs = mapGeo.map((p) => [p.latitude, p.longitude]) as [number, number][]
    // Ensure at least two points for bounds; duplicate if only one
    const pts = latlngs.length === 1 ? [latlngs[0], [latlngs[0][0] + 0.0001, latlngs[0][1] + 0.0001]] : latlngs
    mapRef.current.fitBounds(pts, { padding: [24, 24] })
  }, [mapGeo])

  useEffect(() => {
    const baseUrl = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000'
    fetch(`${baseUrl}/api/health`)
      .then(async (res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const data = await res.json()
        setHealth(data.status || 'ok')
      })
      .catch((err) => setHealth(`error: ${String(err)}`))
  }, [])

  // Debounced site autocomplete
  useEffectReact(() => {
    let t: any
    const query = site?.trim() || ''
    if (query.length >= 2) {
      t = setTimeout(async () => {
        try {
          setSiteLoading(true)
          const suggestions = await api.searchSites(query, 10)
          setSiteSuggestions(suggestions)
        } catch {
          setSiteSuggestions([])
        } finally {
          setSiteLoading(false)
        }
      }, 200)
    } else {
      setSiteSuggestions([])
    }
    return () => t && clearTimeout(t)
  }, [site])

  return (
    <div className="app">
      <header className="app-header">
        <div>
          <h1>RAN Quality Evaluator</h1>
          <div className="sub">API Health: <code>{health}</code></div>
        </div>
      </header>

      <main className="layout">
        <section className="panel controls">
          <h2>Controls</h2>
          <div className="form-row">
            <label className="field">
              <span>Site ATT</span>
              <input
                value={site}
                onChange={(e) => setSite(e.target.value)}
                placeholder="SITE_ID"
              />
              {(siteLoading || siteSuggestions.length > 0) && (
                <div className="suggest">
                  {siteLoading && <div className="suggest-item muted">Searching…</div>}
                  {!siteLoading && siteSuggestions.length > 0 && (
                    <div className="suggest-list">
                      {siteSuggestions.map((s) => (
                        <div
                          key={s}
                          className="suggest-item"
                          onClick={() => { setSite(s); setSiteSuggestions([]) }}
                        >
                          {s}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </label>
            <label className="field">
              <span>Radius (km)</span>
              <input
                type="number"
                min={0.1}
                step={0.1}
                value={radiusKm}
                onChange={(e) => setRadiusKm(parseFloat(e.target.value))}
              />
            </label>
          </div>

          <div className="section">
            <h3>Site Data</h3>
            <div className="btn-grid">
              <button disabled={loading} onClick={async () => {
                try {
                  setLoading(true); setError(null);
                  const data = await api.ranges(site);
                  setResult(data);
                } catch (e:any) { setError(String(e)); } finally { setLoading(false); }
              }}>Ranges</button>

              <button disabled={loading} onClick={async () => {
                try {
                  setLoading(true); setError(null);
                  const data = await api.cellChanges(site, { group_by: 'network' });
                  setResult(data);
                } catch (e:any) { setError(String(e)); } finally { setLoading(false); }
              }}>Cell Changes (network)</button>

              <button disabled={loading} onClick={async () => {
                try {
                  setLoading(true); setError(null);
                  const data = await api.cqi(site, {});
                  setResult(data);
                } catch (e:any) { setError(String(e)); } finally { setLoading(false); }
              }}>CQI (all)</button>

              <button disabled={loading} onClick={async () => {
                try {
                  setLoading(true); setError(null);
                  const data = await api.traffic(site, { technology: '4G' });
                  setResult(data);
                } catch (e:any) { setError(String(e)); } finally { setLoading(false); }
              }}>Traffic (4G)</button>

              <button disabled={loading} onClick={async () => {
                try {
                  setLoading(true); setError(null);
                  const data = await api.voice(site, { technology: '4G' });
                  setResult(data);
                } catch (e:any) { setError(String(e)); } finally { setLoading(false); }
              }}>Voice (4G)</button>
            </div>
          </div>

          <div className="section">
            <h3>Neighbors</h3>
            <div className="btn-grid">
              <button disabled={loading} onClick={async () => {
                try {
                  setLoading(true); setError(null);
                  const data = await api.neighbors(site, { radius_km: radiusKm });
                  setResult(data);
                } catch (e:any) { setError(String(e)); } finally { setLoading(false); }
              }}>Neighbors List</button>

              <button disabled={loading} onClick={async () => {
                try {
                  setLoading(true); setError(null);
                  const data = await api.neighborsGeo(site, { radius_km: radiusKm });
                  setResult(data);
                } catch (e:any) { setError(String(e)); } finally { setLoading(false); }
              }}>Neighbors Geo</button>

              <button disabled={loading} onClick={async () => {
                try {
                  setLoading(true); setError(null);
                  const data = await api.neighborsCqi(site, { technology: '4G', radius_km: radiusKm });
                  setResult(data);
                } catch (e:any) { setError(String(e)); } finally { setLoading(false); }
              }}>Neighbors CQI (4G)</button>

              <button disabled={loading} onClick={async () => {
                try {
                  setLoading(true); setError(null);
                  const data = await api.neighborsTraffic(site, { technology: '4G', radius_km: radiusKm });
                  setResult(data);
                } catch (e:any) { setError(String(e)); } finally { setLoading(false); }
              }}>Neighbors Traffic (4G)</button>

              <button disabled={loading} onClick={async () => {
                try {
                  setLoading(true); setError(null);
                  const data = await api.neighborsVoice(site, { technology: '4G', radius_km: radiusKm });
                  setResult(data);
                } catch (e:any) { setError(String(e)); } finally { setLoading(false); }
              }}>Neighbors Voice (4G)</button>

              <button disabled={loading} className="primary" onClick={async () => {
                try {
                  setLoading(true); setError(null);
                  const data = await api.neighborsGeo(site, { radius_km: radiusKm });
                  setMapGeo(Array.isArray(data) ? data : []);
                } catch (e:any) { setError(String(e)); } finally { setLoading(false); }
              }}>Show Map</button>
            </div>
          </div>

          {loading && <div className="note">Loading...</div>}
          {error && <div className="note error">{error}</div>}
        </section>

        <section className="panel outputs">
          <div className="output-grid">
            <div className="output-block">
              <div className="block-title">Response</div>
              <pre className="code-block">
                {result ? JSON.stringify(result, null, 2) : 'No data yet'}
              </pre>
            </div>
            <div className="output-block">
              <div className="block-title">Map</div>
              {mapGeo.length > 0 ? (
                <div className="map-wrap">
                  <MapContainer ref={mapRef} center={mapCenter} zoom={13} style={{ height: '100%', width: '100%' }}>
                    <TileLayer
                      attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                      url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                    />
                    {/* Draw radius circle around center (in meters) if available */}
                    {(() => {
                      const c = mapGeo.find((p) => p.role === 'center')
                      if (!c) return null
                      // Approximate radius from current radiusKm when fetched
                      const radiusMeters = Math.max(100, Math.round(radiusKm * 1000))
                      return (
                        <Circle center={[c.latitude, c.longitude]} radius={radiusMeters} pathOptions={{ color: '#ff9f0a', fillOpacity: 0.05 }} />
                      )
                    })()}
                    {mapGeo.map((p, idx) => (
                      <CircleMarker
                        key={`${p.role}-${p.att_name}-${idx}`}
                        center={[p.latitude, p.longitude]}
                        radius={p.role === 'center' ? 8 : 6}
                        pathOptions={{ color: p.role === 'center' ? '#ff3b30' : '#007aff', fillOpacity: 0.8 }}
                      >
                        <Popup>
                          <div>
                            <div><strong>{p.role.toUpperCase()}</strong></div>
                            <div>Site: {p.att_name}</div>
                            <div>Lat: {p.latitude.toFixed(6)}</div>
                            <div>Lon: {p.longitude.toFixed(6)}</div>
                          </div>
                        </Popup>
                      </CircleMarker>
                    ))}
                  </MapContainer>
                </div>
              ) : (
                <div className="note">Click "Show Map" to visualize neighbors.</div>
              )}
            </div>
          </div>
        </section>
      </main>
    </div>
  )
}

export default App
