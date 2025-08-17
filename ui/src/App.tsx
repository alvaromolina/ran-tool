import { useEffect, useMemo, useState } from 'react'
import './App.css'
import { api } from './api'
import { MapContainer, TileLayer, CircleMarker, Popup, Circle } from 'react-leaflet'
import { useRef, useEffect as useEffectReact } from 'react'
import { SimpleLineChart, SimpleStackedBar } from './components/Charts'

function App() {
  const [health, setHealth] = useState<string>('checking...')
  const [site, setSite] = useState<string>('TESTSITE')
  const [siteSuggestions, setSiteSuggestions] = useState<string[]>([])
  const [siteLoading, setSiteLoading] = useState(false)
  const [radiusKm, setRadiusKm] = useState<number>(5)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [mapGeo, setMapGeo] = useState<Array<{ role: 'center'|'neighbor', att_name: string, latitude: number, longitude: number }>>([])
  // Chart datasets for M3 plots
  const [siteCqi, setSiteCqi] = useState<any[]>([])
  const [siteTraffic, setSiteTraffic] = useState<any[]>([])
  const [siteVoice, setSiteVoice] = useState<any[]>([])
  const [nbCqi, setNbCqi] = useState<any[]>([])
  const [nbTraffic, setNbTraffic] = useState<any[]>([])
  const [nbVoice, setNbVoice] = useState<any[]>([])

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

  // Auto-fetch site datasets when a valid site is set
  useEffectReact(() => {
    const s = site?.trim()
    if (!s || s.length < 2) return
    let cancelled = false
    ;(async () => {
      try {
        setLoading(true)
        setError(null)
        const [cqi, traffic, voice] = await Promise.all([
          api.cqi(s, {}),
          api.traffic(s, { technology: '4G' }),
          api.voice(s, { technology: '4G' }),
        ])
        if (cancelled) return
        setSiteCqi(Array.isArray(cqi) ? cqi : [])
        setSiteTraffic(Array.isArray(traffic) ? traffic : [])
        setSiteVoice(Array.isArray(voice) ? voice : [])
      } catch (e: any) {
        if (!cancelled) setError(String(e))
      } finally {
        if (!cancelled) setLoading(false)
      }
    })()
    return () => { cancelled = true }
  }, [site])

  // Auto-fetch neighbor datasets (including geo) when site or radius changes
  useEffectReact(() => {
    const s = site?.trim()
    if (!s || s.length < 2) return
    let cancelled = false
    ;(async () => {
      try {
        setLoading(true)
        setError(null)
        const [geo, cqi, traffic, voice] = await Promise.all([
          api.neighborsGeo(s, { radius_km: radiusKm }),
          api.neighborsCqi(s, { technology: '4G', radius_km: radiusKm }),
          api.neighborsTraffic(s, { technology: '4G', radius_km: radiusKm }),
          api.neighborsVoice(s, { technology: '4G', radius_km: radiusKm }),
        ])
        if (cancelled) return
        setMapGeo(Array.isArray(geo) ? geo : [])
        setNbCqi(Array.isArray(cqi) ? cqi : [])
        setNbTraffic(Array.isArray(traffic) ? traffic : [])
        setNbVoice(Array.isArray(voice) ? voice : [])
      } catch (e: any) {
        if (!cancelled) setError(String(e))
      } finally {
        if (!cancelled) setLoading(false)
      }
    })()
    return () => { cancelled = true }
  }, [site, radiusKm])

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

          {loading && <div className="note">Loading...</div>}
          {error && <div className="note error">{error}</div>}
        </section>

        <section className="panel outputs">
          <div className="output-grid">
            <div className="output-block">
              <div className="block-title">Plot01 – Site CQIs</div>
              <SimpleLineChart data={siteCqi} title="CQIs" />
            </div>
            <div className="output-block">
              <div className="block-title">Plot02 – Site Data Traffic</div>
              <SimpleStackedBar data={siteTraffic} title="Traffic" />
            </div>
            <div className="output-block">
              <div className="block-title">Plot03 – Site Voice Traffic</div>
              <SimpleStackedBar data={siteVoice} title="Voice Traffic" />
            </div>
            <div className="output-block">
              <div className="block-title">Plot05 – Neighbor CQIs</div>
              <SimpleLineChart data={nbCqi} title="Neighbor CQIs" />
            </div>
            <div className="output-block">
              <div className="block-title">Plot06 – Neighbor Data Traffic</div>
              <SimpleStackedBar data={nbTraffic} title="Neighbor Traffic" />
            </div>
            <div className="output-block">
              <div className="block-title">Plot07 – Neighbor Voice Traffic</div>
              <SimpleStackedBar data={nbVoice} title="Neighbor Voice" />
            </div>
            <div className="output-block">
              <div className="block-title">Plot04 – Map</div>
              {mapGeo.length > 0 ? (
                <div className="map-wrap">
                  <MapContainer ref={mapRef} center={mapCenter} zoom={13} style={{ height: '100%', width: '100%' }}>
                    <TileLayer
                      attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                      url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                    />
                    {(() => {
                      const c = mapGeo.find((p) => p.role === 'center')
                      if (!c) return null
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
                <div className="note">Enter a valid site to load the map.</div>
              )}
            </div>
          </div>
        </section>
      </main>
    </div>
  )
}

export default App
