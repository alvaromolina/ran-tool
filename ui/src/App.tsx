import { useEffect, useMemo, useState } from 'react'
import './App.css'
import { api } from './api'
import { MapContainer, TileLayer, CircleMarker, Popup, Circle } from 'react-leaflet'
import { useRef, useEffect as useEffectReact } from 'react'
import { SimpleLineChart, SimpleStackedBar } from './components/Charts'

function App() {
  const [health, setHealth] = useState<string>('checking...')
  const [theme, setTheme] = useState<'dark'|'light'>(() => (localStorage.getItem('theme') as 'dark'|'light') || 'dark')
  const [site, setSite] = useState<string>('')
  const [siteSuggestions, setSiteSuggestions] = useState<string[]>([])
  const [siteLoading, setSiteLoading] = useState(false)
  const [radiusKm, setRadiusKm] = useState<number>(5)
  const [loadingSite, setLoadingSite] = useState(false)
  const [loadingNb, setLoadingNb] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [mapGeo, setMapGeo] = useState<Array<{ role: 'center'|'neighbor', att_name: string, latitude: number, longitude: number }>>([])
  // Chart datasets for M3 plots
  const [siteCqi, setSiteCqi] = useState<any[]>([])
  const [siteTraffic, setSiteTraffic] = useState<any[]>([])
  const [siteVoice, setSiteVoice] = useState<any[]>([])
  const [nbCqi, setNbCqi] = useState<any[]>([])
  const [nbTraffic, setNbTraffic] = useState<any[]>([])
  const [nbVoice, setNbVoice] = useState<any[]>([])
  // Evaluation (M4)
  const [evalLoading, setEvalLoading] = useState(false)
  const [evalResult, setEvalResult] = useState<null | { site_att: string; input_date: string; overall: 'Pass'|'Fail'|'Restored'|'Inconclusive'|null; options: any; metrics: any[] }>(null)
  const [evalError, setEvalError] = useState<string | null>(null)
  const [evalDate, setEvalDate] = useState<string>('')
  const [evalDateTouched, setEvalDateTouched] = useState<boolean>(false)
  const [evalThreshold, setEvalThreshold] = useState<number>(() => {
    const v = localStorage.getItem('eval.threshold');
    return v != null ? parseFloat(v) : 0.05;
  })
  const [evalPeriod, setEvalPeriod] = useState<number>(() => {
    const v = localStorage.getItem('eval.period');
    return v != null ? parseInt(v, 10) : 7;
  })
  const [evalGuard, setEvalGuard] = useState<number>(() => {
    const v = localStorage.getItem('eval.guard');
    return v != null ? parseInt(v, 10) : 7;
  })

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
    // apply theme attribute to <html>
    document.documentElement.setAttribute('data-theme', theme)
    localStorage.setItem('theme', theme)
  }, [theme])

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

  // Persist evaluation options
  useEffect(() => { localStorage.setItem('eval.threshold', String(evalThreshold)) }, [evalThreshold])
  useEffect(() => { localStorage.setItem('eval.period', String(evalPeriod)) }, [evalPeriod])
  useEffect(() => { localStorage.setItem('eval.guard', String(evalGuard)) }, [evalGuard])

  // Prefill input date from ranges when site changes (only if user didn't select a date yet)
  useEffect(() => {
    if (!site) { setEvalDate(''); setEvalDateTouched(false); return }
    let cancelled = false
    api.ranges(site)
      .then(r => {
        if (cancelled) return
        const suggested = r.max_date || new Date().toISOString().slice(0,10)
        if (!evalDateTouched) setEvalDate(suggested)
      })
      .catch(() => {/* ignore for prefill */})
    return () => { cancelled = true }
  }, [site])

  // Trigger evaluation on site/options/date change
  useEffect(() => {
    if (!site) { setEvalResult(null); return }
    const input_date = (evalDate && /^\d{4}-\d{2}-\d{2}$/.test(evalDate)) ? evalDate : new Date().toISOString().slice(0,10)
    let cancelled = false
    setEvalLoading(true)
    setEvalError(null)
    api.evaluate({ site_att: site, input_date, threshold: evalThreshold, period: evalPeriod, guard: evalGuard })
      .then(res => { if (!cancelled) setEvalResult(res) })
      .catch(err => { if (!cancelled) setEvalError(String(err)) })
      .finally(() => { if (!cancelled) setEvalLoading(false) })
    return () => { cancelled = true }
  }, [site, evalThreshold, evalPeriod, evalGuard, evalDate])

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

  // Track whether we've fetched at least once to decide loader vs empty-state
  const [fetchedSiteOnce, setFetchedSiteOnce] = useState(false)
  const [fetchedNbOnce, setFetchedNbOnce] = useState(false)

  // Auto-fetch site datasets when a valid site is set
  useEffectReact(() => {
    const s = site?.trim()
    if (!s || s.length < 2) return
    let cancelled = false
    ;(async () => {
      try {
        setLoadingSite(true)
        setError(null)
        // clear old site datasets to avoid showing stale charts while loading
        setSiteCqi([]); setSiteTraffic([]); setSiteVoice([])
        // mark that we've started fetching so loaders appear
        setFetchedSiteOnce(true)
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
        if (!cancelled) setLoadingSite(false)
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
        setLoadingNb(true)
        setError(null)
        // clear neighbor datasets while loading
        setMapGeo([]); setNbCqi([]); setNbTraffic([]); setNbVoice([])
        // show loaders immediately for neighbors
        setFetchedNbOnce(true)
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
        setFetchedNbOnce(true)
      } catch (e: any) {
        if (!cancelled) setError(String(e))
      } finally {
        if (!cancelled) setLoadingNb(false)
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
        <button
          className="theme-toggle"
          onClick={() => setTheme(prev => prev === 'dark' ? 'light' : 'dark')}
          title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} mode`}
        >
          {theme === 'dark' ? 'Light mode' : 'Dark mode'}
        </button>
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
          <div className="form-row">
            <label className="field">
              <span>Threshold (%)</span>
              <input
                type="number"
                min={0}
                max={100}
                step={0.5}
                value={Math.round(evalThreshold * 10000) / 100}
                onChange={e => setEvalThreshold(Math.max(0, Math.min(1, (parseFloat(e.target.value) || 0) / 100)))}
              />
            </label>
            <label className="field">
              <span>Input date</span>
              <input
                type="date"
                value={evalDate}
                onChange={(e) => { setEvalDate(e.target.value); setEvalDateTouched(true) }}
              />
            </label>
            <label className="field">
              <span>Period (days)</span>
              <input
                type="number"
                min={1}
                max={90}
                step={1}
                value={evalPeriod}
                onChange={e => setEvalPeriod(Math.max(1, Math.min(90, parseInt(e.target.value || '0', 10))))}
              />
            </label>
            <label className="field">
              <span>Guard (days)</span>
              <input
                type="number"
                min={0}
                max={90}
                step={1}
                value={evalGuard}
                onChange={e => setEvalGuard(Math.max(0, Math.min(90, parseInt(e.target.value || '0', 10))))}
              />
            </label>
          </div>

          {/* per-chart loading placeholders show in each chart. We only show loaders after first fetch starts */}
          {error && <div className="note error">{error}</div>}
        </section>

        <section className="panel outputs">
          <div className={`evaluation-banner ${evalResult?.overall ? `is-${evalResult.overall.toLowerCase()}` : ''}`}>
            {site ? (
              evalLoading ? (
                <div className="eval-loading">Evaluating…</div>
              ) : evalError ? (
                <div className="eval-error">Evaluation error: {evalError}</div>
              ) : evalResult ? (
                <>
                  <div className="eval-title">Evaluation Result</div>
                  <div className="eval-summary">
                    <span className="badge">{evalResult.overall || 'Inconclusive'}</span>
                    <span className="meta">Date: {evalResult.input_date}</span>
                    <span className="meta">Threshold: {(evalResult.options?.threshold ?? 0.05) * 100}%</span>
                  </div>
                  {evalResult.metrics?.length ? (
                    <div className="eval-metrics">
                      <div className="mtable">
                        <div className="thead">
                          <div>Name</div>
                          <div>Δ After/Before</div>
                          <div>Δ Last/After</div>
                          <div>Class</div>
                          <div>Verdict</div>
                        </div>
                        <div className="tbody">
                          {evalResult.metrics.map((m, i) => (
                            <div className="trow" key={i}>
                              <div className="cell name">{m.name}</div>
                              <div className="cell num">{m.delta_after_before != null ? `${(m.delta_after_before*100).toFixed(1)}%` : '—'}</div>
                              <div className="cell num">{m.delta_last_after != null ? `${(m.delta_last_after*100).toFixed(1)}%` : '—'}</div>
                              <div className="cell">{m.klass || '—'}</div>
                              <div className="cell"><span className={`vbadge vb-${(m.verdict||'inconclusive').toLowerCase()}`}>{m.verdict || 'Inconclusive'}</span></div>
                            </div>
                          ))}
                        </div>
                      </div>
                    </div>
                  ) : null}
                </>
              ) : (
                <div className="eval-empty">No evaluation yet.</div>
              )
            ) : (
              <div className="eval-empty">Choose a site to see the evaluation summary.</div>
            )}
          </div>
          <div className="output-grid">
            <div className="output-block">
              <div className="block-title">Plot01 – Site CQIs</div>
              <SimpleLineChart data={siteCqi} title="CQIs" loading={loadingSite && fetchedSiteOnce} />
            </div>
            <div className="output-block">
              <div className="block-title">Plot02 – Site Data Traffic</div>
              <SimpleStackedBar data={siteTraffic} title="Traffic" loading={loadingSite && fetchedSiteOnce} />
            </div>
            <div className="output-block">
              <div className="block-title">Plot03 – Site Voice Traffic</div>
              <SimpleStackedBar data={siteVoice} title="Voice Traffic" loading={loadingSite && fetchedSiteOnce} />
            </div>
            <div className="output-block">
              <div className="block-title">Plot05 – Neighbor CQIs</div>
              <SimpleLineChart data={nbCqi} title="Neighbor CQIs" loading={loadingNb && fetchedNbOnce} />
            </div>
            <div className="output-block">
              <div className="block-title">Plot06 – Neighbor Data Traffic</div>
              <SimpleStackedBar data={nbTraffic} title="Neighbor Traffic" loading={loadingNb && fetchedNbOnce} />
            </div>
            <div className="output-block">
              <div className="block-title">Plot07 – Neighbor Voice Traffic</div>
              <SimpleStackedBar data={nbVoice} title="Neighbor Voice" loading={loadingNb && fetchedNbOnce} />
            </div>
            <div className="output-block">
              <div className="block-title">Plot04 – Map</div>
              {loadingNb && fetchedNbOnce ? (
                <div className="chart-loading" style={{ height: 360 }} />
              ) : mapGeo.length > 0 ? (
                <div className="map-wrap">
                  <MapContainer ref={mapRef} center={mapCenter} zoom={13} style={{ height: '100%', width: '100%' }}>
                    <TileLayer
                      attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                      url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                    />
                    {mapGeo.map((g, idx) => (
                      g.role === 'center' ? (
                        <CircleMarker key={`c-${idx}`} center={[g.latitude, g.longitude]} radius={8} pathOptions={{ color: '#34c759' }}>
                          <Popup>Center: {g.att_name}</Popup>
                        </CircleMarker>
                      ) : (
                        <CircleMarker key={`n-${idx}`} center={[g.latitude, g.longitude]} radius={6} pathOptions={{ color: '#0a84ff' }}>
                          <Popup>Neighbor: {g.att_name}</Popup>
                        </CircleMarker>
                      )
                    ))}
                    {/* radius circle for context */}
                    <Circle center={mapCenter} radius={radiusKm * 1000} pathOptions={{ color: '#9aa0a6' }} />
                  </MapContainer>
                </div>
              ) : (
                <div className="chart-empty" style={{ height: 360 }}>No neighbor geometry available. Choose a valid site and radius.</div>
              )}
            </div>
          </div>
        </section>
      </main>
    </div>
  )
}

export default App
