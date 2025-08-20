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
  const [showSitesModal, setShowSitesModal] = useState(false)
  const [downloadingPdf, setDownloadingPdf] = useState(false)
  // Evaluation (M4)
  const [evalLoading, setEvalLoading] = useState(false)
  const [evalResult, setEvalResult] = useState<null | { site_att: string; input_date: string; overall: 'Pass'|'Fail'|'Restored'|'Inconclusive'|null; options: any; metrics: any[] }>(null)
  const [evalError, setEvalError] = useState<string | null>(null)
  const [evalDate, setEvalDate] = useState<string>('')
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

  // A valid evaluation date must be explicitly chosen by the user
  const isValidEvalDate = useMemo(() => /^\d{4}-\d{2}-\d{2}$/.test(evalDate), [evalDate])

  const mapCenter = useMemo(() => {
    const center = mapGeo.find((r) => r.role === 'center')
    if (center) return [center.latitude, center.longitude] as [number, number]
    return [19.4326, -99.1332] as [number, number] // fallback (CDMX)
  }, [mapGeo])

  const neighborNames = useMemo(() => (
    (mapGeo || []).filter(g => g.role === 'neighbor').map(g => g.att_name)
  ), [mapGeo])

  // Build per-technology totals for stacked traffic charts
  function asTrafficByTech(rows: any[]): any[] {
    if (!Array.isArray(rows)) return []
    return rows.map((r) => {
      const t = r?.time || r?.date || r?.day || r?.timestamp
      const g3 = (r?.h3g_traffic_d_user_ps_gb || 0) + (r?.e3g_traffic_d_user_ps_gb || 0) + (r?.n3g_traffic_d_user_ps_gb || 0)
      const g4 = (r?.h4g_traffic_d_user_ps_gb || 0) + (r?.s4g_traffic_d_user_ps_gb || 0) + (r?.e4g_traffic_d_user_ps_gb || 0) + (r?.n4g_traffic_d_user_ps_gb || 0)
      const g5 = (r?.e5g_nsa_traffic_pdcp_gb_5gendc_4glegn || 0) + (r?.n5g_nsa_traffic_pdcp_gb_5gendc_4glegn || 0) + (r?.e5g_nsa_traffic_pdcp_gb_5gendc_5gleg || 0) + (r?.n5g_nsa_traffic_pdcp_gb_5gendc_5gleg || 0)
      return { time: t, traffic_3g_gb: g3, traffic_4g_gb: g4, traffic_5g_gb: g5 }
    })
  }

  // Build voice series: 3G CS vs VoLTE
  function asVoice3GVolte(rows: any[]): any[] {
    if (!Array.isArray(rows)) return []
    return rows.map((r) => {
      const t = r?.time || r?.date || r?.day || r?.timestamp
      const volte = (r?.user_traffic_volte_e || 0) + (r?.user_traffic_volte_h || 0) + (r?.user_traffic_volte_n || 0) + (r?.user_traffic_volte_s || 0)
      const cs3g = (r?.h3g_traffic_v_user_cs || 0) + (r?.e3g_traffic_v_user_cs || 0) + (r?.n3g_traffic_v_user_cs || 0)
      return { time: t, voice_3g_cs: cs3g, voice_volte: volte }
    })
  }

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


  // Trigger evaluation only when site and a valid date have been chosen
  useEffect(() => {
    if (!site || !isValidEvalDate) { setEvalResult(null); return }
    const input_date = evalDate
    let cancelled = false
    setEvalLoading(true)
    setEvalError(null)
    api.evaluate({ site_att: site, input_date, threshold: evalThreshold, period: evalPeriod, guard: evalGuard })
      .then(res => { if (!cancelled) setEvalResult(res) })
      .catch(err => { if (!cancelled) setEvalError(String(err)) })
      .finally(() => { if (!cancelled) setEvalLoading(false) })
    return () => { cancelled = true }
  }, [site, evalThreshold, evalPeriod, evalGuard, evalDate, isValidEvalDate])

  async function handleDownloadReport() {
    if (!site || !isValidEvalDate) return
    try {
      setDownloadingPdf(true)
      const input_date = evalDate
      const blob = await api.reportPdf({
        site_att: site,
        input_date,
        threshold: evalThreshold,
        period: evalPeriod,
        guard: evalGuard,
        radius_km: radiusKm,
        include_debug: false,
      })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `ran_evaluation_${site}.pdf`
      document.body.appendChild(a)
      a.click()
      a.remove()
      URL.revokeObjectURL(url)
    } catch (e) {
      alert(`Failed to generate report: ${e}`)
    } finally {
      setDownloadingPdf(false)
    }
  }

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

  // Auto-fetch site datasets only when site and a valid date are set
  useEffectReact(() => {
    const s = site?.trim()
    if (!s || s.length < 2 || !isValidEvalDate) return
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
          api.traffic(s, {}),
          api.voice(s, {}),
        ])
        if (cancelled) return
        setSiteCqi(Array.isArray(cqi) ? cqi : [])
        setSiteTraffic(Array.isArray(traffic) ? asTrafficByTech(traffic) : [])
        setSiteVoice(Array.isArray(voice) ? asVoice3GVolte(voice) : [])
      } catch (e: any) {
        if (!cancelled) setError(String(e))
      } finally {
        if (!cancelled) setLoadingSite(false)
      }
    })()
    return () => { cancelled = true }
  }, [site, isValidEvalDate])

  // Auto-fetch neighbor datasets (including geo) only when site and a valid date are set
  useEffectReact(() => {
    const s = site?.trim()
    if (!s || s.length < 2 || !isValidEvalDate) return
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
          api.neighborsTraffic(s, { radius_km: radiusKm }),
          api.neighborsVoice(s, { radius_km: radiusKm }),
        ])
        if (cancelled) return
        setMapGeo(Array.isArray(geo) ? geo : [])
        setNbCqi(Array.isArray(cqi) ? cqi : [])
        setNbTraffic(Array.isArray(traffic) ? asTrafficByTech(traffic) : [])
        setNbVoice(Array.isArray(voice) ? asVoice3GVolte(voice) : [])
        setFetchedNbOnce(true)
      } catch (e: any) {
        if (!cancelled) setError(String(e))
      } finally {
        if (!cancelled) setLoadingNb(false)
      }
    })()
    return () => { cancelled = true }
  }, [site, radiusKm, isValidEvalDate])

  return (
    <div className="app">
      <header className="app-header">
        <div>
          <h1>RAN Quality Evaluator</h1>
          <div className="sub">API Health: <code>{health}</code></div>
        </div>
        <button
          className="theme-toggle icon"
          onClick={() => setTheme(prev => prev === 'dark' ? 'light' : 'dark')}
          title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} mode`}
          aria-label={`Switch to ${theme === 'dark' ? 'light' : 'dark'} mode`}
        >
          {theme === 'dark' ? (
            // Sun icon for switching to light
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
              <circle cx="12" cy="12" r="4.5" stroke="currentColor" strokeWidth="1.5"/>
              <path d="M12 2v3M12 19v3M4.22 4.22l2.12 2.12M17.66 17.66l2.12 2.12M2 12h3M19 12h3M4.22 19.78l2.12-2.12M17.66 6.34l2.12-2.12" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/>
            </svg>
          ) : (
            // Moon icon for switching to dark
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
              <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" stroke="currentColor" strokeWidth="1.5" fill="currentColor"/>
            </svg>
          )}
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
              <span>Input date</span>
              <input
                type="date"
                value={evalDate}
                onChange={(e) => { setEvalDate(e.target.value) }}
              />
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
                    <span className="meta">
                      <button
                        className="button-show"
                        onClick={handleDownloadReport}
                        disabled={downloadingPdf || evalLoading || !isValidEvalDate}
                        title="Download PDF report"
                      >
                        {downloadingPdf ? 'Downloading…' : 'Download PDF report'}
                      </button>
                    </span>
                  </div>
                  {evalResult.metrics?.length ? (
                    <div className="eval-metrics">
                      {(() => {
                        const siteMetrics = (evalResult.metrics || []).filter((m: any) => typeof m?.name === 'string' && m.name.startsWith('Site '))
                        const nbMetrics = (evalResult.metrics || []).filter((m: any) => typeof m?.name === 'string' && m.name.startsWith('Neighbors '))
                        const Table = ({ title, rows }: { title: string; rows: any[] }) => (
                          <div className="eval-col">
                            <div className="col-title">{title}</div>
                            <div className="mtable">
                              <div className="thead">
                                <div>Name</div>
                                <div>Δ After/Before</div>
                                <div>Δ Last/After</div>
                                <div>Class</div>
                                <div>Verdict</div>
                              </div>
                              <div className="tbody">
                                {rows.map((m: any, i: number) => (
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
                        )
                        return (
                          <div className="eval-columns">
                            <Table title="Site" rows={siteMetrics} />
                            <Table title="Neighbors" rows={nbMetrics} />
                          </div>
                        )
                      })()}
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
            {(() => {
              const inputDate = (evalResult?.input_date || evalDate) as string | undefined
              const ranges = (evalResult?.options?.ranges || {}) as any
              // prefer the true last date present in any loaded dataset
              const dateKeys = ['date','time','day','timestamp']
              function rowsMaxDate(rows: any[]): number | null {
                if (!rows || rows.length === 0) return null
                let best: number | null = null
                for (const r of rows) {
                  for (const k of dateKeys) {
                    if (r && r[k] != null) {
                      const t = typeof r[k] === 'string' ? Date.parse(r[k]) : (typeof r[k] === 'number' ? r[k] : NaN)
                      if (!Number.isNaN(t)) best = best == null ? t : Math.max(best, t)
                      break
                    }
                  }
                }
                return best
              }
              const candidatesNum: Array<number> = []
              const s1 = rowsMaxDate(siteCqi); if (s1 != null) candidatesNum.push(s1)
              const s2 = rowsMaxDate(siteTraffic); if (s2 != null) candidatesNum.push(s2)
              const s3 = rowsMaxDate(siteVoice); if (s3 != null) candidatesNum.push(s3)
              const n1 = rowsMaxDate(nbCqi); if (n1 != null) candidatesNum.push(n1)
              const n2 = rowsMaxDate(nbTraffic); if (n2 != null) candidatesNum.push(n2)
              const n3 = rowsMaxDate(nbVoice); if (n3 != null) candidatesNum.push(n3)
              const dataMaxStr = candidatesNum.length ? new Date(Math.max(...candidatesNum)).toISOString().slice(0,10) : undefined
              const maxCandidate = [
                dataMaxStr,
                ranges?.last?.to,
                ranges?.after?.to,
                ranges?.before?.to,
              ].filter(Boolean).sort().slice(-1)[0] as string | undefined
              const xMax = maxCandidate || inputDate
              const xMin = inputDate ? new Date(new Date(inputDate).getTime() - 30*24*3600*1000).toISOString().slice(0,10) : undefined
              const lineColor = theme === 'dark' ? '#ffffff' : '#111111'
              const regionStroke = theme === 'dark' ? '#3ea0ff' : '#0a84ff'
              const regionFill = theme === 'dark' ? '#2578ff' : '#0a84ff'
              const regionFillOpacity = theme === 'dark' ? 0.14 : 0.06
              const regionStrokeWidth = theme === 'dark' ? 2 : 1
              const vLines = inputDate ? [{ x: inputDate, stroke: lineColor, strokeDasharray: '8 8', strokeWidth: 3 as const, label: '' }] : []
              const mkRegion = (r?: {from?: string; to?: string}) => (r?.from && r?.to ? { from: r.from, to: r.to, stroke: regionStroke, strokeDasharray: '6 6', strokeWidth: regionStrokeWidth, fill: regionFill, fillOpacity: regionFillOpacity } : null)
              const mkLastRegion = (r?: {from?: string; to?: string}) => (r?.from && r?.to ? { from: r.from, to: r.to, stroke: regionStroke, strokeDasharray: '6 6', strokeWidth: 2, fill: regionFill, fillOpacity: 0 } : null)
              const regions = [mkRegion(ranges?.before), mkRegion(ranges?.after), mkLastRegion(ranges?.last)].filter(Boolean) as any[]
              const common = { xMin, xMax, vLines, regions }
              return (
                <>
                  <div className="output-block">
                    <div className="block-title">Plot01 – Site CQIs</div>
                    <SimpleLineChart data={siteCqi} title="CQIs" loading={loadingSite && fetchedSiteOnce} {...common} />
                  </div>
                  <div className="output-block">
                    <div className="block-title">Plot02 – Site Data Traffic</div>
                    <SimpleStackedBar data={siteTraffic} title="Traffic" loading={loadingSite && fetchedSiteOnce} {...common} />
                  </div>
                  <div className="output-block">
                    <div className="block-title">Plot03 – Site Voice Traffic</div>
                    <SimpleStackedBar data={siteVoice} title="Voice Traffic" loading={loadingSite && fetchedSiteOnce} {...common} />
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
                              <CircleMarker key={`n-${idx}`} center={[g.latitude, g.longitude]} radius={6} pathOptions={{ color: '#f4c20d' }}>
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
                    <div style={{ marginTop: 8 }}>
                      <button className="btn-grid button-show" onClick={() => setShowSitesModal(true)}>Show sites</button>
                    </div>
                  </div>
                  <div className="output-block">
                    <div className="block-title">Plot05 – Neighbor CQIs</div>
                    <SimpleLineChart data={nbCqi} title="Neighbor CQIs" loading={loadingNb && fetchedNbOnce} {...common} />
                  </div>
                  <div className="output-block">
                    <div className="block-title">Plot06 – Neighbor Data Traffic</div>
                    <SimpleStackedBar data={nbTraffic} title="Neighbor Traffic" loading={loadingNb && fetchedNbOnce} {...common} />
                  </div>
                  <div className="output-block">
                    <div className="block-title">Plot07 – Neighbor Voice Traffic</div>
                    <SimpleStackedBar data={nbVoice} title="Neighbor Voice" loading={loadingNb && fetchedNbOnce} {...common} />
                  </div>
                </>
              )
            })()}
          </div>
        </section>
        {showSitesModal && (
          <div className="modal-backdrop" onClick={() => setShowSitesModal(false)}>
            <div className="modal" onClick={(e) => e.stopPropagation()}>
              <div className="modal-title">Neighbor Sites</div>
              <div className="modal-body">
                <textarea readOnly value={neighborNames.join('\n')} onFocus={(e) => e.currentTarget.select()} />
              </div>
              <div className="modal-actions">
                <button className="theme-toggle" onClick={() => setShowSitesModal(false)}>Close</button>
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  )
}

export default App
