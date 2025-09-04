import { useEffect, useMemo, useState } from 'react'
import './App.css'
import { api } from './api'
import { MapContainer, TileLayer, CircleMarker, Popup, Circle, Tooltip } from 'react-leaflet'
import { useRef, useEffect as useEffectReact } from 'react'
import { SimpleLineChart, SimpleStackedBar } from './components/Charts'
import html2canvas from 'html2canvas'
import jsPDF from 'jspdf'
import { StatsigProvider, useClientAsyncInit } from '@statsig/react-bindings';
import { StatsigAutoCapturePlugin } from '@statsig/web-analytics';
import { StatsigSessionReplayPlugin } from '@statsig/session-replay';


function App() {

  const { client } = useClientAsyncInit(
    "client-KHopjAwMOOYZmJI6daYMwqyqPyW7eV3JcaKiz4xRn8U",
    { userID: 'a-user' }, 
    { plugins: [ new StatsigAutoCapturePlugin(), new StatsigSessionReplayPlugin() ] },
  );

  const [health, setHealth] = useState<string>('checking...')
  const [theme, setTheme] = useState<'dark'|'light'>(() => (localStorage.getItem('theme') as 'dark'|'light') || 'dark')
  const [site, setSite] = useState<string>('')
  // Site must be explicitly selected from suggestions; free-typed text won't trigger API calls
  const [selectedSite, setSelectedSite] = useState<string>('')
  const [isValidSite, setIsValidSite] = useState<boolean>(false)
  const [siteSuggestions, setSiteSuggestions] = useState<string[]>([])
  const [siteLoading, setSiteLoading] = useState(false)
  const [radiusKm, setRadiusKm] = useState<number>(5)
  const [vecinos, setVecinos] = useState<string[]>([])
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
  // Event dates for selected site (LTE + UMTS)
  const [eventDates, setEventDates] = useState<Array<{ tech: '3G'|'4G'; date: string; add_cell: number|null; delete_cell: number|null; total_cell: number|null; remark: string|null }>>([])
  const [eventDatesLoading, setEventDatesLoading] = useState(false)
  const [eventDatesError, setEventDatesError] = useState<string|null>(null)
  const [showSitesModal, setShowSitesModal] = useState(false)
  const [showEventDatesModal, setShowEventDatesModal] = useState(false)
  const [downloadingOutputsPdf, setDownloadingOutputsPdf] = useState(false)
  // Neighbor sites list for modal
  const [nbSites, setNbSites] = useState<Array<{ site_name: string; region: string|null; province: string|null; municipality: string|null; vendor: string|null }>>([])
  const [nbSitesLoading, setNbSitesLoading] = useState(false)
  const [nbSitesError, setNbSitesError] = useState<string|null>(null)
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
  const outputsRef = useRef<HTMLElement | null>(null)
  const siteInputRef = useRef<HTMLInputElement | null>(null)
  const evalDateInputRef = useRef<HTMLInputElement | null>(null)

  // Ensure neighbor site list is unique by site name
  function dedupeNbSites(rows: Array<{ site_name: string; region: string|null; province: string|null; municipality: string|null; vendor: string|null }>): typeof rows {
    const seen = new Set<string>()
    const out: typeof rows = []
    for (const r of rows || []) {
      const k = r?.site_name ?? ''
      if (k && !seen.has(k)) { seen.add(k); out.push(r) }
    }
    return out
  }

  // A valid evaluation date must be explicitly chosen by the user
  const isValidEvalDate = useMemo(() => /^\d{4}-\d{2}-\d{2}$/.test(evalDate), [evalDate])

  const mapCenter = useMemo(() => {
    const center = mapGeo.find((r) => r.role === 'center')
    if (center) return [center.latitude, center.longitude] as [number, number]
    return [19.4326, -99.1332] as [number, number] // fallback (CDMX)
  }, [mapGeo])


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

  // Open the native date picker when clicking the icon/button
  function openNativeDatePicker() {
    const el = evalDateInputRef.current
    if (!el) return
    try {
      // @ts-ignore: showPicker is supported in modern browsers
      if (typeof el.showPicker === 'function') { el.showPicker(); return }
    } catch {}
    el.focus()
    // Fallback clicks (Safari/WebKit)
    try { el.dispatchEvent(new MouseEvent('mousedown', { bubbles: true })); } catch {}
    try { el.dispatchEvent(new MouseEvent('click', { bubbles: true })); } catch {}
  }

  async function handleDownloadOutputsPdf() {
    if (!outputsRef.current) return
    try {
      setDownloadingOutputsPdf(true)
      // Derive metadata without re-fetching data
      const siteId = (selectedSite || site || 'SITE').trim()
      const inputDate = (evalResult?.input_date || evalDate || '').trim()
      // Compute last date from currently loaded datasets and ranges
      function rowsMaxDate(rows: any[]): number | null {
        if (!Array.isArray(rows) || rows.length === 0) return null
        const dateKeys = ['date','time','day','timestamp']
        let best: number | null = null
        for (const r of rows) {
          for (const k of dateKeys) {
            const v = (r as any)?.[k]
            if (v != null) {
              const t = typeof v === 'string' ? Date.parse(v) : (typeof v === 'number' ? v : NaN)
              if (!Number.isNaN(t)) best = best == null ? t : Math.max(best, t)
              break
            }
          }
        }
        return best
      }
      const candidatesNum: number[] = []
      const pushIf = (n: number | null) => { if (n != null) candidatesNum.push(n) }
      pushIf(rowsMaxDate(siteCqi));
      pushIf(rowsMaxDate(siteTraffic));
      pushIf(rowsMaxDate(siteVoice));
      pushIf(rowsMaxDate(nbCqi));
      pushIf(rowsMaxDate(nbTraffic));
      pushIf(rowsMaxDate(nbVoice));
      const dataMaxStr = candidatesNum.length ? new Date(Math.max(...candidatesNum)).toISOString().slice(0,10) : undefined
      const ranges = (evalResult?.options?.ranges || {}) as any
      const maxCandidate = [dataMaxStr, ranges?.last?.to, ranges?.after?.to, ranges?.before?.to]
        .filter(Boolean).sort().slice(-1)[0] as string | undefined
      const lastDate = maxCandidate || inputDate || ''

      // Helper to capture a specific element via an offscreen clone
      // Lower capture scale to reduce image size while preserving legibility
      const scale = 1.5
      async function captureElement(el: HTMLElement, extraCss?: string): Promise<HTMLCanvasElement> {
        const off = document.createElement('div')
        off.style.position = 'fixed'
        off.style.left = '-10000px'
        off.style.top = '0'
        off.style.width = `${outputsRef.current!.clientWidth}px`
        off.style.zIndex = '-1'
        document.body.appendChild(off)
        const clone = el.cloneNode(true) as HTMLElement
        clone.classList.add('exporting-pdf')
        // Optional export-only CSS to normalize fonts/spacing
        if (extraCss) {
          const style = document.createElement('style')
          style.textContent = extraCss
          off.appendChild(style)
        }
        off.appendChild(clone)
        try {
          const canvas = await html2canvas(clone, { scale, useCORS: true, allowTaint: true, backgroundColor: '#ffffff' })
          return canvas
        } finally {
          document.body.removeChild(off)
        }
      }

      // Helper to capture a LIVE element (needed for Leaflet map)
      async function captureLiveElement(el: HTMLElement, extraCss?: string, customScale?: number): Promise<HTMLCanvasElement> {
        let style: HTMLStyleElement | null = null
        try {
          if (extraCss) {
            style = document.createElement('style')
            style.textContent = extraCss
            document.body.appendChild(style)
          }
          // Give the map a short moment to finish tile rendering
          await new Promise((r) => setTimeout(r, 600))
          const canvas = await html2canvas(el, { scale: customScale ?? scale, useCORS: true, allowTaint: true, backgroundColor: '#ffffff' })
          return canvas
        } finally {
          if (style) document.body.removeChild(style)
        }
      }

      // Prepare PDF landscape
      const pdf = new jsPDF('l', 'pt', 'a4')
      const pageWidth = pdf.internal.pageSize.getWidth()
      const pageHeight = pdf.internal.pageSize.getHeight()
      const margin = 36

      // PAGE 1: Title centered + metadata row + Evaluation table + Neighbor map
      // Title (use centered alignment for precise centering)
      pdf.setFont('helvetica', 'bold')
      pdf.setFontSize(20)
      const title = 'RAN Quality Analysis Report'
      const titleY = margin + 10
      pdf.text(title, pageWidth / 2, titleY, { align: 'center' } as any)
      // No separate metadata row on page 1 anymore; keep a small spacer under the title
      pdf.setFont('helvetica', 'normal')
      pdf.setFontSize(10)
      const rowY = titleY + 14

      const src = outputsRef.current
      const evalMetricsEl = src.querySelector('.eval-metrics') as HTMLElement | null
      const mapBlockEl = src.querySelector('.block-map') as HTMLElement | null
      // Evaluation header with badge (from on-screen banner)
      let y = rowY + 20
      {
        const overall = (evalResult?.overall || '').toString()
        const badge = overall.toLowerCase()
        let fill: [number,number,number] = [229,231,235]
        let textColor: [number,number,number] = [31,41,55]
        if (badge.includes('pass')) { fill = [16,185,129]; textColor = [255,255,255] }
        else if (badge.includes('fail')) { fill = [185,28,28]; textColor = [255,255,255] }
        else if (badge.includes('restor')) { fill = [37,99,235]; textColor = [255,255,255] }
        // Line 1: "Evaluation Result"
        pdf.setFont('helvetica', 'bold'); pdf.setFontSize(14)
        pdf.text('Evaluation Result', margin, y)
        y += 18 // move to next line for badge row
        // Line 2: badge + info
        const bh = 18
        const tBadge = (overall || '—')
        const tWBadge = pdf.getTextWidth(tBadge)
        const padXBadge = 8
        const bw = Math.max(48, tWBadge + padXBadge * 2)
        const bx = margin
        const by = y + 2
        const rectY = by - 12
        pdf.setFillColor(fill[0], fill[1], fill[2])
        pdf.roundedRect(bx, rectY, bw, bh, 8, 8, 'F')
        pdf.setTextColor(textColor[0], textColor[1], textColor[2])
        pdf.setFont('helvetica', 'bold'); pdf.setFontSize(11)
        // Vertically center: jsPDF uses baseline; approximate ascent with 0.35*fontSize
        const baseAdjust = 0.35 * 11
        const textY = rectY + (bh / 2) + baseAdjust - 1
        pdf.text(tBadge, bx + bw / 2, textY, { align: 'center' } as any)
        pdf.setTextColor(0,0,0)
        // Right of badge: site, dates and threshold
        const infoX = bx + bw + 12
        const thrStr = isFinite(evalThreshold as number) ? `${Math.round((evalThreshold as number) * 100)}%` : '—'
        pdf.setFont('helvetica', 'normal'); pdf.setFontSize(11)
        pdf.text(`Site: ${siteId}    Date: ${inputDate || '—'}    Last Date: ${lastDate || '—'}    Threshold: ${thrStr}`, infoX, by)
        // Move Y down for tables (extra padding to avoid overlap)
        y = by + 28
      }
      if (evalMetricsEl && mapBlockEl) {
        // Programmatic table rendering for two columns (Site, Neighbors)
        function extract(col: HTMLElement) {
          // Use compact, fixed headers
          const headers = ['Name','Before','After','Last','A/B','L/B','Class','Verdict']
          const rows: string[][] = []
          const rEls = Array.from(col.querySelectorAll('.tbody .trow')) as HTMLElement[]
          for (const r of rEls) {
            const cells = Array.from(r.children) as HTMLElement[]
            // Normalize cell text and remove decorative quote-only columns sometimes present around A/B and L/B
            let vals = cells
              .map(c => (c.textContent || '').replace(/\s+/g, ' ').trim())
              .filter(t => t.length > 0 && !/^['"“”]$/.test(t))
            // Ensure exactly 8 columns by trimming extras from the right if needed (keep main metrics)
            if (vals.length > headers.length) {
              // Prefer removing stray characters left after filter
              vals = vals.filter(t => !/^['"“”]$/.test(t))
            }
            if (vals.length > headers.length) vals = vals.slice(0, headers.length)
            rows.push(vals)
          }
          return { headers, rows }
        }
        const cols = Array.from(evalMetricsEl.querySelectorAll('.eval-col')) as HTMLElement[]
        const left = cols[0] ? extract(cols[0]) : { headers: [], rows: [] }
        const right = cols[1] ? extract(cols[1]) : { headers: [], rows: [] }

        // Layout constants
        const totalW = pageWidth - margin * 2
        const gap = 18
        const colW = (totalW - gap) / 2
        const colX = [margin, margin + colW + gap]
        const headerH = 16
        let rowH = 14
        let fontHeader = 10
        let fontBody = 9

        // Estimate height and adapt to available space with the map
        const rowsCount = Math.max(left.rows.length, right.rows.length)
        const minMapH = 200 // pt target
        function neededTableH(rh: number, hh: number) { return hh + rowsCount * rh }
        let tableH = neededTableH(rowH, headerH)
        let available = pageHeight - y - margin - (minMapH + 8)
        while (tableH > available && rowH > 10) {
          rowH -= 1; fontBody -= 0.5; fontHeader -= 0.5; tableH = neededTableH(rowH, headerH)
        }

        // Column inner widths matching on-screen look
        const cw = [120, 60, 60, 60, 72, 72, 68, 64]
        const totalCw = cw.reduce((a, b) => a + b, 0)
        const scaleX = Math.min(1, colW / totalCw)

        // Subheaders above each table: "Site" and "Neighbors"
        pdf.setFont('helvetica', 'bold'); pdf.setFontSize(13)
        pdf.text('Site', colX[0], y)
        pdf.text('Neighbors', colX[1], y)
        // Push table headers below subheaders to avoid overlap
        y += 14

        function drawTable(x: number, data: { headers: string[]; rows: string[][] }) {
          // Header
          pdf.setFont('helvetica', 'bold'); pdf.setFontSize(fontHeader)
          let tx = x
          const yHeader = y
          for (let i = 0; i < cw.length; i++) {
            const w = cw[i] * scaleX;
            const text = data.headers[i] || ''
            pdf.text(text, tx + 2, yHeader)
            tx += w
          }
          // underline header
          pdf.setDrawColor(200,200,200)
          pdf.setLineWidth(0.5)
          pdf.line(x, y + 4, x + (cw.reduce((a,b)=>a+b,0) * scaleX), y + 4)
          // Rows
          pdf.setFont('helvetica', 'normal'); pdf.setFontSize(fontBody)
          let yy = y + headerH
          for (const r of data.rows) {
            let xx = x
            for (let i = 0; i < cw.length; i++) {
              const w = cw[i] * scaleX
              let text = r[i] || ''
              if (i === cw.length - 1) {
                // verdict badge styling
                const verdict = text.toLowerCase()
                let fill: [number,number,number] = [230,230,230]
                let color: [number,number,number] = [20,20,20]
                if (verdict.includes('pass')) { fill = [209, 250, 229]; color = [6, 78, 59] }
                else if (verdict.includes('fail')) { fill = [254, 226, 226]; color = [127, 29, 29] }
                else if (verdict.includes('restor')) { fill = [219, 234, 254]; color = [30, 64, 175] }
                // Compute pill width from text and center within cell
                const fs = Math.max(8, fontBody)
                pdf.setFont('helvetica', 'bold'); pdf.setFontSize(fs)
                const tW = pdf.getTextWidth(text)
                const padX = 6
                const rectW = Math.min(w - 4, Math.max(36 * scaleX, tW + padX * 2))
                const rectH = Math.max(12, rowH - 6)
                const rectX = xx + 2 + (w - 4 - rectW) / 2
                const rectY = yy - rowH + (rowH - rectH) / 2
                pdf.setFillColor(fill[0], fill[1], fill[2])
                pdf.roundedRect(rectX, rectY, rectW, rectH, 6, 6, 'F')
                pdf.setTextColor(color[0], color[1], color[2])
                // Vertically center text inside pill
                const baseAdj = 0.35 * fs
                const tY = rectY + rectH / 2 + baseAdj - 1
                pdf.text(text, rectX + rectW / 2, tY, { align: 'center' } as any)
                // Restore defaults for the rest of the table body
                pdf.setTextColor(0,0,0)
                pdf.setFont('helvetica', 'normal'); pdf.setFontSize(fontBody)
              } else {
                // right align numeric columns 1..6
                const isNum = i > 0 && i < cw.length - 2
                if (isNum) {
                  const tw = pdf.getTextWidth(text)
                  pdf.text(text, xx + w - 2 - tw, yy - 4)
                } else {
                  pdf.text(text, xx + 2, yy - 4)
                }
              }
              xx += w
            }
            yy += rowH
          }
        }

        // Draw both tables
        drawTable(colX[0], left)
        drawTable(colX[1], right)

        // Compute actual bottom of tables to place the map
        const tableBottom = y + headerH + rowsCount * rowH
        y = tableBottom + 8

        // Capture and draw the map scaled to remaining height
        // Capture the Leaflet container itself so tile and overlay transforms share the same origin
        const liveMapEl = (mapBlockEl.querySelector('.leaflet-container') || mapBlockEl.querySelector('.map-wrap') || mapBlockEl) as HTMLElement
        // Ask Leaflet to settle layout so transforms are up-to-date
        try {
          if (mapRef.current) {
            mapRef.current.invalidateSize(false)
            // no-op pan to flush transform state without moving
            // @ts-ignore
            mapRef.current.panBy([0, 0], { animate: false })
            // re-set view at current center/zoom to ensure pane offsets are consistent
            try {
              const center = mapRef.current.getCenter()
              const zoom = mapRef.current.getZoom()
              mapRef.current.setView(center, zoom, { animate: false })
            } catch {}
          }
        } catch { /* ignore */ }
        // Give Leaflet a moment to reflow before snapshot
        await new Promise((r) => setTimeout(r, 700))
        const mapCanvas = await captureLiveElement(
          liveMapEl,
          '.block-title{display:none!important;}.btn-grid{display:none!important;}.button-show{display:none!important;}' +
          '.map-wrap{padding:0!important;margin:0!important;}' +
          '.leaflet-zoom-animated{transition:none!important;}',
          Math.min(((window as any).devicePixelRatio || 1), 1.5)
        )
        const mapW = pageWidth - margin * 2
        // Title for the map section
        pdf.setFont('helvetica', 'bold'); pdf.setFontSize(13)
        pdf.text('Neighbors Map', margin, y)
        y += 10
        let mapH = mapCanvas.height * mapW / mapCanvas.width
        const remaining = pageHeight - margin - y
        if (mapH > remaining) mapH = remaining
        // Use JPEG with medium quality to reduce PDF size
        pdf.addImage(mapCanvas.toDataURL('image/jpeg', 0.72), 'JPEG', margin, y, mapW, mapH)
        y += mapH
      } else if (src) {
        // Fallback: capture the whole outputs if specific sections not found
        const canvas = await captureElement(src)
        const imgW = pageWidth - margin * 2
        const imgH = canvas.height * imgW / canvas.width
        pdf.addImage(canvas.toDataURL('image/jpeg', 0.72), 'JPEG', margin, y, imgW, imgH)
      }

      // PAGE 2: Site KPIs (Plot01–Plot03)
      const blocks = Array.from(src.querySelectorAll('.output-grid .output-block')) as HTMLElement[]
      if (blocks.length >= 3) {
        const siteBlocks = blocks.slice(0, 3)
        const container2 = document.createElement('div')
        container2.style.position = 'fixed'
        container2.style.left = '-10000px'
        container2.style.top = '0'
        container2.style.width = `${outputsRef.current!.clientWidth}px`
        container2.style.zIndex = '-1'
        document.body.appendChild(container2)
        siteBlocks.forEach(b => container2.appendChild(b.cloneNode(true)))
        let canvas2: HTMLCanvasElement | null = null
        try {
          // Hide individual block titles for a cleaner page of charts
          const style = document.createElement('style')
          style.textContent = '.block-title{display:none!important;}'
          container2.appendChild(style)
          canvas2 = await html2canvas(container2, { scale, useCORS: true, allowTaint: true })
        } finally {
          document.body.removeChild(container2)
        }
        if (canvas2) {
          pdf.addPage()
          const maxW = pageWidth - margin * 2
          const maxH = pageHeight - margin * 2
          const w0 = maxW
          const h0 = canvas2.height * w0 / canvas2.width
          const scaleFit = Math.min(1, maxH / h0)
          const drawW = w0 * scaleFit
          const drawH = h0 * scaleFit
          const x = margin + (maxW - drawW) / 2
          const y2 = margin + (maxH - drawH) / 2
          pdf.addImage(canvas2.toDataURL('image/jpeg', 0.72), 'JPEG', x, y2, drawW, drawH)
        }
      }

      // PAGE 3: Neighbor KPIs (Plot05–Plot07)
      const nbStart = blocks.findIndex(b => b.id === 'plot05')
      const nbBlocks = nbStart >= 0 ? blocks.slice(nbStart, nbStart + 3) : blocks.slice(-3)
      if (nbBlocks.length) {
        const container3 = document.createElement('div')
        container3.style.position = 'fixed'
        container3.style.left = '-10000px'
        container3.style.top = '0'
        container3.style.width = `${outputsRef.current!.clientWidth}px`
        container3.style.zIndex = '-1'
        document.body.appendChild(container3)
        nbBlocks.forEach(b => container3.appendChild(b.cloneNode(true)))
        let canvas3: HTMLCanvasElement | null = null
        try {
          const style = document.createElement('style')
          style.textContent = '.block-title{display:none!important;}'
          container3.appendChild(style)
          canvas3 = await html2canvas(container3, { scale, useCORS: true, allowTaint: true })
        } finally {
          document.body.removeChild(container3)
        }
        if (canvas3) {
          pdf.addPage()
          const maxW = pageWidth - margin * 2
          const maxH = pageHeight - margin * 2
          const w0 = maxW
          const h0 = canvas3.height * w0 / canvas3.width
          const scaleFit = Math.min(1, maxH / h0)
          const drawW = w0 * scaleFit
          const drawH = h0 * scaleFit
          const x = margin + (maxW - drawW) / 2
          const y3 = margin + (maxH - drawH) / 2
          pdf.addImage(canvas3.toDataURL('image/jpeg', 0.72), 'JPEG', x, y3, drawW, drawH)
        }
      }

      // Footer with generator text and page numbers on every page
      const total = pdf.getNumberOfPages()
      for (let i = 1; i <= total; i++) {
        pdf.setPage(i)
        pdf.setFont('helvetica', 'normal')
        pdf.setFontSize(9)
        const left = 'Generated with RAN Quality Evaluator v01'
        const right = `Page ${i} of ${total}`
        pdf.text(left, margin, pageHeight - 16)
        const rw = pdf.getTextWidth(right)
        pdf.text(right, pageWidth - margin - rw, pageHeight - 16)
      }

      // Save file
      const sanitize = (s: string) => s.replace(/[^A-Za-z0-9_\-]+/g, '_')
      const fname = `RAN_Quality_Report_${sanitize(siteId)}_${sanitize(inputDate || 'NA')}_${sanitize(lastDate || 'NA')}.pdf`
      pdf.save(fname)
    } catch (e) {
      alert(`Failed to generate outputs PDF: ${e}`)
    } finally {
      setDownloadingOutputsPdf(false)
    }
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


  // Trigger evaluation only when a selected site and a valid date have been chosen
  useEffect(() => {
    const s = selectedSite?.trim()
    if (!s || !isValidSite || !isValidEvalDate) { setEvalResult(null); return }
    const input_date = evalDate
    let cancelled = false
    setEvalLoading(true)
    setEvalError(null)
    api.evaluate({ site_att: s, input_date, threshold: evalThreshold, period: evalPeriod, guard: evalGuard, radius_km: radiusKm, vecinos: vecinos })
      .then(res => { if (!cancelled) setEvalResult(res) })
      .catch(err => { if (!cancelled) setEvalError(String(err)) })
      .finally(() => { if (!cancelled) setEvalLoading(false) })
    return () => { cancelled = true }
  }, [selectedSite, evalThreshold, evalPeriod, evalGuard, evalDate, isValidEvalDate, isValidSite, radiusKm])

  // Tie chart loading spinners to evaluation loading state
  useEffect(() => {
    if (!isValidSite || !isValidEvalDate) return
    setFetchedSiteOnce(true)
    setFetchedNbOnce(true)
    setLoadingSite(evalLoading)
    setLoadingNb(evalLoading)
  }, [evalLoading, isValidSite, isValidEvalDate])


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

  // Validate the selected site against backend; only then allow API calls
  useEffectReact(() => {
    const s = selectedSite?.trim()
    if (!s || s.length < 2) { setIsValidSite(false); return }
    let cancelled = false
    ;(async () => {
      try {
        // If ranges returns successfully, consider the site valid
        await api.ranges(s)
        if (!cancelled) setIsValidSite(true)
      } catch {
        if (!cancelled) setIsValidSite(false)
      }
    })()
    return () => { cancelled = true }
  }, [selectedSite])

  // Fetch event dates when a valid site is selected
  useEffectReact(() => {
    const s = selectedSite?.trim()
    if (!s || !isValidSite) { setEventDates([]); return }
    let cancelled = false
    ;(async () => {
      try {
        setEventDatesLoading(true)
        setEventDatesError(null)
        const rows = await api.eventDates(s, { limit: 200 })
        if (cancelled) return
        setEventDates(Array.isArray(rows) ? rows : [])
      } catch (e: any) {
        if (!cancelled) setEventDatesError(String(e))
      } finally {
        if (!cancelled) setEventDatesLoading(false)
      }
    })()
    return () => { cancelled = true }
  }, [selectedSite, isValidSite])

  // Track whether we've fetched at least once to decide loader vs empty-state
  const [fetchedSiteOnce, setFetchedSiteOnce] = useState(false)
  const [fetchedNbOnce, setFetchedNbOnce] = useState(false)

  // Fetch neighbor site details when opening modal
  useEffectReact(() => {
    const s = selectedSite?.trim()
    if (!showSitesModal || !s || !isValidSite) return
    let cancelled = false
    ;(async () => {
      try {
        setNbSitesLoading(true)
        setNbSitesError(null)
        const rows = await api.neighborsList(s, { radius_km: radiusKm })
        if (cancelled) return
        setNbSites(Array.isArray(rows) ? dedupeNbSites(rows) : [])
      } catch (e: any) {
        if (!cancelled) setNbSitesError(String(e))
      } finally {
        if (!cancelled) setNbSitesLoading(false)
      }
    })()
    return () => { cancelled = true }
  }, [showSitesModal, selectedSite, radiusKm, isValidSite])

  function exportNeighborsCsv() {
    if (!nbSites || nbSites.length === 0) return
    const headers = ['site_name','region','province','municipality','vendor']
    const esc = (v: any) => {
      if (v == null) return ''
      const s = String(v)
      if (/[",\n]/.test(s)) return '"' + s.replace(/"/g, '""') + '"'
      return s
    }
    const rows = nbSites.map(r => [r.site_name, r.region ?? '', r.province ?? '', r.municipality ?? '', r.vendor ?? ''])
    const csv = [headers.join(','), ...rows.map(r => r.map(esc).join(','))].join('\n')
    const blob = new Blob(["\uFEFF" + csv], { type: 'text/csv;charset=utf-8;' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    const fname = `neighbors_${(selectedSite||'site').replace(/[^A-Za-z0-9_\-]+/g,'_')}_r${radiusKm}km.csv`
    a.href = url
    a.download = fname
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    URL.revokeObjectURL(url)
  }

  // Derive site and neighbor datasets from evaluate().data
  // Helper: extract a canonical time key from a row
  function timeKeyOf(row: any): string | null {
    if (!row || typeof row !== 'object') return null
    const ks = ['date','time','day','timestamp']
    for (const k of ks) { if (row[k] != null) return String(row[k]) }
    return null
  }
  // Helper: combine window buckets (before/after/between/mid/last) into a single deduped array by time key
  function combineWindows(win?: { [k: string]: any[] }): any[] {
    if (!win) return []
    const order = ['before','after','between','mid','last']
    const map = new Map<string, any>()
    for (const w of order) {
      const arr = Array.isArray((win as any)[w]) ? (win as any)[w] : []
      for (const r of arr) {
        const t = timeKeyOf(r)
        if (!t) continue
        // merge shallowly, later windows overwrite to ensure latest value per timestamp
        const prev = map.get(t) || {}
        const merged = { ...prev, ...r }
        // Ensure both common x keys are present so charts can snap correctly
        if (merged.date == null) merged.date = t
        if (merged.time == null) merged.time = t
        map.set(t, merged)
      }
    }
    // sort by time ascending if parseable
    const rows = Array.from(map.entries()).map(([t, obj]) => ({ ...obj, date: obj.date ?? t, time: obj.time ?? t }))
    rows.sort((a, b) => {
      const ta = Date.parse(a.date || a.time || a.day || a.timestamp || '')
      const tb = Date.parse(b.date || b.time || b.day || b.timestamp || '')
      if (Number.isNaN(ta) || Number.isNaN(tb)) return String(a.date).localeCompare(String(b.date))
      return ta - tb
    })
    // Ensure daily continuity so chart snapping hits exact event/guard dates
    function fillDaily(rr: any[]): any[] {
      if (!rr.length) return rr
      const parse = (s: string) => Date.parse(s)
      const fmt = (t: number) => new Date(t).toISOString().slice(0,10)
      const startT = parse(rr[0].date)
      const endT = parse(rr[rr.length - 1].date)
      if (Number.isNaN(startT) || Number.isNaN(endT)) return rr
      const have = new Set<string>(rr.map(r => String(r.date || r.time)))
      const out = [...rr]
      for (let t = startT; t <= endT; t += 24*3600*1000) {
        const d = fmt(t)
        if (!have.has(d)) out.push({ date: d, time: d })
      }
      out.sort((a, b) => String(a.date).localeCompare(String(b.date)))
      return out
    }
    return fillDaily(rows)
  }
  // Helper: build CQI merged series (columns cqi_3g, cqi_4g, cqi_5g)
  function buildCqiMerged(scope: 'site'|'neighbors', data: any): any[] {
    const root = data?.[scope]?.cqi || {}
    const win3 = root?.['3G'] as any || {}
    const win4 = root?.['4G'] as any || {}
    const win5 = root?.['5G'] as any || {}
    const arr3 = combineWindows(win3)
    const arr4 = combineWindows(win4)
    const arr5 = combineWindows(win5)
    const map = new Map<string, any>()
    function mergeArr(arr: any[], col: 'cqi_3g'|'cqi_4g'|'cqi_5g') {
      for (const r of arr) {
        const t = timeKeyOf(r)
        if (!t) continue
        const prev = map.get(t) || {}
        // Try to locate a CQI-like numeric field from row; accept first numeric value
        let val: number | undefined = undefined
        for (const [k, v] of Object.entries(r)) {
          if (typeof v === 'number' && /(cqi|lte|nr|umts|3g|4g|5g)/i.test(k)) { val = v; break }
        }
        // If not found, attempt common keys explicitly
        if (val == null) {
          const keys = ['lte_cqi','nr_cqi','umts_cqi','cqi','avg_cqi','mean_cqi']
          for (const k of keys) { if (typeof (r as any)[k] === 'number') { val = (r as any)[k]; break } }
        }
        map.set(t, { ...prev, date: t, [col]: val })
      }
    }
    mergeArr(arr3, 'cqi_3g'); mergeArr(arr4, 'cqi_4g'); mergeArr(arr5, 'cqi_5g')
    const rows = Array.from(map.values())
    rows.sort((a, b) => Date.parse(a.date) - Date.parse(b.date))
    return rows
  }

  useEffectReact(() => {
    const d = (evalResult as any)?.data
    if (!d) {
      // when no data, clear charts but keep previous fetched flags
      setSiteCqi([]); setSiteTraffic([]); setSiteVoice([])
      setNbCqi([]); setNbTraffic([]); setNbVoice([]); setMapGeo([])
      return
    }
    try {
      setFetchedSiteOnce(true); setFetchedNbOnce(true)
      setLoadingSite(false); setLoadingNb(false)
      // Geo
      setMapGeo(Array.isArray(d?.neighbors?.geo) ? d.neighbors.geo : [])
      // Site datasets
      const siteTrafficRows = combineWindows(d?.site?.traffic?.total)
      const siteVoiceRows = combineWindows(d?.site?.voice?.total)
      setSiteTraffic(asTrafficByTech(siteTrafficRows))
      setSiteVoice(asVoice3GVolte(siteVoiceRows))
      setSiteCqi(buildCqiMerged('site', d))
      // Neighbor datasets
      const nbTrafficRows = combineWindows(d?.neighbors?.traffic?.total)
      const nbVoiceRows = combineWindows(d?.neighbors?.voice?.total)
      setNbTraffic(asTrafficByTech(nbTrafficRows))
      setNbVoice(asVoice3GVolte(nbVoiceRows))
      setNbCqi(buildCqiMerged('neighbors', d))
    } catch (e) {
      setError(String(e))
    }
  }, [evalResult])

  // Previous neighbor fetch effect removed; neighbors derived from evaluate().data

  return (
    <StatsigProvider client={client} loadingComponent={<div>Loading...</div>}>
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
          {!selectedSite && (
            <div className="callout start-callout" role="status" aria-live="polite">
              <div className="callout-title">Start by choosing a site</div>
              <div className="callout-body">
                Enter a Site ATT code in the box below and pick one of the suggestions to load data.
              </div>
              <div className="callout-actions">
                <button className="button-show" type="button" onClick={() => siteInputRef.current?.focus()}>
                  Search site
                </button>
              </div>
            </div>
          )}

          <section className="panel controls">
            <h2>Controls</h2>
            <div className="form-row">
              <label className="field">
                <span>Site ATT</span>
                <input
                  ref={siteInputRef}
                  value={site}
                  onChange={(e) => { setSite(e.target.value); setSelectedSite(''); setIsValidSite(false); }}
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
                            onClick={() => { setSite(s); setSelectedSite(s); setSiteSuggestions([]) }}
                          >
                            {s}
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                )}
              </label>

              <label className="field date-field">
                <span>Input date</span>
                <div className="date-input-wrap">
                  <input
                    ref={evalDateInputRef}
                    type="date"
                    value={evalDate}
                    onChange={(e) => { setEvalDate(e.target.value) }}
                  />
                  <button
                    type="button"
                    className="date-picker-button"
                    aria-label="Open calendar"
                    title="Open calendar"
                    onClick={openNativeDatePicker}
                  />
                </div>
                <div style={{ marginTop: 6 }}>
                  <button
                    className="btn-grid btn-primary button-show button-small"
                    type="button"
                    onClick={() => setShowEventDatesModal(true)}
                    disabled={!isValidSite}
                    title={isValidSite ? 'Open event dates' : 'Select a valid site first'}
                  >
                    Suggested event dates…
                  </button>
                </div>
              </label>
              <label className="field">
                <span>Neighbours (sites)</span>
                <textarea
                  rows={5}
                  value={vecinos}
                  onChange={(e) => { setVecinos(e.target.value); setRadiusKm(0) }}
                />
              </label>
              <label className="field">
                <span>Radius (km)</span>
                <input
                  type="number"
                  min={0.1}
                  step={0.1}
                  value={radiusKm}
                  onChange={(e) => { setRadiusKm(parseFloat(e.target.value)); setVecinos("") }}
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

          <section className="panel outputs" ref={outputsRef}>
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
                          onClick={handleDownloadOutputsPdf}
                          disabled={downloadingOutputsPdf}
                          title="Generate PDF"
                        >
                          {downloadingOutputsPdf ? 'Generating…' : 'Generate PDF'}
                        </button>
                      </span>
                    </div>
                    {evalResult.metrics?.length ? (
                      <div className="eval-metrics">
                        {(() => {
                          const siteMetrics = (evalResult.metrics || []).filter((m: any) => typeof m?.name === 'string' && m.name.startsWith('Site '))
                          const nbMetrics = (evalResult.metrics || []).filter((m: any) => typeof m?.name === 'string' && m.name.startsWith('Neighbors '))
                          const Table = ({ title, rows }: { title: string; rows: any[] }) => {
                            const fmtVal = (v: any) => (v != null && Number.isFinite(v)) ? (Number(v).toFixed(2)) : '—'
                            const fmtPct = (v: any) => (v != null && Number.isFinite(v)) ? `${(Number(v)*100).toFixed(1)}%` : '—'
                            return (
                              <div className="eval-col">
                                <div className="col-title">{title}</div>
                                <div className="mtable">
                                  <div className="thead">
                                    <div>Name</div>
                                    <div>Before</div>
                                    <div>After</div>
                                    <div>Last</div>
                                    <div>ΔAfter/Before</div>
                                    <div>ΔLast/Before</div>
                                    <div>Class</div>
                                    <div>Verdict</div>
                                  </div>
                                  <div className="tbody">
                                    {rows.map((m: any, i: number) => (
                                      <div className="trow" key={i}>
                                        <div className="cell name">{(m.name || '').replace(/^(Site|Neighbors)\s+/, '')}</div>
                                        <div className="cell num">{fmtVal(m.before_mean)}</div>
                                        <div className="cell num">{fmtVal(m.after_mean)}</div>
                                        <div className="cell num">{fmtVal(m.last_mean)}</div>
                                        <div className="cell num">{fmtPct(m.delta_after_before)}</div>
                                        <div className="cell num">{fmtPct(m.delta_last_before)}</div>
                                        <div className="cell">{m.klass || '—'}</div>
                                        <div className="cell"><span className={`vbadge vb-${(m.verdict||'inconclusive').toLowerCase()}`}>{m.verdict || 'Inconclusive'}</span></div>
                                      </div>
                                    ))}
                                  </div>
                                </div>
                              </div>
                            )
                          }
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
                const mkLastRegion = (r?: {from?: string; to?: string}) => (r?.from && r?.to ? { from: r.from, to: r.to, stroke: regionStroke, strokeDasharray: '6 6', strokeWidth: 2, fill: regionFill, fillOpacity: regionFillOpacity } : null)
                const regions = [mkRegion(ranges?.before), mkRegion(ranges?.after), mkLastRegion(ranges?.last)].filter(Boolean) as any[]
                const common = { xMin, xMax, vLines, regions }
                return (
                  <>
                    <div className="output-block">
                      <div className="block-title">Plot01 – Site CQIs</div>
                      <SimpleLineChart data={siteCqi} title="Site CQIs" loading={loadingSite && fetchedSiteOnce} {...common} />
                    </div>
                    <div className="output-block">
                      <div className="block-title">Plot02 – Site Data Traffic</div>
                      <SimpleStackedBar data={siteTraffic} title="Site Data Traffic" loading={loadingSite && fetchedSiteOnce} {...common} />
                    </div>
                    <div className="output-block">
                      <div className="block-title">Plot03 – Site Voice Traffic</div>
                      <SimpleStackedBar data={siteVoice} title="Site Voice Traffic" loading={loadingSite && fetchedSiteOnce} {...common} />
                    </div>
                    <div className="output-block block-map">
                      <div className="block-title">Plot04 – Map</div>
                      {loadingNb && fetchedNbOnce ? (
                        <div className="chart-loading" style={{ height: 360 }} />
                      ) : mapGeo.length > 0 ? (
                        <div className="map-wrap">
                          <MapContainer
                            ref={mapRef}
                            center={mapCenter}
                            zoom={13}
                            style={{ height: '100%', width: '100%' }}
                            preferCanvas={true}
                            zoomAnimation={false}
                            whenReady={() => {
                              try {
                                if (!mapCenter || !Array.isArray(mapCenter) || mapCenter.length !== 2) return
                                const [lat, lng] = mapCenter as [number, number]
                                const r = (radiusKm || 0) * 1000
                                if (!r || r <= 0) return
                                const dLat = r / 111320
                                const cosLat = Math.max(Math.cos(lat * Math.PI / 180), 1e-6)
                                const dLng = r / (111320 * cosLat)
                                const south = lat - dLat
                                const north = lat + dLat
                                const west = lng - dLng
                                const east = lng + dLng
                                // @ts-ignore
                                mapRef.current?.fitBounds([[south, west], [north, east]], { padding: [20, 20], animate: false })
                              } catch {}
                            }}
                          >
                            <TileLayer
                              attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                              url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                              crossOrigin="anonymous"
                              detectRetina={false}
                            />
                            {/* radius circle for context (render first so it's beneath markers) */}
                            <Circle center={mapCenter} radius={radiusKm * 1000} pathOptions={{ color: '#9aa0a6' }} interactive={false} />
                            {mapGeo.map((g, idx) => (
                              g.role === 'center' ? (
                                <CircleMarker key={`c-${idx}`} center={[g.latitude, g.longitude]} radius={8} pathOptions={{ color: '#34c759' }}>
                                  <Tooltip permanent direction="bottom" offset={[0, 10]} opacity={0.85} className="map-label">{g.att_name}</Tooltip>
                                  <Popup>Center: {g.att_name}</Popup>
                                </CircleMarker>
                              ) : (
                                <CircleMarker key={`n-${idx}`} center={[g.latitude, g.longitude]} radius={6} pathOptions={{ color: '#f4c20d' }}>
                                  <Tooltip permanent direction="bottom" offset={[0, 10]} opacity={0.85} className="map-label">{g.att_name}</Tooltip>
                                  <Popup>Neighbor: {g.att_name}</Popup>
                                </CircleMarker>
                              )
                            ))}
                          </MapContainer>
                        </div>
                      ) : (
                        <div className="chart-empty" style={{ height: 360 }}>No neighbor geometry available. Choose a valid site and radius.</div>
                      )}
                      <div style={{ marginTop: 8 }}>
                        <button className="btn-grid button-show" onClick={() => setShowSitesModal(true)}>Show sites</button>
                      </div>
                    </div>
                    <div className="output-block" id="plot05">
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
              <div className="modal modal-wide" onClick={(e) => e.stopPropagation()}>
                <div className="modal-title">Neighbor Sites</div>
                <div className="modal-body">
                  {nbSitesLoading ? (
                    <div className="note">Loading…</div>
                  ) : nbSitesError ? (
                    <div className="note error">{nbSitesError}</div>
                  ) : nbSites.length === 0 ? (
                    <div className="note muted">No neighbor sites found.</div>
                  ) : (
                    <div className="event-table neighbors">
                      <div className="thead">
                        <div>Site name</div>
                        <div>Region</div>
                        <div>Province</div>
                        <div>Municipality</div>
                        <div>Vendor</div>
                      </div>
                      <div className="tbody">
                        {nbSites.map((r, i) => (
                          <div className="trow" key={i}>
                            <div className="cell">{r.site_name}</div>
                            <div className="cell">{r.region || '—'}</div>
                            <div className="cell">{r.province || '—'}</div>
                            <div className="cell">{r.municipality || '—'}</div>
                            <div className="cell">{r.vendor || '—'}</div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
                <div className="modal-actions">
                  <button className="theme-toggle" onClick={exportNeighborsCsv} disabled={nbSitesLoading || nbSites.length===0}>Export CSV</button>
                  <button className="theme-toggle" onClick={() => setShowSitesModal(false)}>Close</button>
                </div>
              </div>
            </div>
          )}
          {showEventDatesModal && (
            <div className="modal-backdrop" onClick={() => setShowEventDatesModal(false)}>
              <div className="modal modal-wide" onClick={(e) => e.stopPropagation()}>
                <div className="modal-title">Event Dates</div>
                <div className="modal-body">
                  {eventDatesLoading ? (
                    <div className="note">Loading…</div>
                  ) : eventDatesError ? (
                    <div className="note error">{eventDatesError}</div>
                  ) : eventDates.length === 0 ? (
                    <div className="note muted">No events found.</div>
                  ) : (
                    <div className="event-table">
                      <div className="thead">
                        <div>Tech</div>
                        <div>Date</div>
                        <div>+Cells</div>
                        <div>-Cells</div>
                        <div>Total</div>
                        <div>Remark</div>
                      </div>
                      <div className="tbody">
                        {eventDates.slice(0, 500).map((r, i) => {
                          const tClass = r.tech === '4G' ? 't4' : 't3'
                          return (
                            <div
                              key={`${r.tech}-${r.date}-${i}`}
                              className="trow clickable"
                              onClick={() => { setEvalDate(r.date); setShowEventDatesModal(false) }}
                              title="Use this date for evaluation"
                            >
                              <div className="cell badge"><span className={`badge-tech ${tClass}`}>{r.tech}</span></div>
                              <div className="cell">{r.date}</div>
                              <div className="cell num">{r.add_cell ?? '—'}</div>
                              <div className="cell num">{r.delete_cell ?? '—'}</div>
                              <div className="cell num">{r.total_cell ?? '—'}</div>
                              <div className="cell remark" title={r.remark ?? ''}>{r.remark ?? '—'}</div>
                            </div>
                          )
                        })}
                      </div>
                    </div>
                  )}
                </div>
                <div className="modal-actions">
                  <button className="theme-toggle" onClick={() => setShowEventDatesModal(false)}>Close</button>
                </div>
              </div>
            </div>
          )}
        </main>
      </div>
    </StatsigProvider>
  )
}

export default App
