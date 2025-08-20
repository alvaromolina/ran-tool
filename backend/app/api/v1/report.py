from datetime import datetime
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field
from typing import Optional, List, Tuple
import io
import base64
import pandas as pd

from .evaluate import EvaluateRequest, evaluate
from cell_change_evolution.select_db_cqi_daily import (
    get_cqi_daily,
    get_traffic_data_daily,
    get_traffic_voice_daily,
)
from cell_change_evolution.select_db_neighbor_cqi_daily import (
    get_neighbor_cqi_daily,
    get_neighbor_traffic_data,
    get_neighbor_traffic_voice,
)

router = APIRouter(prefix="/report", tags=["report"]) 

class ReportRequest(EvaluateRequest):
    include_debug: bool = Field(False, description="Include debug timings/metadata in the report")

HTML_STYLE = """
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, 'Noto Sans', 'Liberation Sans', sans-serif; font-size: 12px; color: #111; }
    h1 { font-size: 18px; margin: 0 0 6px 0; }
    h2 { font-size: 14px; margin: 14px 0 6px 0; }
    .meta { color: #555; font-size: 11px; }
    .badge { display: inline-block; padding: 2px 8px; border-radius: 999px; font-weight: 600; border: 1px solid #999; }
    .is-pass { background: #d1fae5; color: #064e3b; border-color: #a7f3d0; }
    .is-fail { background: #fee2e2; color: #7f1d1d; border-color: #fecaca; }
    .is-restored { background: #dbeafe; color: #1e3a8a; border-color: #bfdbfe; }
    .is-inconclusive { background: #e5e7eb; color: #111827; border-color: #e5e7eb; }
    table { width: 100%; border-collapse: collapse; }
    thead th { text-align: left; border-bottom: 1px solid #ddd; padding: 6px 4px; font-size: 11px; color: #444; }
    tbody td { padding: 6px 4px; border-bottom: 1px solid #f0f0f0; vertical-align: top; }
    .num { font-variant-numeric: tabular-nums; text-align: right; white-space: nowrap; }
    .small { font-size: 10px; color: #666; }
    .charts { margin-top: 14px; }
    .chart { page-break-inside: avoid; margin: 10px 0 18px 0; }
    .chart h3 { margin: 0 0 6px 0; font-size: 13px; }
    .chart img { width: 100%; height: auto; border: 1px solid #e5e7eb; }
  </style>
"""

def _parse_date(s: Optional[str]) -> Optional[pd.Timestamp]:
    try:
        return pd.to_datetime(s) if s else None
    except Exception:
        return None


def _mk_plot_image(df: Optional[pd.DataFrame], y_cols: List[str], title: str, ranges: dict) -> Optional[str]:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return None
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
    except Exception:
        return None
    # Prepare data
    df = df.copy()
    if 'time' in df.columns:
        df['__t'] = pd.to_datetime(df['time'], errors='coerce')
    elif 'date' in df.columns:
        df['__t'] = pd.to_datetime(df['date'], errors='coerce')
    else:
        return None
    df = df.dropna(subset=['__t']).sort_values('__t')
    if df.empty:
        return None
    # Plot
    fig, ax = plt.subplots(figsize=(7.5, 2.6), dpi=150)
    colors = ['#2563eb', '#16a34a', '#ca8a04', '#dc2626']
    for i, col in enumerate(y_cols):
        if col in df.columns:
            ax.plot(df['__t'], pd.to_numeric(df[col], errors='coerce'), label=col, color=colors[i % len(colors)], linewidth=1.4)
    # Shade ranges
    b = ranges.get('before') or {}
    a = ranges.get('after') or {}
    l = ranges.get('last') or {}
    def shade(rng, color, alpha):
        s = _parse_date((rng or {}).get('from'))
        e = _parse_date((rng or {}).get('to'))
        if s is not None and e is not None:
            ax.axvspan(s, e, color=color, alpha=alpha, linewidth=0)
    shade(b, '#3b82f6', 0.10)
    shade(a, '#22c55e', 0.10)
    shade(l, '#0ea5e9', 0.04)
    # Styling
    ax.grid(True, linestyle='--', linewidth=0.4, alpha=0.4)
    ax.set_title(title)
    ax.legend(loc='upper right', fontsize=8)
    fig.autofmt_xdate()
    buf = io.BytesIO()
    plt.tight_layout()
    fig.savefig(buf, format='png')
    plt.close(fig)
    data_uri = 'data:image/png;base64,' + base64.b64encode(buf.getvalue()).decode('ascii')
    return data_uri


def render_html(resp: dict, include_debug: bool, chart_images: List[Tuple[str, Optional[str]]]) -> str:
    overall = (resp.get("overall") or "Inconclusive").lower()
    opts = resp.get("options", {})
    ranges = opts.get("ranges", {})
    rows = resp.get("metrics", [])

    def fmt(x):
        if x is None:
            return "—"
        try:
            return f"{x:.4g}" if abs(x) >= 1 else f"{x:.4f}"
        except Exception:
            return str(x)

    def pct(x):
        if x is None:
            return "—"
        try:
            return f"{x*100:.1f}%"
        except Exception:
            return str(x)

    head = f"""
    <h1>RAN Quality Evaluation Report</h1>
    <div class=meta>
      Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}<br/>
      Site: <b>{resp.get('site_att','')}</b> &nbsp; | &nbsp; Input date: <b>{resp.get('input_date','')}</b><br/>
      Threshold: {opts.get('threshold')} &nbsp; Period: {opts.get('period')} &nbsp; Guard: {opts.get('guard')} &nbsp; Radius (km): {opts.get('radius_km')}
    </div>
    <h2>Overall Result</h2>
    <div><span class="badge is-{overall}">{resp.get('overall') or 'Inconclusive'}</span></div>
    <h2>Ranges</h2>
    <div class=small>
      Before: {ranges.get('before')}<br/>
      After: {ranges.get('after')}<br/>
      Last: {ranges.get('last')}<br/>
    </div>
    """

    tbl = [
        "<h2>Metric Details</h2>",
        "<table>",
        "<thead><tr>"
        "<th>Name</th><th class=num>Before</th><th class=num>After</th><th class=num>Last</th>"
        "<th class=num>Δ After/Before</th><th class=num>Δ Last/Before</th>"
        "<th>Class</th><th>Verdict</th>"
        "</tr></thead>",
        "<tbody>",
    ]
    for m in rows:
        tbl.append(
            "<tr>"
            f"<td>{m.get('name','')}</td>"
            f"<td class=num>{fmt(m.get('before_mean'))}</td>"
            f"<td class=num>{fmt(m.get('after_mean'))}</td>"
            f"<td class=num>{fmt(m.get('last_mean'))}</td>"
            f"<td class=num>{pct(m.get('delta_after_before'))}</td>"
            f"<td class=num>{pct(m.get('delta_last_before'))}</td>"
            f"<td>{m.get('klass','')}</td>"
            f"<td>{m.get('verdict','')}</td>"
            "</tr>"
        )
    tbl.append("</tbody></table>")

    debug_html = ""
    if include_debug:
        debug_html = f"""
        <h2>Debug</h2>
        <pre class=small>{opts}</pre>
        """

    # Charts section
    charts_html_parts: List[str] = []
    if chart_images:
        charts_html_parts.append('<h2>Charts</h2>')
        charts_html_parts.append('<div class="charts">')
        for title, img in chart_images:
            if not img:
                continue
            charts_html_parts.append(f'<div class="chart"><h3>{title}</h3><img src="{img}" alt="{title}"/></div>')
        charts_html_parts.append('</div>')
    charts_html = ''.join(charts_html_parts)

    html = f"<html><head>{HTML_STYLE}</head><body>{head}{''.join(tbl)}{charts_html}{debug_html}</body></html>"
    return html

@router.post("")
def create_report(req: ReportRequest):
    # Reuse evaluation logic
    try:
        resp_model = evaluate(req)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Evaluation failed: {e}")

    resp_dict = resp_model.model_dump() if hasattr(resp_model, 'model_dump') else resp_model  # pydantic v2/v1
    # Build charts (exclude map). Focus on 4G metrics to keep report concise.
    ranges = (resp_dict.get('options', {}) or {}).get('ranges', {}) or {}
    # Determine plotting window: from 'before.from' to max of after.to/last.to
    b_from = (ranges.get('before') or {}).get('from')
    a_to = (ranges.get('after') or {}).get('to')
    l_to = (ranges.get('last') or {}).get('to')
    start = b_from
    end = l_to or a_to

    def _site(series_fn, title: str, y_cols: List[str]) -> Tuple[str, Optional[str]]:
        try:
            df = series_fn(att_name=req.site_att, min_date=start, max_date=end, technology='4G')
        except Exception:
            df = None
        img = _mk_plot_image(df, y_cols, title, ranges)
        return title, img

    def _nb(series_fn, title: str, y_cols: List[str]) -> Tuple[str, Optional[str]]:
        try:
            df = series_fn(site_list=req.site_att, min_date=start, max_date=end, technology='4G', radius_km=req.radius_km)
        except Exception:
            df = None
        img = _mk_plot_image(df, y_cols, title, ranges)
        return title, img

    chart_images: List[Tuple[str, Optional[str]]] = []
    # Site charts (4G)
    chart_images.append(_site(get_cqi_daily, 'Site CQI 4G', ['lte_cqi']))
    chart_images.append(_site(
        get_traffic_data_daily,
        'Site Data Traffic 4G',
        ['h4g_traffic_d_user_ps_gb', 's4g_traffic_d_user_ps_gb', 'e4g_traffic_d_user_ps_gb', 'n4g_traffic_d_user_ps_gb']
    ))
    chart_images.append(_site(
        get_traffic_voice_daily,
        'Site Voice Traffic 4G',
        ['user_traffic_volte_e', 'user_traffic_volte_h', 'user_traffic_volte_n', 'user_traffic_volte_s']
    ))
    # Neighbor charts (4G)
    chart_images.append(_nb(get_neighbor_cqi_daily, 'Neighbors CQI 4G', ['lte_cqi']))
    chart_images.append(_nb(get_neighbor_traffic_data, 'Neighbors Data Traffic 4G', ['ps_gb_uldl', 'traffic_dlul_tb']))
    chart_images.append(_nb(get_neighbor_traffic_voice, 'Neighbors Voice Traffic 4G', ['traffic_voice']))

    html = render_html(resp_dict, include_debug=req.include_debug, chart_images=chart_images)

    # Try WeasyPrint
    try:
        from weasyprint import HTML
        pdf_bytes = HTML(string=html).write_pdf()
        buf = io.BytesIO(pdf_bytes)
        headers = {"Content-Disposition": f"attachment; filename=ran_evaluation_{resp_dict.get('site_att','site')}.pdf"}
        return StreamingResponse(buf, media_type="application/pdf", headers=headers)
    except Exception as e:
        # If WeasyPrint unavailable, return HTML with guidance
        msg = (
            "PDF generation failed. Ensure WeasyPrint is installed and system deps (Cairo, Pango, GDK-PixBuf) are available. "
            f"Error: {e}"
        )
        return JSONResponse(status_code=500, content={"error": msg, "html_preview": html})
