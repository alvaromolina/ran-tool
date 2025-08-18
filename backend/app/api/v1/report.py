from datetime import datetime
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field
from typing import Optional
import io

from .evaluate import EvaluateRequest, evaluate

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
  </style>
"""

def render_html(resp: dict, include_debug: bool) -> str:
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
        "<th class=num>Δ After/Before</th><th class=num>Δ Last/After</th>"
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
            f"<td class=num>{pct(m.get('delta_last_after'))}</td>"
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

    html = f"<html><head>{HTML_STYLE}</head><body>{head}{''.join(tbl)}{debug_html}</body></html>"
    return html

@router.post("")
def create_report(req: ReportRequest):
    # Reuse evaluation logic
    try:
        resp_model = evaluate(req)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Evaluation failed: {e}")

    resp_dict = resp_model.model_dump() if hasattr(resp_model, 'model_dump') else resp_model  # pydantic v2/v1
    html = render_html(resp_dict, include_debug=req.include_debug)

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
