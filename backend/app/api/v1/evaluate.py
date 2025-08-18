from datetime import date, timedelta
from typing import List, Literal, Optional, Tuple, Callable, Any, Dict
import time
import concurrent.futures

import numpy as np
import pandas as pd
from fastapi import APIRouter
from pydantic import BaseModel, Field

from .sites import df_json_records
from cell_change_evolution.select_db_master_node import get_max_date
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

router = APIRouter(prefix="/evaluate", tags=["evaluate"])

# ----- Models -----
class EvaluateRequest(BaseModel):
    site_att: str = Field(..., description="Site ATT identifier")
    input_date: date = Field(..., description="Reference input date")
    threshold: float = Field(0.05, ge=0.0, le=1.0, description="Delta threshold as fraction, default 0.05 (5%)")
    period: int = Field(7, ge=1, le=90, description="Period window in days")
    guard: int = Field(7, ge=0, le=90, description="Guard window in days")
    radius_km: float = Field(5.0, ge=0.1, le=50, description="Neighbor aggregation radius in km")
    debug: bool = Field(False, description="Include debug timings in response")

MetricClass = Literal[
    "UpUp", "UpFlat", "UpDown",
    "FlatUp", "FlatFlat", "FlatDown",
    "DownUp", "DownFlat", "DownDown",
]

class MetricEvaluation(BaseModel):
    name: str
    before_mean: Optional[float]
    after_mean: Optional[float]
    last_mean: Optional[float]
    delta_after_before: Optional[float]
    delta_last_after: Optional[float]
    klass: Optional[MetricClass]
    verdict: Optional[Literal["Pass", "Fail", "Restored", "Inconclusive"]]

class EvaluateResponse(BaseModel):
    site_att: str
    input_date: date
    options: dict
    overall: Optional[Literal["Pass", "Fail", "Restored", "Inconclusive"]]
    metrics: List[MetricEvaluation]


# ----- Helpers -----
def _date_str(d: Optional[date]) -> Optional[str]:
    return str(d) if d else None


def _range_mean(df: Optional[pd.DataFrame], preferred_cols: List[str]) -> Optional[float]:
    if df is None or df.empty:
        return None
    # Normalize: replace inf -> nan; ensure numeric
    df = df.replace([np.inf, -np.inf], np.nan)
    # Silence future downcasting change by explicitly inferring dtypes
    try:
        df = df.infer_objects(copy=False)
    except Exception:
        pass
    # Try preferred columns in order
    for col in preferred_cols:
        if col in df.columns:
            s = pd.to_numeric(df[col], errors="coerce")
            val = float(s.mean()) if s.notna().any() else None
            if val is not None and not np.isnan(val):
                return val
    # Fallback: mean of all numeric columns
    num_df = df.select_dtypes(include=[np.number])
    if num_df.empty:
        # try coercing any that look numeric
        num_df = df.apply(pd.to_numeric, errors="ignore")
        num_df = num_df.select_dtypes(include=[np.number])
    if num_df.empty:
        return None
    m = float(num_df.mean(numeric_only=True).mean())
    return None if np.isnan(m) else m


def _delta(a: Optional[float], b: Optional[float]) -> Optional[float]:
    # delta = (a - b) / max(|b|, eps) where a is newer period
    if a is None or b is None:
        return None
    eps = 1e-9
    denom = max(abs(b), eps)
    return (a - b) / denom


def _bucket(d: Optional[float], thr: float) -> str:
    if d is None:
        return "Flat"
    if d >= thr:
        return "Up"
    if d <= -thr:
        return "Down"
    return "Flat"


def _classify(delta_ab: Optional[float], delta_la: Optional[float], thr: float) -> Tuple[MetricClass, str]:
    a = _bucket(delta_ab, thr)
    b = _bucket(delta_la, thr)
    klass: MetricClass = f"{a}{b}"  # type: ignore[assignment]
    # Verdict per §6
    if a == "Down" and b == "Up":
        verdict = "Restored"
    elif a == "Down" and b in ("Flat", "Down"):
        verdict = "Fail"
    elif a in ("Up", "Flat") and b in ("Up", "Flat"):
        verdict = "Pass"
    else:
        verdict = "Inconclusive"
    return klass, verdict


def _call_with_timeout(fn: Callable[..., Any], timeout_s: float, *args, **kwargs) -> Any:
    """Run blocking DB selector with a timeout to avoid hanging requests."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(fn, *args, **kwargs)
        try:
            return fut.result(timeout=timeout_s)
        except concurrent.futures.TimeoutError:
            print(f"[evaluate] Timeout: {getattr(fn, '__name__', str(fn))} exceeded {timeout_s}s")
            return None
        except Exception as e:
            print(f"[evaluate] Error in {getattr(fn, '__name__', str(fn))}: {e}")
            return None


def _compute_range(site_att: str, tech: Optional[str], start: Optional[date], end: Optional[date], metric: str, radius_km: float, timings: Optional[Dict[str, float]] = None) -> Optional[float]:
    # metric keys: 'site_cqi', 'site_data', 'site_voice', 'nb_cqi', 'nb_data', 'nb_voice'
    frm = _date_str(start)
    to = _date_str(end)
    t0 = time.perf_counter()
    if metric == 'site_cqi':
        df = _call_with_timeout(get_cqi_daily, 4.0, att_name=site_att, min_date=frm, max_date=to, technology=tech)
        # select_db_cqi_daily outputs: umts_cqi, lte_cqi, nr_cqi
        val = _range_mean(df, preferred_cols=['umts_cqi', 'lte_cqi', 'nr_cqi'])
        if timings is not None:
            timings[f"{metric}:{tech}:{frm}:{to}"] = time.perf_counter() - t0
        return val
    if metric == 'site_data':
        df = _call_with_timeout(get_traffic_data_daily, 4.0, att_name=site_att, min_date=frm, max_date=to, technology=tech, vendor=None)
        val = _range_mean(df, preferred_cols=['ps_gb_uldl', 'traffic_dlul_tb'])
        if timings is not None:
            timings[f"{metric}:{tech}:{frm}:{to}"] = time.perf_counter() - t0
        return val
    if metric == 'site_voice':
        df = _call_with_timeout(get_traffic_voice_daily, 4.0, att_name=site_att, min_date=frm, max_date=to, technology=tech, vendor=None)
        val = _range_mean(df, preferred_cols=['traffic_voice'])
        if timings is not None:
            timings[f"{metric}:{tech}:{frm}:{to}"] = time.perf_counter() - t0
        return val
    if metric == 'nb_cqi':
        df = _call_with_timeout(get_neighbor_cqi_daily, 4.0, site_list=site_att, min_date=frm, max_date=to, technology=tech, radius_km=radius_km)
        val = _range_mean(df, preferred_cols=['umts_cqi', 'lte_cqi', 'nr_cqi'])
        if timings is not None:
            timings[f"{metric}:{tech}:{frm}:{to}"] = time.perf_counter() - t0
        return val
    if metric == 'nb_data':
        df = _call_with_timeout(get_neighbor_traffic_data, 4.0, site_list=site_att, min_date=frm, max_date=to, technology=tech, radius_km=radius_km, vendor=None)
        val = _range_mean(df, preferred_cols=['ps_gb_uldl', 'traffic_dlul_tb'])
        if timings is not None:
            timings[f"{metric}:{tech}:{frm}:{to}"] = time.perf_counter() - t0
        return val
    if metric == 'nb_voice':
        df = _call_with_timeout(get_neighbor_traffic_voice, 4.0, site_list=site_att, min_date=frm, max_date=to, technology=tech, radius_km=radius_km, vendor=None)
        val = _range_mean(df, preferred_cols=['traffic_voice'])
        if timings is not None:
            timings[f"{metric}:{tech}:{frm}:{to}"] = time.perf_counter() - t0
        return val
    return None


@router.post("")
def evaluate(req: EvaluateRequest) -> EvaluateResponse:
    # Define windows per §6
    max_d = _call_with_timeout(get_max_date, 3.0)
    max_date_source = "db"
    # Fallback: if DB global max is unavailable, try deriving from this site's available data within a bounded window
    if not max_d:
        span = 120  # days around input date to probe
        probe_min = req.input_date - timedelta(days=span)
        probe_max = req.input_date + timedelta(days=span)
        candidates: list = []
        # We purposefully keep tight timeouts to avoid stalls
        try:
            df1 = _call_with_timeout(get_cqi_daily, 3.5, att_name=req.site_att, min_date=str(probe_min), max_date=str(probe_max), technology=None)
            if isinstance(df1, pd.DataFrame) and not df1.empty and 'time' in df1.columns:
                tmax = pd.to_datetime(df1['time'], errors='coerce').max()
                if pd.notna(tmax):
                    candidates.append(tmax.date())
        except Exception:
            pass
        try:
            df2 = _call_with_timeout(get_traffic_data_daily, 3.5, att_name=req.site_att, min_date=str(probe_min), max_date=str(probe_max), technology=None, vendor=None)
            if isinstance(df2, pd.DataFrame) and not df2.empty and 'time' in df2.columns:
                tmax = pd.to_datetime(df2['time'], errors='coerce').max()
                if pd.notna(tmax):
                    candidates.append(tmax.date())
        except Exception:
            pass
        try:
            df3 = _call_with_timeout(get_traffic_voice_daily, 3.5, att_name=req.site_att, min_date=str(probe_min), max_date=str(probe_max), technology=None, vendor=None)
            if isinstance(df3, pd.DataFrame) and not df3.empty and 'time' in df3.columns:
                tmax = pd.to_datetime(df3['time'], errors='coerce').max()
                if pd.notna(tmax):
                    candidates.append(tmax.date())
        except Exception:
            pass
        if candidates:
            max_d = max(candidates)
            max_date_source = "site_fallback"
        else:
            max_date_source = "none"
    before_start = req.input_date - timedelta(days=req.guard + req.period)
    before_end = req.input_date - timedelta(days=req.guard)
    after_start = req.input_date + timedelta(days=req.guard)
    after_end = req.input_date + timedelta(days=req.guard + req.period)
    last_end = max_d
    last_start = max_d - timedelta(days=req.period) if max_d else None

    # Metric plan: (name, metric_key, tech)
    plan: List[Tuple[str, str, Optional[str]]] = []
    for tech in ("3G", "4G", "5G"):
        plan.append((f"Site CQI {tech}", 'site_cqi', tech))
    for tech in ("3G", "4G", "5G"):
        plan.append((f"Site Data {tech}", 'site_data', tech))
    for tech in ("3G", "4G"):
        plan.append((f"Site Voice {tech}", 'site_voice', tech))
    for tech in ("3G", "4G", "5G"):
        plan.append((f"Neighbors CQI {tech}", 'nb_cqi', tech))
    for tech in ("3G", "4G", "5G"):
        plan.append((f"Neighbors Data {tech}", 'nb_data', tech))
    for tech in ("3G", "4G"):
        plan.append((f"Neighbors Voice {tech}", 'nb_voice', tech))

    metrics: List[MetricEvaluation] = []
    debug_timings: Dict[str, float] = {}

    # Build parallel tasks for each metric/window
    Task = Tuple[str, str, Optional[str], str]  # (name, mkey, tech, window)
    tasks: List[Task] = []
    for name, mkey, tech in plan:
        tasks.append((name, mkey, tech, 'before'))
        tasks.append((name, mkey, tech, 'after'))
        if last_start and last_end:
            tasks.append((name, mkey, tech, 'last'))

    def run_task(task: Task) -> Tuple[Task, Optional[float], float]:
        name, mkey, tech, window = task
        if window == 'before':
            s, e = before_start, before_end
        elif window == 'after':
            s, e = after_start, after_end
        else:
            s, e = last_start, last_end
        t0 = time.perf_counter()
        val = _compute_range(req.site_att, tech, s, e, mkey, req.radius_km, None)
        elapsed = time.perf_counter() - t0
        return task, val, elapsed

    # Global time budget to return partial results
    GLOBAL_BUDGET_S = 25.0
    start_time = time.perf_counter()
    results: Dict[Task, Optional[float]] = {}

    ex = concurrent.futures.ThreadPoolExecutor(max_workers=8)
    try:
        future_map = {ex.submit(run_task, t): t for t in tasks}
        for fut in concurrent.futures.as_completed(future_map):
            task, val, elapsed = fut.result()
            results[task] = val
            if req.debug:
                name, mkey, tech, window = task
                debug_timings[f"{mkey}:{tech}:{window}"] = elapsed
            if time.perf_counter() - start_time > GLOBAL_BUDGET_S:
                print("[evaluate] Global time budget exceeded; returning partial results")
                # Cancel remaining tasks and shutdown executor without waiting
                for f in future_map.keys():
                    if not f.done():
                        f.cancel()
                ex.shutdown(wait=False, cancel_futures=True)
                break
    finally:
        # If not already shut down, shut down quickly without waiting for lingering tasks
        try:
            ex.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass

    # Assemble metric entries
    for name, mkey, tech in plan:
        b = results.get((name, mkey, tech, 'before'))
        a = results.get((name, mkey, tech, 'after'))
        l = results.get((name, mkey, tech, 'last')) if (last_start and last_end) else None
        d_ab = _delta(a, b)
        d_la = _delta(l, a)
        klass, verdict = _classify(d_ab, d_la, req.threshold)
        metrics.append(MetricEvaluation(
            name=name,
            before_mean=b,
            after_mean=a,
            last_mean=l,
            delta_after_before=d_ab,
            delta_last_after=d_la,
            klass=klass,
            verdict=verdict,  # type: ignore[arg-type]
        ))

    # Overall roll-up: Pass if all Pass; Restored if any Restored and rest Pass; Fail otherwise
    vlist = [m.verdict or "Inconclusive" for m in metrics]
    if all(v == "Pass" for v in vlist):
        overall = "Pass"
    elif any(v == "Restored" for v in vlist) and all(v in ("Pass", "Restored") for v in vlist):
        overall = "Restored"
    elif any(v == "Fail" for v in vlist):
        overall = "Fail"
    else:
        overall = "Inconclusive"

    return EvaluateResponse(
        site_att=req.site_att,
        input_date=req.input_date,
        options={
            "threshold": req.threshold,
            "period": req.period,
            "guard": req.guard,
            "radius_km": req.radius_km,
            "ranges": {
                "before": {"from": str(before_start), "to": str(before_end)},
                "after": {"from": str(after_start), "to": str(after_end)},
                "last": {"from": str(last_start) if last_start else None, "to": str(last_end) if last_end else None},
            },
            "global_budget_s": 25.0,
            "partial": len({k: v for k, v in results.items() if v is not None}) < len(tasks),
            **({"debug_timings": debug_timings, "max_date_source": max_date_source} if req.debug else {}),
        },
        overall=overall,  # type: ignore[arg-type]
        metrics=metrics,
    )
