from datetime import date, timedelta
from typing import List, Literal, Optional, Tuple, Callable, Any, Dict
import time
import concurrent.futures

import numpy as np
import pandas as pd
from fastapi import APIRouter
from pydantic import BaseModel, Field

from .sites import df_json_records, get_neighbors_geo
from cell_change_evolution.select_db_master_node import get_max_date
from cell_change_evolution.select_db_cqi_daily import (
    get_cqi_daily_calculated,
    get_traffic_data_daily,
    get_traffic_voice_daily,
)
from cell_change_evolution.select_db_neighbor_cqi_daily import (
    get_neighbor_cqi_daily_calculated,
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
    delta_last_before: Optional[float]
    klass: Optional[MetricClass]
    verdict: Optional[Literal["Pass", "Fail", "Restored", "Inconclusive"]]

class EvaluateResponse(BaseModel):
    site_att: str
    input_date: date
    options: dict
    overall: Optional[Literal["Pass", "Fail", "Restored", "Inconclusive"]]
    metrics: List[MetricEvaluation]
    # Consolidated datasets so the UI can render everything from a single call
    data: Optional[dict] = None


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
            # Ignore zeros by treating them as missing
            s = s.replace(0, np.nan)
            val = float(s.mean()) if s.notna().any() else None
            if val is not None and not np.isnan(val):
                return val
    # Fallback: sum across available numeric columns per row, then average that series
    num_df = df.select_dtypes(include=[np.number])
    if num_df.empty:
        # coerce object-like numerics
        coerced = df.apply(lambda s: pd.to_numeric(s, errors="coerce") if s.dtype == object else s)
        num_df = coerced.select_dtypes(include=[np.number])
    if num_df.empty:
        return None
    # drop columns that are entirely NaN after coercion
    # also ignore zeros by converting them to NaN before aggregation/averaging
    num_df = num_df.replace(0, np.nan).dropna(axis=1, how='all')
    if num_df.empty:
        return None
    # Compute row-wise sums over available values. If a row has no valid values, keep it as NaN
    has_any = num_df.notna().any(axis=1)
    row_sums = num_df.fillna(0).sum(axis=1)
    row_sums = row_sums.where(has_any, np.nan)
    m = float(row_sums.mean())
    return None if np.isnan(m) else m


def _sum_mean(df: Optional[pd.DataFrame], include_cols: List[str]) -> Optional[float]:
    """Sum selected columns row-wise and return the mean, ignoring zeros/NaNs.
    If none of the include_cols exist, fall back to _range_mean over numerics.
    """
    if df is None or df.empty:
        return None
    df = df.replace([np.inf, -np.inf], np.nan)
    try:
        df = df.infer_objects(copy=False)
    except Exception:
        pass
    cols = [c for c in include_cols if c in df.columns]
    if not cols:
        return _range_mean(df, preferred_cols=[])
    num_df = df[cols].apply(lambda s: pd.to_numeric(s, errors="coerce"))
    num_df = num_df.replace(0, np.nan)
    if num_df.empty:
        return None
    has_any = num_df.notna().any(axis=1)
    row_sums = num_df.fillna(0).sum(axis=1)
    row_sums = row_sums.where(has_any, np.nan)
    m = float(row_sums.mean())
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
    if a == "Down" and b in ("Flat", "Up"):
        verdict = "Restored"
    elif a == "Down" and b == "Down":
        verdict = "Fail"
    elif a == "Flat" and b == "Down":
        verdict = "Fail"
    elif a == "Flat" and b in ("Flat", "Up"):
        verdict = "Pass"
    elif a in ("Up") and b == "Down":
        verdict = "Fail"
    elif a in ("Up") and b in ("Flat", "Up"):
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
        print(f"[evaluate] Computing {metric} for {site_att} ({tech}) from {frm} to {to}")
        df = _call_with_timeout(get_cqi_daily_calculated, 10.0, att_name=site_att, min_date=frm, max_date=to, technology=tech)
        # select_db_cqi_daily outputs: umts_cqi, lte_cqi, nr_cqi
        val = _range_mean(df, preferred_cols=['umts_cqi', 'lte_cqi', 'nr_cqi'])
        # Scale CQI to 0-100 for API output consistency
        if val is not None and not np.isnan(val):
            val = float(val) * 100.0
        if timings is not None:
            timings[f"{metric}:{tech}:{frm}:{to}"] = time.perf_counter() - t0
        return val
    if metric == 'site_data':
        # Aggregate total data traffic across 3G + 4G + 5G
        df = _call_with_timeout(get_traffic_data_daily, 10.5, att_name=site_att, min_date=frm, max_date=to, technology=None, vendor=None)
        DATA_COLS = [
            # 3G packet data
            "h3g_traffic_d_user_ps_gb", "e3g_traffic_d_user_ps_gb", "n3g_traffic_d_user_ps_gb",
            # 4G packet data (multiple regions/prefixes)
            "h4g_traffic_d_user_ps_gb", "s4g_traffic_d_user_ps_gb", "e4g_traffic_d_user_ps_gb", "n4g_traffic_d_user_ps_gb",
            # 5G NSA PDCP data per legs
            "e5g_nsa_traffic_pdcp_gb_5gendc_4glegn", "n5g_nsa_traffic_pdcp_gb_5gendc_4glegn",
            "e5g_nsa_traffic_pdcp_gb_5gendc_5gleg", "n5g_nsa_traffic_pdcp_gb_5gendc_5gleg",
        ]
        val = _sum_mean(df, DATA_COLS)
        if timings is not None:
            timings[f"{metric}:{tech}:{frm}:{to}"] = time.perf_counter() - t0
        return val
    if metric == 'site_voice':
        # Aggregate total voice traffic across 3G CS + VoLTE
        df = _call_with_timeout(get_traffic_voice_daily, 10.5, att_name=site_att, min_date=frm, max_date=to, technology=None, vendor=None)
        VOICE_COLS = [
            # 3G CS voice
            "h3g_traffic_v_user_cs", "e3g_traffic_v_user_cs", "n3g_traffic_v_user_cs",
            # VoLTE components
            "user_traffic_volte_e", "user_traffic_volte_h", "user_traffic_volte_n", "user_traffic_volte_s",
        ]
        val = _sum_mean(df, VOICE_COLS)
        if timings is not None:
            timings[f"{metric}:{tech}:{frm}:{to}"] = time.perf_counter() - t0
        return val
    if metric == 'nb_cqi':
        df = _call_with_timeout(get_neighbor_cqi_daily_calculated, 10.0, site=site_att, min_date=frm, max_date=to, technology=tech, radius_km=radius_km)
        val = _range_mean(df, preferred_cols=['umts_cqi', 'lte_cqi', 'nr_cqi'])
        # Scale CQI to 0-100 for API output consistency
        if val is not None and not np.isnan(val):
            val = float(val) * 100.0
        if timings is not None:
            timings[f"{metric}:{tech}:{frm}:{to}"] = time.perf_counter() - t0
        return val
    if metric == 'nb_data':
        df = _call_with_timeout(get_neighbor_traffic_data, 10.0, site=site_att, min_date=frm, max_date=to, technology=None, radius_km=radius_km, vendor=None)
        DATA_COLS = [
            "h3g_traffic_d_user_ps_gb", "e3g_traffic_d_user_ps_gb", "n3g_traffic_d_user_ps_gb",
            "h4g_traffic_d_user_ps_gb", "s4g_traffic_d_user_ps_gb", "e4g_traffic_d_user_ps_gb", "n4g_traffic_d_user_ps_gb",
            "e5g_nsa_traffic_pdcp_gb_5gendc_4glegn", "n5g_nsa_traffic_pdcp_gb_5gendc_4glegn",
            "e5g_nsa_traffic_pdcp_gb_5gendc_5gleg", "n5g_nsa_traffic_pdcp_gb_5gendc_5gleg",
        ]
        val = _sum_mean(df, DATA_COLS)
        if timings is not None:
            timings[f"{metric}:{tech}:{frm}:{to}"] = time.perf_counter() - t0
        return val
    if metric == 'nb_voice':
        df = _call_with_timeout(get_neighbor_traffic_voice, 10.0, site=site_att, min_date=frm, max_date=to, technology=None, radius_km=radius_km, vendor=None)
        VOICE_COLS = [
            "h3g_traffic_v_user_cs", "e3g_traffic_v_user_cs", "n3g_traffic_v_user_cs",
            "user_traffic_volte_e", "user_traffic_volte_h", "user_traffic_volte_n", "user_traffic_volte_s",
        ]
        val = _sum_mean(df, VOICE_COLS)
        if timings is not None:
            timings[f"{metric}:{tech}:{frm}:{to}"] = time.perf_counter() - t0
        return val
    return None


def _fetch_timeseries(site_att: str, tech: Optional[str], start: Optional[date], end: Optional[date], metric: str, radius_km: float) -> list:
    """Fetch raw timeseries rows (JSON records) for a given metric/window.
    metric in { 'site_cqi','site_data','site_voice','nb_cqi','nb_data','nb_voice' }.
    """
    frm = _date_str(start)
    to = _date_str(end)
    if metric == 'site_cqi':
        df = _call_with_timeout(get_cqi_daily_calculated, 10.0, att_name=site_att, min_date=frm, max_date=to, technology=tech)
        # Scale CQI columns to 0-100 for API output
        try:
            if isinstance(df, pd.DataFrame) and not df.empty:
                for col in ('umts_cqi', 'lte_cqi', 'nr_cqi'):
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce') * 100.0
        except Exception:
            pass
        return df_json_records(df)
    if metric == 'site_data':
        df = _call_with_timeout(get_traffic_data_daily, 10.5, att_name=site_att, min_date=frm, max_date=to, technology=None, vendor=None)
        return df_json_records(df)
    if metric == 'site_voice':
        df = _call_with_timeout(get_traffic_voice_daily, 10.5, att_name=site_att, min_date=frm, max_date=to, technology=None, vendor=None)
        return df_json_records(df)
    if metric == 'nb_cqi':
        df = _call_with_timeout(get_neighbor_cqi_daily_calculated, 10.0, site=site_att, min_date=frm, max_date=to, technology=tech, radius_km=radius_km)
        # Scale CQI columns to 0-100 for API output
        try:
            if isinstance(df, pd.DataFrame) and not df.empty:
                for col in ('umts_cqi', 'lte_cqi', 'nr_cqi'):
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce') * 100.0
        except Exception:
            pass
        return df_json_records(df)
    if metric == 'nb_data':
        df = _call_with_timeout(get_neighbor_traffic_data, 10.0, site=site_att, min_date=frm, max_date=to, technology=None, radius_km=radius_km, vendor=None)
        return df_json_records(df)
    if metric == 'nb_voice':
        df = _call_with_timeout(get_neighbor_traffic_voice, 10.0, site=site_att, min_date=frm, max_date=to, technology=None, radius_km=radius_km, vendor=None)
        return df_json_records(df)
    return []


@router.post("")
def evaluate(req: EvaluateRequest) -> EvaluateResponse:
    # Define windows per §6
    max_d = _call_with_timeout(get_max_date, 8.0)
    max_date_source = "db"


    # Fallback: if DB global max is unavailable, try deriving from this site's available data within a bounded window
    if not max_d:
        span = 120  # days around input date to probe
        probe_min = req.input_date - timedelta(days=span)
        probe_max = req.input_date + timedelta(days=span)
        candidates: list = []
        # We purposefully keep tight timeouts to avoid stalls
        try:
            df1 = _call_with_timeout(get_cqi_daily_calculated, 10, att_name=req.site_att, min_date=str(probe_min), max_date=str(probe_max), technology=None)
            if isinstance(df1, pd.DataFrame) and not df1.empty and 'time' in df1.columns:
                tmax = pd.to_datetime(df1['time'], errors='coerce').max()
                if pd.notna(tmax):
                    candidates.append(tmax.date())
        except Exception:
            pass
        try:
            df2 = _call_with_timeout(get_traffic_data_daily, 10, att_name=req.site_att, min_date=str(probe_min), max_date=str(probe_max), technology=None, vendor=None)
            if isinstance(df2, pd.DataFrame) and not df2.empty and 'time' in df2.columns:
                tmax = pd.to_datetime(df2['time'], errors='coerce').max()
                if pd.notna(tmax):
                    candidates.append(tmax.date())
        except Exception:
            pass
        try:
            df3 = _call_with_timeout(get_traffic_voice_daily, 10, att_name=req.site_att, min_date=str(probe_min), max_date=str(probe_max), technology=None, vendor=None)
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
    # Define inclusive windows with exactly `period` days each
    # before: ends just before the guard; after: starts just after the guard
    before_end = req.input_date - timedelta(days=req.guard + 1)
    before_start = before_end - timedelta(days=req.period - 1)
    after_start = req.input_date + timedelta(days=req.guard + 1)
    after_end = after_start + timedelta(days=req.period - 1)
    last_end = max_d
    last_start = (last_end - timedelta(days=req.period - 1)) if last_end else None

    # Metric plan: keep CQI per-tech, but aggregate Data and Voice totals
    plan: List[Tuple[str, str, Optional[str]]] = []
    for tech in ("3G", "4G", "5G"):
        plan.append((f"Site CQI {tech}", 'site_cqi', tech))
    plan.append(("Site Data (3G+4G+5G)", 'site_data', None))
    plan.append(("Site Voice (3G+VoLTE)", 'site_voice', None))
    for tech in ("3G", "4G", "5G"):
        plan.append((f"Neighbors CQI {tech}", 'nb_cqi', tech))
    plan.append(("Neighbors Data (3G+4G+5G)", 'nb_data', None))
    plan.append(("Neighbors Voice (3G+VoLTE)", 'nb_voice', None))

    metrics: List[MetricEvaluation] = []
    debug_timings: Dict[str, float] = {}

    # Build tasks: first site_*, then neighbor_* to run in two phases
    Task = Tuple[str, str, Optional[str], str]  # (name, mkey, tech, window)
    site_tasks: List[Task] = []
    nb_tasks: List[Task] = []
    for name, mkey, tech in plan:
        bucket = site_tasks if mkey.startswith('site_') else nb_tasks
        bucket.append((name, mkey, tech, 'before'))
        bucket.append((name, mkey, tech, 'after'))
        if last_start and last_end:
            bucket.append((name, mkey, tech, 'last'))

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

    def run_phase(phase_tasks: List[Task]) -> None:
        nonlocal results
        if not phase_tasks:
            return
        remaining = GLOBAL_BUDGET_S - (time.perf_counter() - start_time)
        if remaining <= 0:
            return
        ex = concurrent.futures.ThreadPoolExecutor(max_workers=6)
        try:
            future_map = {ex.submit(run_task, t): t for t in phase_tasks}
            for fut in concurrent.futures.as_completed(future_map, timeout=remaining if remaining > 0 else None):
                task, val, elapsed = fut.result()
                results[task] = val
                if req.debug:
                    name, mkey, tech, window = task
                    debug_timings[f"{mkey}:{tech}:{window}"] = elapsed
                if time.perf_counter() - start_time > GLOBAL_BUDGET_S:
                    print("[evaluate] Global time budget exceeded; returning partial results")
                    for f in future_map.keys():
                        if not f.done():
                            f.cancel()
                    ex.shutdown(wait=False, cancel_futures=True)
                    return
        except concurrent.futures.TimeoutError:
            print("[evaluate] Phase timed out; continuing with partial results")
        finally:
            try:
                ex.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass

    # Phase 1: site metrics
    run_phase(site_tasks)
    # Phase 2: neighbors if time remains
    run_phase(nb_tasks)

    # Assemble metric entries
    for name, mkey, tech in plan:
        b = results.get((name, mkey, tech, 'before'))
        a = results.get((name, mkey, tech, 'after'))
        l = results.get((name, mkey, tech, 'last')) if (last_start and last_end) else None
        d_ab = _delta(a, b)
        d_lb = _delta(l, b)
        klass, verdict = _classify(d_ab, d_lb, req.threshold)
        metrics.append(MetricEvaluation(
            name=name,
            before_mean=b,
            after_mean=a,
            last_mean=l,
            delta_after_before=d_ab,
            delta_last_before=d_lb,
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

    # Build consolidated datasets under remaining global budget
    data_payload: Dict[str, Any] = {
        "site": {"cqi": {}, "traffic": {}, "voice": {}},
        "neighbors": {"cqi": {}, "traffic": {}, "voice": {}, "geo": []},
    }

    # Prepare dataset tasks (windowed)
    DTask = Tuple[str, str, Optional[str], str]  # (scope:site|neighbors, mkey, tech, window)
    d_tasks: List[DTask] = []
    for tech in ("3G", "4G", "5G"):
        d_tasks.append(("site", "site_cqi", tech, "before"))
        d_tasks.append(("site", "site_cqi", tech, "after"))
        # Always include the guard gap between 'before' and 'after'
        d_tasks.append(("site", "site_cqi", tech, "between"))
        if last_start and last_end:
            # Include the span between 'after' and 'last' as 'mid'
            d_tasks.append(("site", "site_cqi", tech, "mid"))
            d_tasks.append(("site", "site_cqi", tech, "last"))
        d_tasks.append(("neighbors", "nb_cqi", tech, "before"))
        d_tasks.append(("neighbors", "nb_cqi", tech, "after"))
        d_tasks.append(("neighbors", "nb_cqi", tech, "between"))
        if last_start and last_end:
            d_tasks.append(("neighbors", "nb_cqi", tech, "mid"))
            d_tasks.append(("neighbors", "nb_cqi", tech, "last"))
    # totals
    for mkey_site, mkey_nb in (("site_data", "nb_data"), ("site_voice", "nb_voice")):
        for w in ("before", "after"):
            d_tasks.append(("site", mkey_site, None, w))
            d_tasks.append(("neighbors", mkey_nb, None, w))
        # Always include the guard gap between 'before' and 'after'
        d_tasks.append(("site", mkey_site, None, "between"))
        d_tasks.append(("neighbors", mkey_nb, None, "between"))
        if last_start and last_end:
            d_tasks.append(("site", mkey_site, None, "mid"))
            d_tasks.append(("site", mkey_site, None, "last"))
            d_tasks.append(("neighbors", mkey_nb, None, "mid"))
            d_tasks.append(("neighbors", mkey_nb, None, "last"))

    def dtask_window_bounds(window: str) -> Tuple[Optional[date], Optional[date]]:
        if window == "before":
            return before_start, before_end
        if window == "after":
            return after_start, after_end
        if window == "between":
            # define the guard gap as the days strictly between before_end and after_start
            s = (before_end + timedelta(days=1)) if before_end else None
            e = (after_start - timedelta(days=1)) if after_start else None
            if s and e and s > e:
                return None, None
            return s, e
        if window == "mid":
            # define the span between end of after and start of last, if last exists
            if not (last_start and last_end):
                return None, None
            s = (after_end + timedelta(days=1)) if after_end else None
            e = (last_start - timedelta(days=1)) if last_start else None
            if s and e and s > e:
                return None, None
            return s, e
        return last_start, last_end

    def run_dtask(task: DTask) -> Tuple[DTask, list]:
        scope, mkey, tech, window = task
        s, e = dtask_window_bounds(window)
        recs = _fetch_timeseries(req.site_att, tech, s, e, mkey, req.radius_km)
        return task, recs

    # Execute dataset tasks with remaining budget
    remaining = 25.0 - (time.perf_counter() - start_time)
    d_results: Dict[DTask, list] = {}
    if remaining > 0:
        ex = concurrent.futures.ThreadPoolExecutor(max_workers=6)
        try:
            future_map = {ex.submit(run_dtask, t): t for t in d_tasks}
            for fut in concurrent.futures.as_completed(future_map, timeout=remaining):
                task, recs = fut.result()
                d_results[task] = recs
        except concurrent.futures.TimeoutError:
            print("[evaluate] dataset collection timed out; returning partial datasets")
        finally:
            try:
                ex.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass

    # Collate into payload
    def put(scope: str, kind: str, tech: Optional[str], window: str, recs: list):
        target = data_payload[scope]
        bucket = None
        if kind in ("site_cqi", "nb_cqi"):
            bucket = target["cqi"]
        elif kind in ("site_data", "nb_data"):
            bucket = target["traffic"]
        elif kind in ("site_voice", "nb_voice"):
            bucket = target["voice"]
        key = tech or "total"
        if key not in bucket:
            bucket[key] = {"before": [], "after": [], "between": [], "mid": [], "last": []}
        if window not in bucket[key]:
            bucket[key][window] = []
        bucket[key][window] = recs

    for task, recs in d_results.items():
        scope, mkey, tech, window = task
        put(scope, mkey, tech, window, recs)

    # Neighbors geo (one-shot, outside windows)
    try:
        geo = get_neighbors_geo(req.site_att, radius_km=req.radius_km) or []
        data_payload["neighbors"]["geo"] = geo
    except Exception as e:
        print(f"[evaluate] neighbors geo error: {e}")

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
            "partial": len({k: v for k, v in results.items() if v is not None}) < (len(site_tasks) + len(nb_tasks)),
            **({"debug_timings": debug_timings, "max_date_source": max_date_source} if req.debug else {}),
        },
        overall=overall,  # type: ignore[arg-type]
        metrics=metrics,
        data=data_payload,
    )
