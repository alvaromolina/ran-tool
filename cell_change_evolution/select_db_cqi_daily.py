import os
import dotenv
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# Load environment variables
dotenv.load_dotenv()
POSTGRES_USERNAME = os.getenv('POSTGRES_USERNAME')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_DB = os.getenv('POSTGRES_DB')

def create_connection():
    """Create database connection using SQLAlchemy"""
    try:
        connection_string = f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        print(f"Error creating database connection: {e}")
        return None

def get_cqi_daily(att_name, min_date=None, max_date=None, technology=None):
    """Get CQI daily data for a single site with optional filters"""
    engine = create_connection()
    if engine is None:
        return None
    
    try:
        # Build technology-specific conditions
        tech_conditions = []
        if technology == '3G':
            tech_conditions = ["u.umts_composite_quality IS NOT NULL"]
        elif technology == '4G':
            tech_conditions = ["l.f4g_composite_quality IS NOT NULL"]
        elif technology == '5G':
            tech_conditions = ["n.nr_composite_quality IS NOT NULL"]
        
        # Build date conditions
        date_conditions = []
        if min_date:
            date_conditions.append(f"COALESCE(l.date, n.date, u.date) >= '{min_date}'")
        if max_date:
            date_conditions.append(f"COALESCE(l.date, n.date, u.date) <= '{max_date}'")
        
        where_conditions = []
        if tech_conditions:
            where_conditions.extend(tech_conditions)
        if date_conditions:
            where_conditions.extend(date_conditions)
        
        where_clause = ""
        if where_conditions:
            where_clause = f"WHERE {' AND '.join(where_conditions)}"
        
        cqi_query = text(f"""
        SELECT 
          COALESCE(l.date, n.date, u.date) AS time,
          COALESCE(l.site_att, n.site_att, u.site_att) AS site_att,
          l.f4g_composite_quality AS lte_cqi,
          n.nr_composite_quality AS nr_cqi,
          u.umts_composite_quality AS umts_cqi
        FROM 
          (SELECT date, site_att, f4g_composite_quality 
           FROM lte_cqi_daily 
           WHERE site_att = :att_name) l
        FULL OUTER JOIN 
          (SELECT date, site_att, nr_composite_quality 
           FROM nr_cqi_daily 
           WHERE site_att = :att_name) n
          ON l.date = n.date AND l.site_att = n.site_att
        FULL OUTER JOIN 
          (SELECT date, site_att, umts_composite_quality 
           FROM umts_cqi_daily 
           WHERE site_att = :att_name) u
          ON COALESCE(l.date, n.date) = u.date AND COALESCE(l.site_att, n.site_att) = u.site_att
        {where_clause}
        ORDER BY 
          site_att ASC, time ASC
        """)
        
        result_df = pd.read_sql(cqi_query, engine, params={'att_name': att_name})
        result_df = sanitize_df(result_df)
        print(f"Retrieved CQI data for site {att_name}: {result_df.shape[0]} records")
        return result_df
        
    except Exception as e:
        print(f"Error executing CQI query: {e}")
        return None
    finally:
        engine.dispose()

def get_cqi_daily_calculated(att_name, min_date=None, max_date=None, technology=None):
    """Return calculated CQI daily.

    - If technology is '3G'|'4G'|'5G', delegate to the specific calculated function and return
      columns [time, site_att, <tech>_cqi].
    - If technology is None, outer-merge UMTS/LTE/NR calculated DataFrames on [time, site_att]
      to match the shape of get_cqi_daily(): [time, site_att, lte_cqi, nr_cqi, umts_cqi].
    """
    # Delegate path
    if technology == '3G':
        return get_umts_cqi_daily_calculated(att_name, min_date=min_date, max_date=max_date)
    if technology == '4G':
        return get_lte_cqi_daily_calculated(att_name, min_date=min_date, max_date=max_date)
    if technology == '5G':
        return get_nr_cqi_daily_calculated(att_name, min_date=min_date, max_date=max_date)

    # technology is None: merge three techs
    df3 = get_umts_cqi_daily_calculated(att_name, min_date=min_date, max_date=max_date)
    df4 = get_lte_cqi_daily_calculated(att_name, min_date=min_date, max_date=max_date)
    df5 = get_nr_cqi_daily_calculated(att_name, min_date=min_date, max_date=max_date)

    # Ensure DataFrames exist
    if df3 is None and df4 is None and df5 is None:
        return None
    if df3 is None:
        df3 = pd.DataFrame(columns=['time', 'site_att', 'umts_cqi'])
    if df4 is None:
        df4 = pd.DataFrame(columns=['time', 'site_att', 'lte_cqi'])
    if df5 is None:
        df5 = pd.DataFrame(columns=['time', 'site_att', 'nr_cqi'])

    # Merge stepwise to preserve columns
    out = pd.merge(df4, df5, on=['time', 'site_att'], how='outer')
    out = pd.merge(out, df3, on=['time', 'site_att'], how='outer')

    # Order and sanitize
    out = out.sort_values(by=['site_att', 'time'])
    out = out[['time', 'site_att', 'lte_cqi', 'nr_cqi', 'umts_cqi']]
    return sanitize_df(out)

def calculate_unified_cqi_nr_row(row):
    """Calculate unified NR (5G) CQI using the specified formula:
    0.17*EXP((1-Acc_MN)*C1) + 0.13*EXP((1-Acc_SN)*C2) + 0.17*EXP((1-Ret_MN)*C3)
    + 0.13*EXP((1-Endc_Ret_Tot)*C4) + 0.20*(1-EXP(Thp_MN*1000*C5)) + 0.20*(1-EXP(Thp_SN*1000*C6))

    Where Acc/Ret are percentages (0..100). Throughputs are in Mbps.
    Falls back to vendor counters if combined fields are missing.
    """
    import math

    # Weights (from attached image)
    W1, W2, W3, W4, W5, W6 = 0.17, 0.13, 0.17, 0.13, 0.20, 0.20
    # Constants (provided)
    C1 = -14.9264816
    C2 = -26.6809026
    C3 = -14.9264816
    C4 = -26.6809026
    C5 = -0.0002006621  # per kbps; formula multiplies Mbps*1000
    C6 = -0.0002006621

    # Acc MN (%), prefer combined
    acc_mn = row.get('acc_mn')
    if acc_mn is None:
        # Build from vendor counters: RRC * S1 * ERAB_SR_4GENDC
        acc_rrc_num = _zn(row.get('e5g_acc_rrc_num_n')) + _zn(row.get('n5g_acc_rrc_num_n'))
        acc_rrc_den = _zn(row.get('e5g_acc_rrc_den_n')) + _zn(row.get('n5g_acc_rrc_den_n'))
        s1_num = _zn(row.get('e5g_s1_sr_num_n')) + _zn(row.get('n5g_s1_sr_num_n'))
        s1_den = _zn(row.get('e5g_s1_sr_den_n')) + _zn(row.get('n5g_s1_sr_den_n'))
        erab4g_num = _zn(row.get('e5g_nsa_acc_erab_sr_4gendc_num_n')) + _zn(row.get('n5g_nsa_acc_erab_sr_4gendc_num_n'))
        erab4g_den = _zn(row.get('e5g_nsa_acc_erab_sr_4gendc_den_n')) + _zn(row.get('n5g_nsa_acc_erab_sr_4gendc_den_n'))
        acc_mn = ((acc_rrc_num/acc_rrc_den if acc_rrc_den else 0)
                  * (s1_num/s1_den if s1_den else 0)
                  * (erab4g_num/erab4g_den if erab4g_den else 0)) * 100

    # Acc SN (%), prefer combined
    acc_sn = row.get('acc_sn')
    if acc_sn is None:
        # Use 5G leg ERAB success over attempts if available
        succ_5g = _zn(row.get('e5g_nsa_acc_erab_succ_5gendc_5gleg_n')) + _zn(row.get('n5g_nsa_acc_erab_succ_5gendc_5gleg_n'))
        att_5g  = _zn(row.get('e5g_nsa_acc_erab_att_5gendc_5gleg_n')) + _zn(row.get('n5g_nsa_acc_erab_att_5gendc_5gleg_n'))
        acc_sn = (succ_5g/att_5g)*100 if att_5g else None

    # Ret MN (%), prefer combined
    ret_mn = row.get('ret_mn')
    if ret_mn is None:
        drop_4g = _zn(row.get('e5g_nsa_ret_erab_drop_4gendc_n')) + _zn(row.get('n5g_nsa_ret_erab_drop_4gendc_n'))
        att_4g  = _zn(row.get('e5g_nsa_ret_erab_att_4gendc_n')) + _zn(row.get('n5g_nsa_ret_erab_att_4gendc_n'))
        ret_mn = (1 - (drop_4g/att_4g)) * 100 if att_4g else None

    # Endc Ret Tot (%), prefer combined
    endc_ret_tot = row.get('endc_ret_tot')
    if endc_ret_tot is None:
        drop_54 = _zn(row.get('e5g_nsa_ret_erab_drop_5gendc_4g5gleg_num_n')) + _zn(row.get('n5g_nsa_ret_erab_drop_5gendc_4g5gleg_num_n'))
        den_54  = _zn(row.get('e5g_nsa_ret_erab_drop_5gendc_4g5gleg_den_n')) + _zn(row.get('n5g_nsa_ret_erab_drop_5gendc_4g5gleg_den_n'))
        endc_ret_tot = (1 - (drop_54/den_54)) * 100 if den_54 else None

    # Throughputs (Mbps), prefer combined
    thp_mn = row.get('thp_mn')
    if thp_mn is None:
        # Prefer MAC DL avg Mbps numer/denom if provided, else PDCP avg
        mac_num = _zn(row.get('e5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_num_n')) + _zn(row.get('n5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_num_n'))
        mac_den = _zn(row.get('e5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_denom_n')) + _zn(row.get('n5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_denom_n'))
        mac_avg = (mac_num/mac_den) if mac_den else None
        pdcp_num = _zn(row.get('e5g_nsa_thp_mn_num')) + _zn(row.get('n5g_nsa_thp_mn_num'))
        pdcp_den = _zn(row.get('e5g_nsa_thp_mn_den')) + _zn(row.get('n5g_nsa_thp_mn_den'))
        pdcp_avg = (pdcp_num/pdcp_den) if pdcp_den else None
        thp_mn = mac_avg if mac_avg is not None else (pdcp_avg if pdcp_avg is not None else 0)

    thp_sn = row.get('thp_sn')
    if thp_sn is None:
        # Use MAC DL avg Mbps from 5G leg if available
        mac_num = _zn(row.get('e5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_num_n')) + _zn(row.get('n5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_num_n'))
        mac_den = _zn(row.get('e5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_denom_n')) + _zn(row.get('n5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_denom_n'))
        thp_sn = (mac_num/mac_den) if mac_den else 0

    # Compose final CQI using provided formula
    def _p(v):
        return (v or 0) / 100.0

    term1 = W1 * math.exp((1 - _p(acc_mn)) * C1)
    term2 = W2 * math.exp((1 - _p(acc_sn)) * C2)
    term3 = W3 * math.exp((1 - _p(ret_mn)) * C3)
    term4 = W4 * math.exp((1 - _p(endc_ret_tot)) * C4)
    term5 = W5 * (1 - math.exp((thp_mn or 0) * 1000.0 * C5))
    term6 = W6 * (1 - math.exp((thp_sn or 0) * 1000.0 * C6))
    nr_cqi = term1 + term2 + term3 + term4 + term5 + term6
    try:
        return round(float(nr_cqi), 8)
    except Exception:
        return float(nr_cqi)

def get_nr_cqi_daily_calculated(att_name, min_date=None, max_date=None):
    """Compute NR (5G) unified CQI per day/site from counters in nr_cqi_daily.

    Returns: time, site_att, nr_cqi
    """
    engine = create_connection()
    if engine is None:
        return None
    try:
        where = ["n.site_att = :att_name"]
        params = {"att_name": att_name}
        if min_date:
            where.append("n.date >= :min_date")
            params["min_date"] = min_date
        if max_date:
            where.append("n.date <= :max_date")
            params["max_date"] = max_date
        where_clause = f"WHERE {' AND '.join(where)}" if where else ""

        sql = text(f"""
            SELECT
              n.date AS time,
              n.site_att,
              n.acc_mn, n.acc_sn, n.endc_ret_tot, n.ret_mn, n.thp_mn, n.thp_sn,
              n.e5g_acc_rrc_num_n, n.e5g_s1_sr_num_n, n.e5g_nsa_acc_erab_sr_4gendc_num_n,
              n.e5g_acc_rrc_den_n, n.e5g_s1_sr_den_n, n.e5g_nsa_acc_erab_sr_4gendc_den_n,
              n.n5g_acc_rrc_num_n, n.n5g_s1_sr_num_n, n.n5g_nsa_acc_erab_sr_4gendc_num_n,
              n.n5g_acc_rrc_den_n, n.n5g_s1_sr_den_n, n.n5g_nsa_acc_erab_sr_4gendc_den_n,
              n.e5g_nsa_ret_erab_drop_4gendc_n, n.e5g_nsa_ret_erab_att_4gendc_n,
              n.e5g_nsa_ret_erab_drop_5gendc_4g5gleg_num_n, n.e5g_nsa_ret_erab_drop_5gendc_4g5gleg_den_n,
              n.n5g_nsa_ret_erab_drop_4gendc_n, n.n5g_nsa_ret_erab_att_4gendc_n,
              n.n5g_nsa_ret_erab_drop_5gendc_4g5gleg_num_n, n.n5g_nsa_ret_erab_drop_5gendc_4g5gleg_den_n,
              n.e5g_nsa_thp_mn_num, n.e5g_nsa_thp_mn_den,
              n.n5g_nsa_thp_mn_num, n.n5g_nsa_thp_mn_den,
              n.e5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_num_n, n.e5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_denom_n,
              n.n5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_num_n, n.n5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_denom_n,
              n.traffic_4gleg_gb, n.e5g_nsa_traffic_pdcp_gb_5gendc_4glegn, n.n5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
              n.traffic_5gleg_gb, n.e5g_nsa_traffic_pdcp_gb_5gendc_5gleg, n.n5g_nsa_traffic_pdcp_gb_5gendc_5gleg
            FROM nr_cqi_daily n
            {where_clause}
            ORDER BY n.site_att ASC, n.date ASC
        """)

        df = pd.read_sql(sql, engine, params=params)
        if df is None or df.empty:
            return df

        df['nr_cqi'] = df.apply(calculate_unified_cqi_nr_row, axis=1)
        out = df[['time', 'site_att', 'nr_cqi']].copy()
        return sanitize_df(out)
    except Exception as e:
        print(f"Error computing NR unified CQI: {e}")
        return None
    finally:
        engine.dispose()

def _sum_fields(row, fields):
    return sum(_zn(row.get(f)) for f in fields)

def calculate_unified_cqi_lte_row(row):
    """Calculate unified LTE (4G) CQI using combined fields when present; fallback to vendor aggregation.

    Uses columns from lte_cqi_daily:
      - Combined (preferred if present): accessibility_ps, retainability_ps, irat_ps,
        thpt_dl_kbps_ran_drb, f4gon3g, ookla_latency, ookla_thp
      - Vendor-prefixed fallbacks: <v>4g_rrc_success_all/attemps_all, <v>4g_s1_success/attemps,
        <v>4g_erab_success/erabs_attemps, <v>4g_retainability_num/denom,
        <v>4g_irat_4g_to_3g_events, <v>4g_erab_succ_established,
        <v>4g_thpt_user_dl_kbps_num/denom, <v>4g_time3g/time4g,
        <v>4g_sumavg_latency (ms), <v>4g_sumavg_dl_kbps, <v>4g_summuestras
    """
    import math

    # Helper for safe division
    def _sdiv(n, d):
        try:
            n = 0 if n is None else float(n)
            d = float(d)
            return (n / d) if d else 0.0
        except Exception:
            return 0.0

    vendors = ['h4g', 's4g', 'e4g', 'n4g']

    # Accessibility (%) — always computed from vendor totals
    erab_success = _sum_fields(row, [f"{v}_erab_success" for v in vendors])
    erabs_attemps = _sum_fields(row, [f"{v}_erabs_attemps" for v in vendors])
    rrc_success = _sum_fields(row, [f"{v}_rrc_success_all" for v in vendors])
    rrc_attemps = _sum_fields(row, [f"{v}_rrc_attemps_all" for v in vendors])
    s1_success  = _sum_fields(row, [f"{v}_s1_success" for v in vendors])
    s1_attemps  = _sum_fields(row, [f"{v}_s1_attemps" for v in vendors])
    acc = (_sdiv(erab_success, erabs_attemps) *
           _sdiv(rrc_success, rrc_attemps) *
           _sdiv(s1_success, s1_attemps) * 100.0)

    # Retainability (%) — from vendor totals
    ret_num   = _sum_fields(row, [f"{v}_retainability_num" for v in vendors])
    ret_denom = _sum_fields(row, [f"{v}_retainability_denom" for v in vendors])
    ret = (1 - _sdiv(ret_num, ret_denom)) * 100.0

    # IRAT to 3G rate (fraction 0..1)
    # IRAT to 3G rate (fraction 0..1) — from vendor totals
    irat_events = _sum_fields(row, [f"{v}_irat_4g_to_3g_events" for v in vendors])
    irat = _sdiv(irat_events, erab_success)  # fraction 0..1

    # Throughput DL (kbps)
    # Throughput DL (kbps) — from vendor totals
    thp_num = _sum_fields(row, [f"{v}_thpt_user_dl_kbps_num" for v in vendors])
    thp_den = _sum_fields(row, [f"{v}_thpt_user_dl_kbps_denom" for v in vendors])
    thp_dl = _sdiv(thp_num, thp_den)

    # 3G share p3g = time3g / (time3g + time4g) — from vendor totals
    t3g = _sum_fields(row, [f"{v}_time3g" for v in vendors])
    t4g = _sum_fields(row, [f"{v}_time4g" for v in vendors])
    p3g = _sdiv(t3g, (t3g + t4g))

    # Ookla latency (ms) and throughput (kbps equivalent)
    # Ookla latency (ms) and throughput (kbps equivalent) — from vendor totals
    lat_sum = _sum_fields(row, [f"{v}_sumavg_latency" for v in vendors])
    dl_sum  = _sum_fields(row, [f"{v}_sumavg_dl_kbps" for v in vendors])
    m_count = _sum_fields(row, [f"{v}_summuestras" for v in vendors])
    latency = _sdiv(lat_sum, m_count) if m_count else 0.0
    ookla_thp = _sdiv(dl_sum, m_count) if m_count else 0.0

    # Apply LTE CQI formula (aligned with vectorized implementation)
    def _as_float(x, default=0.0):
        try:
            return float(0 if x is None else x)
        except Exception:
            return default

    acc_p = _as_float(acc) / 100.0
    ret_p = _as_float(ret) / 100.0
    irat_p = _as_float(irat)  # already a fraction 0..1
    thp_dl_v = _as_float(thp_dl)
    p3g_v = _as_float(p3g)
    latency_v = _as_float(latency)
    ookla_thp_v = _as_float(ookla_thp)

    term_acc = 0.25 * math.exp((1 - acc_p) * -63.91668575)
    term_ret = 0.25 * math.exp((1 - ret_p) * -63.91668575)
    term_irat = 0.05 * math.exp((irat_p) * -22.31435513)
    term_thp_ran = 0.30 * (1 - math.exp(thp_dl_v * -0.000282742))
    term_4g3g = 0.05 * min(1.0, math.exp((p3g_v - 0.10) * -11.15717757))
    term_lat = 0.05 * math.exp((latency_v - 20.0) * -0.00526802578289131)
    term_ookla = 0.05 * (1 - math.exp(ookla_thp_v * -0.00005364793041447))

    lte_cqi = (term_acc + term_ret + term_irat + term_thp_ran + term_4g3g + term_lat + term_ookla)
    try:
        return round(float(lte_cqi), 8)
    except Exception:
        return float(lte_cqi)

def get_lte_cqi_daily_calculated(att_name, min_date=None, max_date=None):
    """Compute LTE (4G) unified CQI per day/site from counters in lte_cqi_daily.

    Returns columns: time (date), site_att, lte_cqi
    """
    engine = create_connection()
    if engine is None:
        return None
    try:
        where = ["l.site_att = :att_name"]
        params = {"att_name": att_name}
        if min_date:
            where.append("l.date >= :min_date")
            params["min_date"] = min_date
        if max_date:
            where.append("l.date <= :max_date")
            params["max_date"] = max_date
        where_clause = f"WHERE {' AND '.join(where)}" if where else ""

        sql = text(f"""
            SELECT
              l.date AS time,
              l.site_att,
              l.accessibility_ps, l.retainability_ps, l.irat_ps,
              l.thpt_dl_kbps_ran_drb,
              l.f4gon3g,
              l.ookla_latency,
              l.ookla_thp,
              -- fallbacks if combined not available
              l.h4g_rrc_success_all, l.h4g_rrc_attemps_all, l.h4g_s1_success, l.h4g_s1_attemps,
              l.h4g_erab_success, l.h4g_erabs_attemps, l.h4g_retainability_num, l.h4g_retainability_denom,
              l.h4g_irat_4g_to_3g_events, l.h4g_erab_succ_established, l.h4g_thpt_user_dl_kbps_num, l.h4g_thpt_user_dl_kbps_denom,
              l.h4g_time3g, l.h4g_time4g, l.h4g_sumavg_latency, l.h4g_sumavg_dl_kbps, l.h4g_summuestras,
              l.s4g_rrc_success_all, l.s4g_rrc_attemps_all, l.s4g_s1_success, l.s4g_s1_attemps,
              l.s4g_erab_success, l.s4g_erabs_attemps, l.s4g_retainability_num, l.s4g_retainability_denom,
              l.s4g_irat_4g_to_3g_events, l.s4g_erab_succ_established, l.s4g_thpt_user_dl_kbps_num, l.s4g_thpt_user_dl_kbps_denom,
              l.s4g_time3g, l.s4g_time4g, l.s4g_sumavg_latency, l.s4g_sumavg_dl_kbps, l.s4g_summuestras,
              l.e4g_rrc_success_all, l.e4g_rrc_attemps_all, l.e4g_s1_success, l.e4g_s1_attemps,
              l.e4g_erab_success, l.e4g_erabs_attemps, l.e4g_retainability_num, l.e4g_retainability_denom,
              l.e4g_irat_4g_to_3g_events, l.e4g_erab_succ_established, l.e4g_thpt_user_dl_kbps_num, l.e4g_thpt_user_dl_kbps_denom,
              l.e4g_time3g, l.e4g_time4g, l.e4g_sumavg_latency, l.e4g_sumavg_dl_kbps, l.e4g_summuestras,
              l.n4g_rrc_success_all, l.n4g_rrc_attemps_all, l.n4g_s1_success, l.n4g_s1_attemps,
              l.n4g_erab_success, l.n4g_erabs_attemps, l.n4g_retainability_num, l.n4g_retainability_denom,
              l.n4g_irat_4g_to_3g_events, l.n4g_erab_succ_established, l.n4g_thpt_user_dl_kbps_num, l.n4g_thpt_user_dl_kbps_denom,
              l.n4g_time3g, l.n4g_time4g, l.n4g_sumavg_latency, l.n4g_sumavg_dl_kbps, l.n4g_summuestras
            FROM lte_cqi_daily l
            {where_clause}
            ORDER BY l.site_att ASC, l.date ASC
        """)

        df = pd.read_sql(sql, engine, params=params)
        if df is None or df.empty:
            return df

        df['lte_cqi'] = df.apply(calculate_unified_cqi_lte_row, axis=1)
        out = df[['time', 'site_att', 'lte_cqi']].copy()
        return sanitize_df(out)
    except Exception as e:
        print(f"Error computing LTE unified CQI: {e}")
        return None
    finally:
        engine.dispose()

def _zn(v):
    """Zero-for-None helper (works with pandas row get)."""
    try:
        if v is None:
            return 0
        # if NaN
        if isinstance(v, float) and (v != v):
            return 0
        return v
    except Exception:
        return 0

def calculate_unified_cqi_umts_row(row):
    """Calculate unified UMTS CQI from a DataFrame row using provided mappings and weights.

    Expects the following fields on the row (aggregated by vendor prefix h/e/n):
      - RRC/NAS/RAB successes & attempts for CS and PS
      - CS drop num/denom
      - PS retain num/denom
      - Throughput DL numerator/denominator (kbps)
    """
    import math

    total_cs_acc_success = (_zn(row.get('h3g_rrc_success_cs')) + _zn(row.get('e3g_rrc_success_cs')) + _zn(row.get('n3g_rrc_success_cs')))
    total_cs_acc_attempts = (_zn(row.get('h3g_rrc_attempts_cs')) + _zn(row.get('e3g_rrc_attempts_cs')) + _zn(row.get('n3g_rrc_attempts_cs')))
    total_cs_nas_success = (_zn(row.get('h3g_nas_success_cs')) + _zn(row.get('e3g_nas_success_cs')) + _zn(row.get('n3g_nas_success_cs')))
    total_cs_nas_attempts = (_zn(row.get('h3g_nas_attempts_cs')) + _zn(row.get('e3g_nas_attempts_cs')) + _zn(row.get('n3g_nas_attempts_cs')))
    total_cs_rab_success = (_zn(row.get('h3g_rab_success_cs')) + _zn(row.get('e3g_rab_success_cs')) + _zn(row.get('n3g_rab_success_cs')))
    total_cs_rab_attempts = (_zn(row.get('h3g_rab_attempts_cs')) + _zn(row.get('e3g_rab_attempts_cs')) + _zn(row.get('n3g_rab_attempts_cs')))

    total_cs_drop_num = (_zn(row.get('h3g_drop_num_cs')) + _zn(row.get('e3g_drop_num_cs')) + _zn(row.get('n3g_drop_num_cs')))
    total_cs_drop_denom = (_zn(row.get('h3g_drop_denom_cs')) + _zn(row.get('e3g_drop_denom_cs')) + _zn(row.get('n3g_drop_denom_cs')))

    total_ps_rrc_success = (_zn(row.get('h3g_rrc_success_ps')) + _zn(row.get('e3g_rrc_success_ps')) + _zn(row.get('n3g_rrc_success_ps')))
    total_ps_rrc_attempts = (_zn(row.get('h3g_rrc_attempts_ps')) + _zn(row.get('e3g_rrc_attempts_ps')) + _zn(row.get('n3g_rrc_attempts_ps')))
    total_ps_nas_success = (_zn(row.get('h3g_nas_success_ps')) + _zn(row.get('e3g_nas_success_ps')) + _zn(row.get('n3g_nas_success_ps')))
    total_ps_nas_attempts = (_zn(row.get('h3g_nas_attempts_ps')) + _zn(row.get('e3g_nas_attempts_ps')) + _zn(row.get('n3g_nas_attempts_ps')))
    total_ps_rab_success = (_zn(row.get('h3g_rab_success_ps')) + _zn(row.get('e3g_rab_success_ps')) + _zn(row.get('n3g_rab_success_ps')))
    total_ps_rab_attempts = (_zn(row.get('h3g_rab_attempts_ps')) + _zn(row.get('e3g_rab_attempts_ps')) + _zn(row.get('n3g_rab_attempts_ps')))

    total_ps_ret_num = (_zn(row.get('h3g_ps_retainability_num')) + _zn(row.get('e3g_ps_retainability_num')) + _zn(row.get('n3g_ps_retainability_num')))
    total_ps_ret_denom = (_zn(row.get('h3g_ps_retainability_denom')) + _zn(row.get('e3g_ps_retainability_denom')) + _zn(row.get('n3g_ps_retainability_denom')))

    total_thpt_num = (_zn(row.get('h3g_thpt_user_dl_kbps_num')) + _zn(row.get('e3g_thpt_user_dl_kbps_num')) + _zn(row.get('n3g_thpt_user_dl_kbps_num')))
    total_thpt_denom = (_zn(row.get('h3g_thpt_user_dl_kbps_denom')) + _zn(row.get('e3g_thpt_user_dl_kbps_denom')) + _zn(row.get('n3g_thpt_user_dl_kbps_denom')))

    unified_cs_acc = ((total_cs_acc_success / total_cs_acc_attempts if total_cs_acc_attempts else 0)
                      * (total_cs_nas_success / total_cs_nas_attempts if total_cs_nas_attempts else 0)
                      * (total_cs_rab_success / total_cs_rab_attempts if total_cs_rab_attempts else 0)) * 100

    unified_cs_ret = (1 - total_cs_drop_num / total_cs_drop_denom) * 100 if total_cs_drop_denom else 0

    unified_ps_acc = ((total_ps_rrc_success / total_ps_rrc_attempts if total_ps_rrc_attempts else 0)
                      * (total_ps_nas_success / total_ps_nas_attempts if total_ps_nas_attempts else 0)
                      * (total_ps_rab_success / total_ps_rab_attempts if total_ps_rab_attempts else 0)) * 100

    unified_ps_ret = (1 - total_ps_ret_num / total_ps_ret_denom) * 100 if total_ps_ret_denom else 0

    unified_thp = (total_thpt_num / total_thpt_denom) if total_thpt_denom else 0

    unified_cqi = (
        0.25 * math.exp((1 - unified_cs_acc / 100) * -58.11779571) +
        0.25 * math.exp((1 - unified_cs_ret / 100) * -58.11779571) +
        0.15 * math.exp((1 - unified_ps_acc / 100) * -28.62016873) +
        0.15 * math.exp((1 - unified_ps_ret / 100) * -28.62016873) +
        0.20 * (1 - math.exp(unified_thp * -0.00094856))
    )

    try:
        return round(float(unified_cqi), 8)
    except Exception:
        return float(unified_cqi)

def get_umts_cqi_daily_calculated(att_name, min_date=None, max_date=None):
    """Compute UMTS (3G) unified CQI per day/site from raw counters in umts_cqi_daily.

    Returns columns: time (date), site_att, umts_cqi
    """
    engine = create_connection()
    if engine is None:
        return None
    try:
        where = ["u.site_att = :att_name"]
        params = {"att_name": att_name}
        if min_date:
            where.append("u.date >= :min_date")
            params["min_date"] = min_date
        if max_date:
            where.append("u.date <= :max_date")
            params["max_date"] = max_date
        where_clause = f"WHERE {' AND '.join(where)}" if where else ""

        # Select the necessary columns only
        sql = text(f"""
            SELECT
              u.date AS time,
              u.site_att,
              u.h3g_rrc_success_cs, u.e3g_rrc_success_cs, u.n3g_rrc_success_cs,
              u.h3g_rrc_attempts_cs, u.e3g_rrc_attempts_cs, u.n3g_rrc_attempts_cs,
              u.h3g_nas_success_cs, u.e3g_nas_success_cs, u.n3g_nas_success_cs,
              u.h3g_nas_attempts_cs, u.e3g_nas_attempts_cs, u.n3g_nas_attempts_cs,
              u.h3g_rab_success_cs, u.e3g_rab_success_cs, u.n3g_rab_success_cs,
              u.h3g_rab_attempts_cs, u.e3g_rab_attempts_cs, u.n3g_rab_attempts_cs,
              u.h3g_drop_num_cs, u.e3g_drop_num_cs, u.n3g_drop_num_cs,
              u.h3g_drop_denom_cs, u.e3g_drop_denom_cs, u.n3g_drop_denom_cs,
              u.h3g_rrc_success_ps, u.e3g_rrc_success_ps, u.n3g_rrc_success_ps,
              u.h3g_rrc_attempts_ps, u.e3g_rrc_attempts_ps, u.n3g_rrc_attempts_ps,
              u.h3g_nas_success_ps, u.e3g_nas_success_ps, u.n3g_nas_success_ps,
              u.h3g_nas_attempts_ps, u.e3g_nas_attempts_ps, u.n3g_nas_attempts_ps,
              u.h3g_rab_success_ps, u.e3g_rab_success_ps, u.n3g_rab_success_ps,
              u.h3g_rab_attempts_ps, u.e3g_rab_attempts_ps, u.n3g_rab_attempts_ps,
              u.h3g_ps_retainability_num, u.e3g_ps_retainability_num, u.n3g_ps_retainability_num,
              u.h3g_ps_retainability_denom, u.e3g_ps_retainability_denom, u.n3g_ps_retainability_denom,
              u.h3g_thpt_user_dl_kbps_num, u.e3g_thpt_user_dl_kbps_num, u.n3g_thpt_user_dl_kbps_num,
              u.h3g_thpt_user_dl_kbps_denom, u.e3g_thpt_user_dl_kbps_denom, u.n3g_thpt_user_dl_kbps_denom
            FROM umts_cqi_daily u
            {where_clause}
            ORDER BY u.site_att ASC, u.date ASC
        """)

        df = pd.read_sql(sql, engine, params=params)
        if df is None or df.empty:
            return df

        # Calculate per-row unified CQI
        df['umts_cqi'] = df.apply(calculate_unified_cqi_umts_row, axis=1)
        # Keep only output columns
        out = df[['time', 'site_att', 'umts_cqi']].copy()
        return sanitize_df(out)

    except Exception as e:
        print(f"Error computing UMTS unified CQI: {e}")
        return None
    finally:
        engine.dispose()

def sanitize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Replace +/-Inf with NaN, cast to object, then replace NaN/NA with None for JSON safety upstream."""
    if df is None:
        return None
    # 1) Replace infinities with NaN (so they are considered nulls)
    df = df.replace([np.inf, -np.inf], np.nan)
    # 2) Cast to object dtype to allow None in numeric columns
    df = df.astype(object)
    # 3) Replace NaN/NA with None
    df = df.where(pd.notnull(df), None)
    return df

def get_traffic_data_daily(att_name, min_date=None, max_date=None, technology=None, vendor=None):
    """Get traffic data daily for a single site with optional filters"""
    engine = create_connection()
    if engine is None:
        return None
    
    try:
        if technology == '3G':
            select_cols = """
          u.date AS time,
          u.site_att,
          u.h3g_traffic_d_user_ps_gb,
          u.e3g_traffic_d_user_ps_gb,
          u.n3g_traffic_d_user_ps_gb,
          NULL as h4g_traffic_d_user_ps_gb,
          NULL as s4g_traffic_d_user_ps_gb,
          NULL as e4g_traffic_d_user_ps_gb,
          NULL as n4g_traffic_d_user_ps_gb,
          NULL as e5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
          NULL as n5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
          NULL as e5g_nsa_traffic_pdcp_gb_5gendc_5gleg,
          NULL as n5g_nsa_traffic_pdcp_gb_5gendc_5gleg
            """
            from_clause = f"FROM umts_cqi_daily u"
            where_conditions = [f"u.site_att = :att_name"]
            tech_condition = "(u.h3g_traffic_d_user_ps_gb IS NOT NULL OR u.e3g_traffic_d_user_ps_gb IS NOT NULL OR u.n3g_traffic_d_user_ps_gb IS NOT NULL)"
            
        elif technology == '4G':
            select_cols = """
          l.date AS time,
          l.site_att,
          NULL as h3g_traffic_d_user_ps_gb,
          NULL as e3g_traffic_d_user_ps_gb,
          NULL as n3g_traffic_d_user_ps_gb,
          l.h4g_traffic_d_user_ps_gb,
          l.s4g_traffic_d_user_ps_gb,
          l.e4g_traffic_d_user_ps_gb,
          l.n4g_traffic_d_user_ps_gb,
          NULL as e5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
          NULL as n5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
          NULL as e5g_nsa_traffic_pdcp_gb_5gendc_5gleg,
          NULL as n5g_nsa_traffic_pdcp_gb_5gendc_5gleg
            """
            from_clause = f"FROM lte_cqi_daily l"
            where_conditions = [f"l.site_att = :att_name"]
            tech_condition = "(l.h4g_traffic_d_user_ps_gb IS NOT NULL OR l.s4g_traffic_d_user_ps_gb IS NOT NULL OR l.e4g_traffic_d_user_ps_gb IS NOT NULL OR l.n4g_traffic_d_user_ps_gb IS NOT NULL)"
            
        elif technology == '5G':
            select_cols = """
          n.date AS time,
          n.site_att,
          NULL as h3g_traffic_d_user_ps_gb,
          NULL as e3g_traffic_d_user_ps_gb,
          NULL as n3g_traffic_d_user_ps_gb,
          NULL as h4g_traffic_d_user_ps_gb,
          NULL as s4g_traffic_d_user_ps_gb,
          NULL as e4g_traffic_d_user_ps_gb,
          NULL as n4g_traffic_d_user_ps_gb,
          n.e5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
          n.n5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
          n.e5g_nsa_traffic_pdcp_gb_5gendc_5gleg,
          n.n5g_nsa_traffic_pdcp_gb_5gendc_5gleg
            """
            from_clause = f"FROM nr_cqi_daily n"
            where_conditions = [f"n.site_att = :att_name"]
            tech_condition = "(n.e5g_nsa_traffic_pdcp_gb_5gendc_4glegn IS NOT NULL OR n.n5g_nsa_traffic_pdcp_gb_5gendc_4glegn IS NOT NULL OR n.e5g_nsa_traffic_pdcp_gb_5gendc_5gleg IS NOT NULL OR n.n5g_nsa_traffic_pdcp_gb_5gendc_5gleg IS NOT NULL)"
            
        else:
            select_cols = """
          COALESCE(u.date, l.date, n.date) AS time,
          COALESCE(u.site_att, l.site_att, n.site_att) AS site_att,
          u.h3g_traffic_d_user_ps_gb,
          u.e3g_traffic_d_user_ps_gb,
          u.n3g_traffic_d_user_ps_gb,
          l.h4g_traffic_d_user_ps_gb,
          l.s4g_traffic_d_user_ps_gb,
          l.e4g_traffic_d_user_ps_gb,
          l.n4g_traffic_d_user_ps_gb,
          n.e5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
          n.n5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
          n.e5g_nsa_traffic_pdcp_gb_5gendc_5gleg,
          n.n5g_nsa_traffic_pdcp_gb_5gendc_5gleg
            """
            from_clause = f"""
        FROM
          (SELECT * FROM umts_cqi_daily WHERE site_att = :att_name) u
        FULL OUTER JOIN
          (SELECT * FROM lte_cqi_daily WHERE site_att = :att_name) l
          ON u.date = l.date AND u.site_att = l.site_att
        FULL OUTER JOIN
          (SELECT * FROM nr_cqi_daily WHERE site_att = :att_name) n
          ON COALESCE(u.date, l.date) = n.date AND COALESCE(u.site_att, l.site_att) = n.site_att
            """
            where_conditions = []
            tech_condition = None
        
        if technology in ['3G', '4G', '5G'] and tech_condition:
            where_conditions.append(tech_condition)
        
        if vendor and technology in ['3G', '4G']:
            vendor_map = {'huawei': 'h', 'ericsson': 'e', 'nokia': 'n', 'samsung': 's'}
            vendor_prefix = vendor_map.get(vendor.lower())
            if vendor_prefix:
                if technology == '3G':
                    where_conditions.append(f"(u.{vendor_prefix}3g_traffic_d_user_ps_gb IS NOT NULL)")
                elif technology == '4G':
                    where_conditions.append(f"(l.{vendor_prefix}4g_traffic_d_user_ps_gb IS NOT NULL)")
        
        if min_date:
            if technology in ['3G', '4G', '5G']:
                time_col = "date"
            else:
                time_col = "COALESCE(u.date, l.date, n.date)"
            where_conditions.append(f"{time_col} >= '{min_date}'")
            
        if max_date:
            if technology in ['3G', '4G', '5G']:
                time_col = "date"
            else:
                time_col = "COALESCE(u.date, l.date, n.date)"
            where_conditions.append(f"{time_col} <= '{max_date}'")
        
        where_clause = ""
        if where_conditions:
            where_clause = f"WHERE {' AND '.join(where_conditions)}"
        
        traffic_query = text(f"""
        SELECT {select_cols}
        {from_clause}
        {where_clause}
        ORDER BY site_att ASC, time ASC
        """)
        
        result_df = pd.read_sql(traffic_query, engine, params={'att_name': att_name})
        result_df = sanitize_df(result_df)
        print(f"Retrieved traffic data for site {att_name}: {result_df.shape[0]} records")
        return result_df
        
    except Exception as e:
        print(f"Error executing traffic data query: {e}")
        return None
    finally:
        engine.dispose()

def get_traffic_voice_daily(att_name, min_date=None, max_date=None, technology=None, vendor=None):
    """Get voice traffic daily for a single site with optional filters"""
    engine = create_connection()
    if engine is None:
        return None
    
    try:
        if technology == '3G':
            select_cols = """
          u.date AS time,
          u.site_att,
          NULL as user_traffic_volte_e,
          NULL as user_traffic_volte_h,
          NULL as user_traffic_volte_n,
          NULL as user_traffic_volte_s,
          u.h3g_traffic_v_user_cs,
          u.e3g_traffic_v_user_cs,
          u.n3g_traffic_v_user_cs
            """
            from_clause = f"FROM umts_cqi_daily u"
            where_conditions = [f"u.site_att = :att_name"]
            tech_condition = "(u.h3g_traffic_v_user_cs IS NOT NULL OR u.e3g_traffic_v_user_cs IS NOT NULL OR u.n3g_traffic_v_user_cs IS NOT NULL)"
            
        elif technology == '4G':
            select_cols = """
          v.date AS time,
          v.site_att,
          v.user_traffic_volte_e,
          v.user_traffic_volte_h,
          v.user_traffic_volte_n,
          v.user_traffic_volte_s,
          NULL as h3g_traffic_v_user_cs,
          NULL as e3g_traffic_v_user_cs,
          NULL as n3g_traffic_v_user_cs
            """
            from_clause = f"FROM volte_cqi_vendor_daily v"
            where_conditions = [f"v.site_att = :att_name"]
            tech_condition = "(v.user_traffic_volte_e IS NOT NULL OR v.user_traffic_volte_h IS NOT NULL OR v.user_traffic_volte_n IS NOT NULL OR v.user_traffic_volte_s IS NOT NULL)"
            
        else:
            select_cols = """
          COALESCE(v.date, u.date) AS time,
          COALESCE(v.site_att, u.site_att) AS site_att,
          v.user_traffic_volte_e,
          v.user_traffic_volte_h,
          v.user_traffic_volte_n,
          v.user_traffic_volte_s,
          u.h3g_traffic_v_user_cs,
          u.e3g_traffic_v_user_cs,
          u.n3g_traffic_v_user_cs
            """
            from_clause = f"""
        FROM
          (SELECT date, site_att, user_traffic_volte_e, user_traffic_volte_h, user_traffic_volte_n, user_traffic_volte_s
           FROM volte_cqi_vendor_daily
           WHERE site_att = :att_name) v
        FULL OUTER JOIN
          (SELECT date, site_att, h3g_traffic_v_user_cs, e3g_traffic_v_user_cs, n3g_traffic_v_user_cs
           FROM umts_cqi_daily
           WHERE site_att = :att_name) u
        ON v.date = u.date AND v.site_att = u.site_att
            """
            where_conditions = []
            tech_condition = None
        
        if technology in ['3G', '4G'] and tech_condition:
            where_conditions.append(tech_condition)
        
        if vendor:
            vendor_map = {'huawei': 'h', 'ericsson': 'e', 'nokia': 'n', 'samsung': 's'}
            vendor_prefix = vendor_map.get(vendor.lower())
            if vendor_prefix and technology:
                if technology == '3G':
                    where_conditions.append(f"(u.{vendor_prefix}3g_traffic_v_user_cs IS NOT NULL)")
                elif technology == '4G':
                    where_conditions.append(f"(v.user_traffic_volte_{vendor_prefix} IS NOT NULL)")
        
        if min_date:
            if technology in ['3G', '4G']:
                time_col = "date"
            else:
                time_col = "COALESCE(v.date, u.date)"
            where_conditions.append(f"{time_col} >= '{min_date}'")
            
        if max_date:
            if technology in ['3G', '4G']:
                time_col = "date"
            else:
                time_col = "COALESCE(v.date, u.date)"
            where_conditions.append(f"{time_col} <= '{max_date}'")
        
        where_clause = ""
        if where_conditions:
            where_clause = f"WHERE {' AND '.join(where_conditions)}"
        
        voice_query = text(f"""
        SELECT {select_cols}
        {from_clause}
        {where_clause}
        ORDER BY site_att ASC, time ASC
        """)
        
        result_df = pd.read_sql(voice_query, engine, params={'att_name': att_name})
        print(f"Retrieved voice traffic data for site {att_name}: {result_df.shape[0]} records")
        return result_df
        
    except Exception as e:
        print(f"Error executing voice traffic query: {e}")
        return None
    finally:
        engine.dispose()

if __name__ == "__main__":
    site_att = 'DIFALO0001'
    
    print("Testing CQI data:")
    cqi_data = get_cqi_daily(site_att, min_date='2024-01-01', max_date='2024-12-31', technology='4G')
    if cqi_data is not None:
        print(f"CQI data shape: {cqi_data.shape}")
        print(cqi_data.head())
    
    print("\nTesting Traffic data:")
    traffic_data = get_traffic_data_daily(site_att, min_date='2024-01-01', max_date='2024-12-31', technology='4G', vendor='huawei')
    if traffic_data is not None:
        print(f"Traffic data shape: {traffic_data.shape}")
        print(traffic_data.head())
    
    print("\nTesting Voice traffic data:")
    voice_data = get_traffic_voice_daily(site_att, min_date='2024-01-01', max_date='2024-12-31', technology='4G', vendor='ericsson')
    if voice_data is not None:
        print(f"Voice data shape: {voice_data.shape}")
        print(voice_data.head())