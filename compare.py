# Validation for LTE CQI row vs vectorized (both in 0..1 scale)
import pandas as pd
import numpy as np

# Import the row-based function from your file
from cell_change_evolution.select_db_cqi_daily import calculate_unified_cqi_lte_row

# Build a small sample dataset. Row0 uses combined fields; Row1 uses vendor fallbacks.
df = pd.DataFrame([
    {
        'time': '2025-01-01', 'site_att': 'SITE_A',
        # Combined fields present
        'accessibility_ps': 97.5, 'retainability_ps': 98.2, 'irat_ps': 3.0, # percent for irat -> we normalize
        'thpt_dl_kbps_ran_drb': 35000, 'f4gon3g': 80.0, # percent 4G share -> we convert to 3G share
        'ookla_latency': 25.0, 'ookla_thp': 25000.0,
        # Vendor fields present (won't be used because combined above are present)
        'h4g_rrc_success_all': 1000, 'h4g_rrc_attemps_all': 1020,
        'e4g_rrc_success_all': 800,  'e4g_rrc_attemps_all': 810,
        'n4g_rrc_success_all': 900,  'n4g_rrc_attemps_all': 910,
        's4g_rrc_success_all': 700,  's4g_rrc_attemps_all': 705,
        'h4g_s1_success': 980, 'h4g_s1_attemps': 990,
        'e4g_s1_success': 780, 'e4g_s1_attemps': 790,
        'n4g_s1_success': 880, 'n4g_s1_attemps': 890,
        's4g_s1_success': 690, 's4g_s1_attemps': 700,
        'h4g_erab_success': 970, 'h4g_erabs_attemps': 980,
        'e4g_erab_success': 770, 'e4g_erabs_attemps': 780,
        'n4g_erab_success': 870, 'n4g_erabs_attemps': 880,
        's4g_erab_success': 680, 's4g_erabs_attemps': 690,
        'h4g_retainability_num': 5, 'h4g_retainability_denom': 1000,
        'e4g_retainability_num': 6, 'e4g_retainability_denom': 1000,
        'n4g_retainability_num': 7, 'n4g_retainability_denom': 1000,
        's4g_retainability_num': 4, 's4g_retainability_denom': 1000,
        'h4g_irat_4g_to_3g_events': 10, 'h4g_erab_succ_established': 980,
        'e4g_irat_4g_to_3g_events': 8,  'e4g_erab_succ_established': 780,
        'n4g_irat_4g_to_3g_events': 9,  'n4g_erab_succ_established': 880,
        's4g_irat_4g_to_3g_events': 7,  's4g_erab_succ_established': 690,
        'h4g_thpt_user_dl_kbps_num': 1_800_000, 'h4g_thpt_user_dl_kbps_denom': 50,
        'e4g_thpt_user_dl_kbps_num': 1_400_000, 'e4g_thpt_user_dl_kbps_denom': 40,
        'n4g_thpt_user_dl_kbps_num': 1_600_000, 'n4g_thpt_user_dl_kbps_denom': 45,
        's4g_thpt_user_dl_kbps_num': 1_300_000, 's4g_thpt_user_dl_kbps_denom': 38,
        'h4g_time3g': 100, 'h4g_time4g': 900, 'e4g_time3g': 80, 'e4g_time4g': 720,
        'n4g_time3g': 90,  'n4g_time4g': 810, 's4g_time3g': 70, 's4g_time4g': 630,
        'h4g_sumavg_latency': 22000, 'e4g_sumavg_latency': 18000, 'n4g_sumavg_latency': 20000, 's4g_sumavg_latency': 16000,
        'h4g_sumavg_dl_kbps': 1_200_000, 'e4g_sumavg_dl_kbps': 1_000_000, 'n4g_sumavg_dl_kbps': 1_100_000, 's4g_sumavg_dl_kbps': 900_000,
        'h4g_summuestras': 1000, 'e4g_summuestras': 800, 'n4g_summuestras': 900, 's4g_summuestras': 700,
    },
    {
        'time': '2025-01-02', 'site_att': 'SITE_A',
        # Combined fields missing -> fallbacks used
        'accessibility_ps': None, 'retainability_ps': None, 'irat_ps': None,
        'thpt_dl_kbps_ran_drb': None, 'f4gon3g': None, 'ookla_latency': None, 'ookla_thp': None,
        # Provide only vendor fields
        'h4g_rrc_success_all': 900, 'h4g_rrc_attemps_all': 950,
        'e4g_rrc_success_all': 700,  'e4g_rrc_attemps_all': 740,
        'n4g_rrc_success_all': 800,  'n4g_rrc_attemps_all': 840,
        's4g_rrc_success_all': 600,  's4g_rrc_attemps_all': 630,
        'h4g_s1_success': 880, 'h4g_s1_attemps': 920,
        'e4g_s1_success': 680, 'e4g_s1_attemps': 710,
        'n4g_s1_success': 780, 'n4g_s1_attemps': 810,
        's4g_s1_success': 590, 's4g_s1_attemps': 610,
        'h4g_erab_success': 870, 'h4g_erabs_attemps': 910,
        'e4g_erab_success': 670, 'e4g_erabs_attemps': 700,
        'n4g_erab_success': 770, 'n4g_erabs_attemps': 800,
        's4g_erab_success': 580, 's4g_erabs_attemps': 600,
        'h4g_retainability_num': 6, 'h4g_retainability_denom': 1000,
        'e4g_retainability_num': 7, 'e4g_retainability_denom': 1000,
        'n4g_retainability_num': 8, 'n4g_retainability_denom': 1000,
        's4g_retainability_num': 5, 's4g_retainability_denom': 1000,
        'h4g_irat_4g_to_3g_events': 12, 'h4g_erab_succ_established': 910,
        'e4g_irat_4g_to_3g_events': 9,  'e4g_erab_succ_established': 700,
        'n4g_irat_4g_to_3g_events': 10, 'n4g_erab_succ_established': 800,
        's4g_irat_4g_to_3g_events': 8,  's4g_erab_succ_established': 600,
        'h4g_thpt_user_dl_kbps_num': 1_500_000, 'h4g_thpt_user_dl_kbps_denom': 55,
        'e4g_thpt_user_dl_kbps_num': 1_200_000, 'e4g_thpt_user_dl_kbps_denom': 45,
        'n4g_thpt_user_dl_kbps_num': 1_300_000, 'n4g_thpt_user_dl_kbps_denom': 50,
        's4g_thpt_user_dl_kbps_num': 1_000_000, 's4g_thpt_user_dl_kbps_denom': 40,
        'h4g_time3g': 150, 'h4g_time4g': 850, 'e4g_time3g': 100, 'e4g_time4g': 700,
        'n4g_time3g': 120, 'n4g_time4g': 780, 's4g_time3g': 90,  's4g_time4g': 610,
        'h4g_sumavg_latency': 24000, 'e4g_sumavg_latency': 19000, 'n4g_sumavg_latency': 21000, 's4g_sumavg_latency': 17000,
        'h4g_sumavg_dl_kbps': 1_100_000, 'e4g_sumavg_dl_kbps': 900_000, 'n4g_sumavg_dl_kbps': 1_000_000, 's4g_sumavg_dl_kbps': 800_000,
        'h4g_summuestras': 1100, 'e4g_summuestras': 900, 'n4g_summuestras': 950, 's4g_summuestras': 800,
    },
])

# Row-based calculation (0..1)
row_vals = df.apply(calculate_unified_cqi_lte_row, axis=1)
df['lte_cqi_row'] = row_vals

# Vectorized calculation in 0..1 scale (mirrors apply_lte_calculations without the final *100)
def _safe_div(num, den): return np.where(den != 0, num / den, 0.0)
def _zn(x): return np.where(pd.isna(x), 0.0, x)

# zero-null vendor columns
vendor_cols = [c for c in df.columns if any(p in c for p in ['h4g_', 'e4g_', 'n4g_', 's4g_'])]
df[vendor_cols] = df[vendor_cols].apply(_zn)

erab_success_total = df[['h4g_erab_success','e4g_erab_success','n4g_erab_success','s4g_erab_success']].sum(axis=1)
erabs_attemps_total = df[['h4g_erabs_attemps','e4g_erabs_attemps','n4g_erabs_attemps','s4g_erabs_attemps']].sum(axis=1)
rrc_success_total  = df[['h4g_rrc_success_all','e4g_rrc_success_all','n4g_rrc_success_all','s4g_rrc_success_all']].sum(axis=1)
rrc_attemps_total  = df[['h4g_rrc_attemps_all','e4g_rrc_attemps_all','n4g_rrc_attemps_all','s4g_rrc_attemps_all']].sum(axis=1)
s1_success_total   = df[['h4g_s1_success','e4g_s1_success','n4g_s1_success','s4g_s1_success']].sum(axis=1)
s1_attemps_total   = df[['h4g_s1_attemps','e4g_s1_attemps','n4g_s1_attemps','s4g_s1_attemps']].sum(axis=1)

retain_num_total   = df[['h4g_retainability_num','e4g_retainability_num','n4g_retainability_num','s4g_retainability_num']].sum(axis=1)
retain_den_total   = df[['h4g_retainability_denom','e4g_retainability_denom','n4g_retainability_denom','s4g_retainability_denom']].sum(axis=1)

irat_events_total  = df[['h4g_irat_4g_to_3g_events','e4g_irat_4g_to_3g_events','n4g_irat_4g_to_3g_events','s4g_irat_4g_to_3g_events']].sum(axis=1)
erab_estab_total   = df[['h4g_erab_succ_established','e4g_erab_succ_established','n4g_erab_succ_established','s4g_erab_succ_established']].sum(axis=1)

thp_num_total      = df[['h4g_thpt_user_dl_kbps_num','e4g_thpt_user_dl_kbps_num','n4g_thpt_user_dl_kbps_num','s4g_thpt_user_dl_kbps_num']].sum(axis=1)
thp_den_total      = df[['h4g_thpt_user_dl_kbps_denom','e4g_thpt_user_dl_kbps_denom','n4g_thpt_user_dl_kbps_denom','s4g_thpt_user_dl_kbps_denom']].sum(axis=1)

time3g_total       = df[['h4g_time3g','e4g_time3g','n4g_time3g','s4g_time3g']].sum(axis=1)
time4g_total       = df[['h4g_time4g','e4g_time4g','n4g_time4g','s4g_time4g']].sum(axis=1)

sumavg_latency_total = df[['h4g_sumavg_latency','e4g_sumavg_latency','n4g_sumavg_latency','s4g_sumavg_latency']].sum(axis=1)
summuestras_total  = df[['h4g_summuestras','e4g_summuestras','n4g_summuestras','s4g_summuestras']].sum(axis=1)
sumavg_dl_kbps_total = df[['h4g_sumavg_dl_kbps','e4g_sumavg_dl_kbps','n4g_sumavg_dl_kbps','s4g_sumavg_dl_kbps']].sum(axis=1)

# Totals -> KPIs
lte_acc = _safe_div(erab_success_total, erabs_attemps_total) * _safe_div(rrc_success_total, rrc_attemps_total) * _safe_div(s1_success_total, s1_attemps_total) * 100
lte_ret = (1 - _safe_div(retain_num_total, retain_den_total)) * 100
lte_irat = _safe_div(irat_events_total, erab_estab_total) * 100
lte_thp_user_dl = _safe_div(thp_num_total, thp_den_total)
lte_4g_on_3g = _safe_div(time3g_total, (time3g_total + time4g_total)) * 100  # 3G share (%)
lte_ookla_lat = _safe_div(sumavg_latency_total, summuestras_total)
lte_ookla_thp = _safe_div(sumavg_dl_kbps_total, summuestras_total)

# Vectorized CQI (0..1) — same constants as row-based
acc_p = lte_acc / 100
ret_p = lte_ret / 100
irat_p = lte_irat / 100
term_acc = 0.25 * np.exp((1 - acc_p) * -63.91668575)
term_ret = 0.25 * np.exp((1 - ret_p) * -63.91668575)
term_irat = 0.05 * np.exp(irat_p * -22.31435513)
term_thp_ran = 0.30 * (1 - np.exp(lte_thp_user_dl * -0.000282742))
term_4g3g = 0.05 * np.minimum(1, np.exp(((lte_4g_on_3g / 100) - 0.10) * -11.15717757))
term_lat = 0.05 * np.exp((lte_ookla_lat - 20.0) * -0.00526802578289131)
term_ookla = 0.05 * (1 - np.exp(lte_ookla_thp * -0.00005364793041447))
lte_cqi_vec = term_acc + term_ret + term_irat + term_thp_ran + term_4g3g + term_lat + term_ookla

df['lte_cqi_vec'] = np.round(lte_cqi_vec.astype(float), 8)
df['lte_cqi_row'] = np.round(df['lte_cqi_row'].astype(float), 8)
df['abs_diff'] = np.abs(df['lte_cqi_row'] - df['lte_cqi_vec'])

print(df[['time','lte_cqi_row','lte_cqi_vec','abs_diff']])