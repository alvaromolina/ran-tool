import pandas as pd
import numpy as np
import os
import dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Load environment variables
dotenv.load_dotenv()
POSTGRES_USERNAME = os.getenv("POSTGRES_USERNAME")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
ROOT_DIRECTORY = os.getenv("ROOT_DIRECTORY")


def get_engine():
    """Create and return a database engine."""
    required_vars = {
        'POSTGRES_USERNAME': POSTGRES_USERNAME,
        'POSTGRES_PASSWORD': POSTGRES_PASSWORD,
        'POSTGRES_HOST': POSTGRES_HOST,
        'POSTGRES_PORT': POSTGRES_PORT,
        'POSTGRES_DB': POSTGRES_DB
    }
    
    for var_name, var_value in required_vars.items():
        if var_value is None or var_value == 'None' or var_value == '':
            raise ValueError(f"Environment variable {var_name} is not set or is None")
    
    try:
        int(POSTGRES_PORT)
    except (ValueError, TypeError) as e:
        raise ValueError(f"POSTGRES_PORT must be a valid integer, got: {POSTGRES_PORT}")
    
    connection_string = f"postgresql+psycopg2://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    return create_engine(connection_string)


def get_daily_aggregated_data_for_group(site_list, min_date, max_date, engine):
    """
    Get daily aggregated raw data for a specific list of sites.
    Returns one row per date with aggregated counters for that day.
    
    Args:
        site_list (list): List of site_att values to filter by
        min_date (str): Start date
        max_date (str): End date
        engine: SQLAlchemy engine
        
    Returns:
        pandas.DataFrame: Daily aggregated raw data for the site group
    """
    # Create placeholders for the site list
    site_placeholders = ', '.join([f"%(site_{i})s" for i in range(len(site_list))])
    
    query = f"""
        SELECT date,
        
        SUM(h4g_erab_success) AS h4g_erab_success,
        SUM(h4g_erabs_attemps) AS h4g_erabs_attemps,
        SUM(h4g_rrc_success_all) AS h4g_rrc_success_all,
        SUM(h4g_rrc_attemps_all) AS h4g_rrc_attemps_all,
        SUM(h4g_s1_success) AS h4g_s1_success,
        SUM(h4g_s1_attemps) AS h4g_s1_attemps,
        SUM(h4g_retainability_num) AS h4g_retainability_num,
        SUM(h4g_retainability_denom) AS h4g_retainability_denom,
        SUM(h4g_thpt_user_dl_kbps_num) AS h4g_thpt_user_dl_kbps_num,
        SUM(h4g_thpt_user_dl_kbps_denom) AS h4g_thpt_user_dl_kbps_denom,
        SUM(h4g_irat_4g_to_3g_events) AS h4g_irat_4g_to_3g_events,
        SUM(h4g_time3g) AS h4g_time3g,
        SUM(h4g_time4g) AS h4g_time4g,
        SUM(h4g_sumavg_latency) AS h4g_sumavg_latency,
        SUM(h4g_summuestras) AS h4g_summuestras,
        SUM(h4g_sumavg_dl_kbps) AS h4g_sumavg_dl_kbps,
        SUM(h4g_traffic_d_user_ps_gb) AS h4g_traffic_d_user_ps_gb,
        
        SUM(e4g_erab_success) AS e4g_erab_success,
        SUM(e4g_erabs_attemps) AS e4g_erabs_attemps,
        SUM(e4g_rrc_success_all) AS e4g_rrc_success_all,
        SUM(e4g_rrc_attemps_all) AS e4g_rrc_attemps_all,
        SUM(e4g_s1_success) AS e4g_s1_success,
        SUM(e4g_s1_attemps) AS e4g_s1_attemps,
        SUM(e4g_retainability_num) AS e4g_retainability_num,
        SUM(e4g_retainability_denom) AS e4g_retainability_denom,
        SUM(e4g_thpt_user_dl_kbps_num) AS e4g_thpt_user_dl_kbps_num,
        SUM(e4g_thpt_user_dl_kbps_denom) AS e4g_thpt_user_dl_kbps_denom,
        SUM(e4g_irat_4g_to_3g_events) AS e4g_irat_4g_to_3g_events,
        SUM(e4g_time3g) AS e4g_time3g,
        SUM(e4g_time4g) AS e4g_time4g,
        SUM(e4g_sumavg_latency) AS e4g_sumavg_latency,
        SUM(e4g_summuestras) AS e4g_summuestras,
        SUM(e4g_sumavg_dl_kbps) AS e4g_sumavg_dl_kbps,
        SUM(e4g_traffic_d_user_ps_gb) AS e4g_traffic_d_user_ps_gb,
        
        SUM(n4g_erab_success) AS n4g_erab_success,
        SUM(n4g_erabs_attemps) AS n4g_erabs_attemps,
        SUM(n4g_rrc_success_all) AS n4g_rrc_success_all,
        SUM(n4g_rrc_attemps_all) AS n4g_rrc_attemps_all,
        SUM(n4g_s1_success) AS n4g_s1_success,
        SUM(n4g_s1_attemps) AS n4g_s1_attemps,
        SUM(n4g_retainability_num) AS n4g_retainability_num,
        SUM(n4g_retainability_denom) AS n4g_retainability_denom,
        SUM(n4g_thpt_user_dl_kbps_num) AS n4g_thpt_user_dl_kbps_num,
        SUM(n4g_thpt_user_dl_kbps_denom) AS n4g_thpt_user_dl_kbps_denom,
        SUM(n4g_irat_4g_to_3g_events) AS n4g_irat_4g_to_3g_events,
        SUM(n4g_time3g) AS n4g_time3g,
        SUM(n4g_time4g) AS n4g_time4g,
        SUM(n4g_sumavg_latency) AS n4g_sumavg_latency,
        SUM(n4g_summuestras) AS n4g_summuestras,
        SUM(n4g_sumavg_dl_kbps) AS n4g_sumavg_dl_kbps,
        SUM(n4g_traffic_d_user_ps_gb) AS n4g_traffic_d_user_ps_gb,
        
        SUM(s4g_erab_success) AS s4g_erab_success,
        SUM(s4g_erabs_attemps) AS s4g_erabs_attemps,
        SUM(s4g_rrc_success_all) AS s4g_rrc_success_all,
        SUM(s4g_rrc_attemps_all) AS s4g_rrc_attemps_all,
        SUM(s4g_s1_success) AS s4g_s1_success,
        SUM(s4g_s1_attemps) AS s4g_s1_attemps,
        SUM(s4g_retainability_num) AS s4g_retainability_num,
        SUM(s4g_retainability_denom) AS s4g_retainability_denom,
        SUM(s4g_thpt_user_dl_kbps_num) AS s4g_thpt_user_dl_kbps_num,
        SUM(s4g_thpt_user_dl_kbps_denom) AS s4g_thpt_user_dl_kbps_denom,
        SUM(s4g_irat_4g_to_3g_events) AS s4g_irat_4g_to_3g_events,
        SUM(s4g_time3g) AS s4g_time3g,
        SUM(s4g_time4g) AS s4g_time4g,
        SUM(s4g_sumavg_latency) AS s4g_sumavg_latency,
        SUM(s4g_summuestras) AS s4g_summuestras,
        SUM(s4g_sumavg_dl_kbps) AS s4g_sumavg_dl_kbps,
        SUM(s4g_traffic_d_user_ps_gb) AS s4g_traffic_d_user_ps_gb
        
        FROM public.lte_cqi_daily
        WHERE date >= %(min_date)s AND date <= %(max_date)s
        AND site_att IN ({site_placeholders})
        GROUP BY date
        ORDER BY date
    """
    
    # Build parameters dictionary
    params = {"min_date": min_date, "max_date": max_date}
    for i, site in enumerate(site_list):
        params[f"site_{i}"] = site
    
    return pd.read_sql_query(query, engine, params=params)


def get_aggregated_data_for_group(site_list, min_date, max_date, engine):
    """
    Get aggregated raw data for a specific list of sites using simple SQL grouping.
    This simulates the 'network' level but filtered by specific sites.
    
    Args:
        site_list (list): List of site_att values to filter by
        min_date (str): Start date
        max_date (str): End date
        engine: SQLAlchemy engine
        
    Returns:
        pandas.DataFrame: Aggregated raw data for the site group
    """
    # Create placeholders for the site list
    site_placeholders = ', '.join([f"%(site_{i})s" for i in range(len(site_list))])
    
    query = f"""
        SELECT %(min_date)s as date,
        
        SUM(h4g_erab_success) AS h4g_erab_success,
        SUM(h4g_erabs_attemps) AS h4g_erabs_attemps,
        SUM(h4g_rrc_success_all) AS h4g_rrc_success_all,
        SUM(h4g_rrc_attemps_all) AS h4g_rrc_attemps_all,
        SUM(h4g_s1_success) AS h4g_s1_success,
        SUM(h4g_s1_attemps) AS h4g_s1_attemps,
        SUM(h4g_retainability_num) AS h4g_retainability_num,
        SUM(h4g_retainability_denom) AS h4g_retainability_denom,
        SUM(h4g_thpt_user_dl_kbps_num) AS h4g_thpt_user_dl_kbps_num,
        SUM(h4g_thpt_user_dl_kbps_denom) AS h4g_thpt_user_dl_kbps_denom,
        SUM(h4g_irat_4g_to_3g_events) AS h4g_irat_4g_to_3g_events,
        SUM(h4g_time3g) AS h4g_time3g,
        SUM(h4g_time4g) AS h4g_time4g,
        SUM(h4g_sumavg_latency) AS h4g_sumavg_latency,
        SUM(h4g_summuestras) AS h4g_summuestras,
        SUM(h4g_sumavg_dl_kbps) AS h4g_sumavg_dl_kbps,
        SUM(h4g_traffic_d_user_ps_gb) AS h4g_traffic_d_user_ps_gb,
        
        SUM(e4g_erab_success) AS e4g_erab_success,
        SUM(e4g_erabs_attemps) AS e4g_erabs_attemps,
        SUM(e4g_rrc_success_all) AS e4g_rrc_success_all,
        SUM(e4g_rrc_attemps_all) AS e4g_rrc_attemps_all,
        SUM(e4g_s1_success) AS e4g_s1_success,
        SUM(e4g_s1_attemps) AS e4g_s1_attemps,
        SUM(e4g_retainability_num) AS e4g_retainability_num,
        SUM(e4g_retainability_denom) AS e4g_retainability_denom,
        SUM(e4g_thpt_user_dl_kbps_num) AS e4g_thpt_user_dl_kbps_num,
        SUM(e4g_thpt_user_dl_kbps_denom) AS e4g_thpt_user_dl_kbps_denom,
        SUM(e4g_irat_4g_to_3g_events) AS e4g_irat_4g_to_3g_events,
        SUM(e4g_time3g) AS e4g_time3g,
        SUM(e4g_time4g) AS e4g_time4g,
        SUM(e4g_sumavg_latency) AS e4g_sumavg_latency,
        SUM(e4g_summuestras) AS e4g_summuestras,
        SUM(e4g_sumavg_dl_kbps) AS e4g_sumavg_dl_kbps,
        SUM(e4g_traffic_d_user_ps_gb) AS e4g_traffic_d_user_ps_gb,
        
        SUM(n4g_erab_success) AS n4g_erab_success,
        SUM(n4g_erabs_attemps) AS n4g_erabs_attemps,
        SUM(n4g_rrc_success_all) AS n4g_rrc_success_all,
        SUM(n4g_rrc_attemps_all) AS n4g_rrc_attemps_all,
        SUM(n4g_s1_success) AS n4g_s1_success,
        SUM(n4g_s1_attemps) AS n4g_s1_attemps,
        SUM(n4g_retainability_num) AS n4g_retainability_num,
        SUM(n4g_retainability_denom) AS n4g_retainability_denom,
        SUM(n4g_thpt_user_dl_kbps_num) AS n4g_thpt_user_dl_kbps_num,
        SUM(n4g_thpt_user_dl_kbps_denom) AS n4g_thpt_user_dl_kbps_denom,
        SUM(n4g_irat_4g_to_3g_events) AS n4g_irat_4g_to_3g_events,
        SUM(n4g_time3g) AS n4g_time3g,
        SUM(n4g_time4g) AS n4g_time4g,
        SUM(n4g_sumavg_latency) AS n4g_sumavg_latency,
        SUM(n4g_summuestras) AS n4g_summuestras,
        SUM(n4g_sumavg_dl_kbps) AS n4g_sumavg_dl_kbps,
        SUM(n4g_traffic_d_user_ps_gb) AS n4g_traffic_d_user_ps_gb,
        
        SUM(s4g_erab_success) AS s4g_erab_success,
        SUM(s4g_erabs_attemps) AS s4g_erabs_attemps,
        SUM(s4g_rrc_success_all) AS s4g_rrc_success_all,
        SUM(s4g_rrc_attemps_all) AS s4g_rrc_attemps_all,
        SUM(s4g_s1_success) AS s4g_s1_success,
        SUM(s4g_s1_attemps) AS s4g_s1_attemps,
        SUM(s4g_retainability_num) AS s4g_retainability_num,
        SUM(s4g_retainability_denom) AS s4g_retainability_denom,
        SUM(s4g_thpt_user_dl_kbps_num) AS s4g_thpt_user_dl_kbps_num,
        SUM(s4g_thpt_user_dl_kbps_denom) AS s4g_thpt_user_dl_kbps_denom,
        SUM(s4g_irat_4g_to_3g_events) AS s4g_irat_4g_to_3g_events,
        SUM(s4g_time3g) AS s4g_time3g,
        SUM(s4g_time4g) AS s4g_time4g,
        SUM(s4g_sumavg_latency) AS s4g_sumavg_latency,
        SUM(s4g_summuestras) AS s4g_summuestras,
        SUM(s4g_sumavg_dl_kbps) AS s4g_sumavg_dl_kbps,
        SUM(s4g_traffic_d_user_ps_gb) AS s4g_traffic_d_user_ps_gb
        
        FROM public.lte_cqi_daily
        WHERE date >= %(min_date)s AND date <= %(max_date)s
        AND site_att IN ({site_placeholders})
    """
    
    # Build parameters dictionary
    params = {"min_date": min_date, "max_date": max_date}
    for i, site in enumerate(site_list):
        params[f"site_{i}"] = site
    
    return pd.read_sql_query(query, engine, params=params)


def apply_lte_group_calculations(df):
    """
    Apply LTE calculations using vectorized numpy operations with safe wrapper functions.
    This is adapted from lte_cqi_level_processor.py to work with group data.
    
    Args:
        df (pandas.DataFrame): Aggregated raw data
        
    Returns:
        pandas.DataFrame: Data with calculated LTE CQI metrics
    """
    # Safe wrapper functions to handle numpy compatibility issues
    def safe_divide(num, denom):
        """Safe division that handles scalar and array operations."""
        try:
            # Handle pandas Series
            if hasattr(denom, 'where'):
                return num.where(denom != 0, 0) / denom.where(denom != 0, 1)
            # Handle numpy arrays
            elif hasattr(denom, '__len__') and not isinstance(denom, str):
                return np.divide(num, denom, out=np.zeros_like(num, dtype=float), where=(denom!=0))
            # Handle scalars
            else:
                return num / denom if denom != 0 else 0
        except Exception as e:
            print(f"Safe divide error: {e}")
            # Return zeros with same shape as input
            if hasattr(num, 'shape'):
                return np.zeros_like(num, dtype=float)
            else:
                return 0
    
    def safe_round(value, decimals=8):
        """Safe rounding that handles scalar and array operations."""
        try:
            return np.round(value, decimals)
        except:
            # Fallback for mixed types
            if np.isscalar(value):
                return round(float(value), decimals)
            else:
                return np.array([round(float(v), decimals) for v in value])
    
    def safe_exp(value):
        """Safe exponential that handles scalar and array operations."""
        try:
            return np.exp(value)
        except:
            # Fallback for mixed types
            if np.isscalar(value):
                return np.exp(float(value))
            else:
                return np.array([np.exp(float(v)) for v in value])
    
    def safe_minimum(a, b):
        """Safe minimum that handles scalar and array operations."""
        try:
            return np.minimum(a, b)
        except:
            # Fallback for mixed types
            if np.isscalar(a) and np.isscalar(b):
                return min(float(a), float(b))
            else:
                return np.array([min(float(av), float(bv)) for av, bv in zip(np.atleast_1d(a), np.atleast_1d(b))])
    
    def zn(value):
        """Zero if null - replace NaN/None with 0."""
        return np.where(pd.isna(value) | (value == None), 0, value)
    
    # Apply zn to all relevant columns at once
    vendor_cols = [col for col in df.columns if any(vendor in col for vendor in ['h4g_', 'e4g_', 'n4g_', 's4g_'])]
    for col in vendor_cols:
        df[col] = zn(df[col])
    
    # Calculate total accessibility (all vendors) - vectorized
    erab_success_total = df['h4g_erab_success'] + df['e4g_erab_success'] + df['n4g_erab_success'] + df['s4g_erab_success']
    erabs_attemps_total = df['h4g_erabs_attemps'] + df['e4g_erabs_attemps'] + df['n4g_erabs_attemps'] + df['s4g_erabs_attemps']
    rrc_success_total = df['h4g_rrc_success_all'] + df['e4g_rrc_success_all'] + df['n4g_rrc_success_all'] + df['s4g_rrc_success_all']
    rrc_attemps_total = df['h4g_rrc_attemps_all'] + df['e4g_rrc_attemps_all'] + df['n4g_rrc_attemps_all'] + df['s4g_rrc_attemps_all']
    s1_success_total = df['h4g_s1_success'] + df['e4g_s1_success'] + df['n4g_s1_success'] + df['s4g_s1_success']
    s1_attemps_total = df['h4g_s1_attemps'] + df['e4g_s1_attemps'] + df['n4g_s1_attemps'] + df['s4g_s1_attemps']
    
    # Calculate total retainability (all vendors) - vectorized
    retainability_num_total = df['h4g_retainability_num'] + df['e4g_retainability_num'] + df['n4g_retainability_num'] + df['s4g_retainability_num']
    retainability_denom_total = df['h4g_retainability_denom'] + df['e4g_retainability_denom'] + df['n4g_retainability_denom'] + df['s4g_retainability_denom']
    
    # Calculate total IRAT (all vendors) - vectorized
    irat_events_total = df['h4g_irat_4g_to_3g_events'] + df['e4g_irat_4g_to_3g_events'] + df['n4g_irat_4g_to_3g_events'] + df['s4g_irat_4g_to_3g_events']
    
    # Calculate total user throughput DL (all vendors) - vectorized
    thpt_num_total = df['h4g_thpt_user_dl_kbps_num'] + df['e4g_thpt_user_dl_kbps_num'] + df['n4g_thpt_user_dl_kbps_num'] + df['s4g_thpt_user_dl_kbps_num']
    thpt_denom_total = df['h4g_thpt_user_dl_kbps_denom'] + df['e4g_thpt_user_dl_kbps_denom'] + df['n4g_thpt_user_dl_kbps_denom'] + df['s4g_thpt_user_dl_kbps_denom']
    
    # Calculate total 4G on 3G (all vendors) - vectorized
    time3g_total = df['h4g_time3g'] + df['e4g_time3g'] + df['n4g_time3g'] + df['s4g_time3g']
    time4g_total = df['h4g_time4g'] + df['e4g_time4g'] + df['n4g_time4g'] + df['s4g_time4g']
    
    # Calculate total Ookla latency (all vendors) - vectorized
    sumavg_latency_total = df['h4g_sumavg_latency'] + df['e4g_sumavg_latency'] + df['n4g_sumavg_latency'] + df['s4g_sumavg_latency']
    summuestras_total = df['h4g_summuestras'] + df['e4g_summuestras'] + df['n4g_summuestras'] + df['s4g_summuestras']
    
    # Calculate total Ookla throughput (all vendors) - vectorized
    sumavg_dl_kbps_total = df['h4g_sumavg_dl_kbps'] + df['e4g_sumavg_dl_kbps'] + df['n4g_sumavg_dl_kbps'] + df['s4g_sumavg_dl_kbps']
    
    # Calculate total traffic (all vendors) - vectorized
    traffic_total = df['h4g_traffic_d_user_ps_gb'] + df['e4g_traffic_d_user_ps_gb'] + df['n4g_traffic_d_user_ps_gb'] + df['s4g_traffic_d_user_ps_gb']
    
    # CREATE ALL NEW COLUMNS AS A DICTIONARY TO AVOID FRAGMENTATION
    new_columns = {}
    
    # Total metrics using safe functions
    new_columns['lte_acc'] = safe_round(
        safe_divide(erab_success_total, erabs_attemps_total) *
        safe_divide(rrc_success_total, rrc_attemps_total) *
        safe_divide(s1_success_total, s1_attemps_total) * 100, 8)
    
    new_columns['lte_ret'] = safe_round((1 - safe_divide(retainability_num_total, retainability_denom_total)) * 100, 8)
    
    new_columns['lte_irat'] = safe_round(safe_divide(irat_events_total, erab_success_total) * 100, 8)
    
    new_columns['lte_thp_user_dl'] = safe_round(safe_divide(thpt_num_total, thpt_denom_total), 8)
    
    new_columns['lte_4g_on_3g'] = safe_round(safe_divide(time3g_total, time3g_total + time4g_total) * 100, 8)
    
    new_columns['lte_ookla_lat'] = safe_round(safe_divide(sumavg_latency_total, summuestras_total), 8)
    
    new_columns['lte_ookla_thp'] = safe_round(safe_divide(sumavg_dl_kbps_total, summuestras_total), 8)
    
    new_columns['lte_traff'] = safe_round(traffic_total / 1024, 8)
    
    # Calculate total LTE CQI (all vendors) - vectorized with safe functions
    new_columns['lte_cqi'] = safe_round(
        (0.25 * safe_exp((1 - new_columns['lte_acc'] / 100) * -63.91668575) +
         0.25 * safe_exp((1 - new_columns['lte_ret'] / 100) * -63.91668575) +
         0.05 * safe_exp((new_columns['lte_irat'] / 100) * -22.31435513) +
         0.30 * (1 - safe_exp(new_columns['lte_thp_user_dl'] * -0.000282742)) +
         0.05 * safe_minimum(1, safe_exp((new_columns['lte_4g_on_3g'] / 100 - 10 / 100) * -11.15717757)) +
         0.05 * safe_exp((new_columns['lte_ookla_lat'] - 20) * -0.00526802578289131) +
         0.05 * (1 - safe_exp(new_columns['lte_ookla_thp'] * -0.00005364793041447))) * 100, 8)
    
    # CREATE DATAFRAME FROM DICTIONARY AND CONCATENATE - THIS AVOIDS FRAGMENTATION
    new_columns_df = pd.DataFrame(new_columns)
    result_df = pd.concat([df, new_columns_df], axis=1)
    
    return result_df


def get_lte_cqi_daily_for_site_group(site_list, date_range, csv_export=False):
    """
    Calculate daily LTE CQI values for a specific group of sites.
    Returns a DataFrame with one row per date for plotting purposes.
    
    Args:
        site_list (list): List of site_att values to include in the group
        date_range (tuple): Tuple of (min_date, max_date) in 'YYYY-MM-DD' format
        csv_export (bool): Whether to export results to CSV
        
    Returns:
        pandas.DataFrame: DataFrame with columns: date, lte_cqi, lte_acc, lte_ret, 
                         lte_irat, lte_thp_user_dl, lte_4g_on_3g, lte_ookla_lat, 
                         lte_ookla_thp, lte_traff
    """
    print(f"Calculating daily LTE CQI for site group: {site_list}")
    print(f"Date range: {date_range[0]} to {date_range[1]}")
    
    if not site_list:
        print("No sites provided")
        return pd.DataFrame()
    
    engine = get_engine()
    
    try:
        # Get daily aggregated data for the site group
        df_daily = get_daily_aggregated_data_for_group(site_list, date_range[0], date_range[1], engine)
        
        if df_daily.empty:
            print(f"No data found for sites: {site_list}")
            return pd.DataFrame()
        
        print(f"Retrieved daily data for {len(site_list)} sites across {len(df_daily)} days")
        
        # Apply LTE calculations to each day
        df_calculated = apply_lte_group_calculations(df_daily)
        
        # Select only the columns needed for plotting
        result_columns = ['date', 'lte_cqi', 'lte_acc', 'lte_ret', 'lte_irat', 
                         'lte_thp_user_dl', 'lte_4g_on_3g', 'lte_ookla_lat', 
                         'lte_ookla_thp', 'lte_traff']
        df_result = df_calculated[result_columns].copy()
        
        print(f"Calculated daily LTE CQI for {len(df_result)} days")
        print(f"Average LTE CQI: {df_result['lte_cqi'].mean():.8f}")
        
        # Export to CSV if requested
        if csv_export:
            sites_filename = '_'.join(site_list[:3])  # Use first 3 sites for filename
            if len(site_list) > 3:
                sites_filename += f"_and_{len(site_list)-3}_more"
            
            filename = f"lte_cqi_daily_{sites_filename}_{date_range[0]}_to_{date_range[1]}.csv"
            
            if ROOT_DIRECTORY:
                output_path = os.path.join(ROOT_DIRECTORY, "output", filename)
            else:
                output_path = os.path.join("output", filename)
            
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df_result.to_csv(output_path, index=False)
            print(f"Daily results exported to CSV: {output_path}")
        
        return df_result
        
    except SQLAlchemyError as e:
        print(f"SQLAlchemy Error during daily group calculation: {e}")
        import traceback
        traceback.print_exc()
        raise
    except Exception as e:
        print(f"General Error during daily group calculation: {e}")
        import traceback
        traceback.print_exc()
        raise


def get_lte_cqi_for_single_site(site_att, date_range, csv_export=False):
    """
    Get LTE CQI and components for a single site for a period of time.
    Uses pre-calculated daily metrics from lte_cqi_metrics_daily table.
    
    Args:
        site_att (str): Site name to retrieve data for
        date_range (tuple): Tuple of (min_date, max_date) in 'YYYY-MM-DD' format
        csv_export (bool): Whether to export results to CSV
        
    Returns:
        pandas.DataFrame: DataFrame with daily metrics for the single site
    """
    print(f"Retrieving LTE CQI data for single site: {site_att}")
    print(f"Date range: {date_range[0]} to {date_range[1]}")
    
    if not site_att:
        print("No site provided")
        return pd.DataFrame()
    
    engine = get_engine()
    
    try:
        with engine.connect() as conn:
            query = """
                SELECT date, region, province, municipality, site_att, vendors, group_level,
                       lte_cqi, lte_acc, lte_ret, lte_irat, lte_thp_user_dl, lte_4g_on_3g, 
                       lte_ookla_lat, lte_ookla_thp, lte_traff
                FROM lte_cqi_metrics_daily
                WHERE date >= :min_date AND date <= :max_date
                AND site_att = :site_att
                AND group_level = 'site'
                ORDER BY date
            """
            
            params = {
                "min_date": date_range[0], 
                "max_date": date_range[1],
                "site_att": site_att
            }
            
            result = conn.execute(text(query), params)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        if df.empty:
            print(f"No data found for site: {site_att}")
            return pd.DataFrame()
        
        print(f"Retrieved data for {len(df)} days")
        print(f"Average LTE CQI: {df['lte_cqi'].mean():.8f}")
        
        # Export to CSV if requested
        if csv_export:
            filename = f"lte_cqi_single_site_{site_att}_{date_range[0]}_to_{date_range[1]}.csv"
            
            if ROOT_DIRECTORY:
                output_path = os.path.join(ROOT_DIRECTORY, "output", filename)
            else:
                output_path = os.path.join("output", filename)
            
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df.to_csv(output_path, index=False)
            print(f"Results exported to CSV: {output_path}")
        
        return df
        
    except SQLAlchemyError as e:
        print(f"SQLAlchemy Error during single site query: {e}")
        import traceback
        traceback.print_exc()
        raise
    except Exception as e:
        print(f"General Error during single site query: {e}")
        import traceback
        traceback.print_exc()
        raise


def get_lte_cqi_for_site_group(site_list, date_range, csv_export=False):
    """
    Calculate LTE CQI for a specific group of sites using the proven methodology
    from lte_cqi_level_processor.py.
    
    Args:
        site_list (list): List of site_att values to include in the group
        date_range (tuple): Tuple of (min_date, max_date) in 'YYYY-MM-DD' format
        csv_export (bool): Whether to export results to CSV
        
    Returns:
        pandas.DataFrame: DataFrame with calculated LTE CQI metrics for the site group
    """
    print(f"Calculating LTE CQI for site group: {site_list}")
    print(f"Date range: {date_range[0]} to {date_range[1]}")
    
    if not site_list:
        print("No sites provided")
        return pd.DataFrame()
    
    engine = get_engine()
    
    try:
        # Get aggregated raw data for the site group
        df_raw = get_aggregated_data_for_group(site_list, date_range[0], date_range[1], engine)
        
        if df_raw.empty:
            print(f"No data found for sites: {site_list}")
            return pd.DataFrame()
        
        print(f"Retrieved raw data for {len(site_list)} sites")
        
        # Apply LTE calculations to get the CQI
        df_calculated = apply_lte_group_calculations(df_raw)
        
        # Add group metadata
        df_calculated['site_count'] = len(site_list)
        df_calculated['site_list'] = ', '.join(site_list)
        df_calculated['group_type'] = 'neighbor_group'
        
        print(f"Calculated LTE CQI: {df_calculated['lte_cqi'].iloc[0]:.8f}")
        
        # Export to CSV if requested
        if csv_export:
            sites_filename = '_'.join(site_list[:3])  # Use first 3 sites for filename
            if len(site_list) > 3:
                sites_filename += f"_and_{len(site_list)-3}_more"
            
            filename = f"lte_cqi_group_{sites_filename}_{date_range[0]}_to_{date_range[1]}.csv"
            
            if ROOT_DIRECTORY:
                output_path = os.path.join(ROOT_DIRECTORY, "output", filename)
            else:
                output_path = os.path.join("output", filename)
            
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df_calculated.to_csv(output_path, index=False)
            print(f"Results exported to CSV: {output_path}")
        
        return df_calculated
        
    except SQLAlchemyError as e:
        print(f"SQLAlchemy Error during group calculation: {e}")
        import traceback
        traceback.print_exc()
        raise
    except Exception as e:
        print(f"General Error during group calculation: {e}")
        import traceback
        traceback.print_exc()
        raise


def process_neighbor_groups(neighbor_groups_dict, date_range, csv_export=False):
    """
    Process multiple neighbor groups and return their LTE CQI values.
    
    Args:
        neighbor_groups_dict (dict): Dictionary where keys are group names and values are lists of site_att
        date_range (tuple): Tuple of (min_date, max_date) in 'YYYY-MM-DD' format
        csv_export (bool): Whether to export results to CSV
        
    Returns:
        pandas.DataFrame: DataFrame with LTE CQI results for all groups
    """
    print(f"Processing {len(neighbor_groups_dict)} neighbor groups")
    
    results = []
    
    for group_name, site_list in neighbor_groups_dict.items():
        print(f"\nProcessing group: {group_name}")
        
        try:
            df_group = get_lte_cqi_for_site_group(site_list, date_range, csv_export=False)
            
            if not df_group.empty:
                # Add group name to the result
                df_group['group_name'] = group_name
                results.append(df_group)
                print(f"✓ Group '{group_name}': LTE CQI = {df_group['lte_cqi'].iloc[0]:.8f}")
            else:
                print(f"✗ Group '{group_name}': No data found")
                
        except Exception as e:
            print(f"✗ Error processing group '{group_name}': {e}")
    
    if not results:
        print("No results generated for any groups")
        return pd.DataFrame()
    
    # Combine all results
    df_all_results = pd.concat(results, ignore_index=True)
    
    # Export combined results if requested
    if csv_export:
        filename = f"lte_cqi_all_neighbor_groups_{date_range[0]}_to_{date_range[1]}.csv"
        
        if ROOT_DIRECTORY:
            output_path = os.path.join(ROOT_DIRECTORY, "output", filename)
        else:
            output_path = os.path.join("output", filename)
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df_all_results.to_csv(output_path, index=False)
        print(f"\nCombined results exported to CSV: {output_path}")
    
    return df_all_results


if __name__ == "__main__":
    # Test with a small group of sites
    test_sites = ["MEXMET0396", "MEXMET0411", "MEXMET0403"]
    test_min_date = "2024-09-01"
    test_max_date = "2024-09-15"
    test_cluster = "test_cluster"
    
    print("Testing lte_cqi_group_processor...")
    
    # Test 1: Aggregated LTE CQI for the entire period
    print("\n1. Testing aggregated LTE CQI calculation...")
    try:
        df_result = get_lte_cqi_for_site_group(
            site_list=test_sites,
            date_range=(test_min_date, test_max_date),
            csv_export=True
        )
        
        if not df_result.empty:
            result = df_result['lte_cqi'].iloc[0]
            print(f"Aggregated test completed successfully. LTE CQI: {result:.8f}")
        else:
            print("Aggregated test failed - no result returned")
            
    except Exception as e:
        print(f"Error during aggregated test: {e}")
        import traceback
        traceback.print_exc()
    
    # Test 2: Daily LTE CQI values for plotting
    print("\n2. Testing daily LTE CQI calculation...")
    try:
        df_daily = get_lte_cqi_daily_for_site_group(
            site_list=test_sites,
            date_range=(test_min_date, test_max_date),
            csv_export=True
        )
        
        if not df_daily.empty:
            avg_cqi = df_daily['lte_cqi'].mean()
            print(f"Daily test completed successfully. {len(df_daily)} days processed.")
            print(f"Average daily LTE CQI: {avg_cqi:.8f}")
            print(f"Date range: {df_daily['date'].min()} to {df_daily['date'].max()}")
        else:
            print("Daily test failed - no result returned")
            
    except Exception as e:
        print(f"Error during daily test: {e}")
        import traceback
        traceback.print_exc()
    
    # Test 3: Single site LTE CQI retrieval
    print("\n3. Testing single site LTE CQI retrieval...")
    try:
        test_single_site = test_sites[0]  # Use first site from the test group
        df_single = get_lte_cqi_for_single_site(
            site_att=test_single_site,
            date_range=(test_min_date, test_max_date),
            csv_export=True
        )
        
        if not df_single.empty:
            avg_cqi = df_single['lte_cqi'].mean()
            print(f"Single site test completed successfully. {len(df_single)} days processed.")
            print(f"Site: {test_single_site}")
            print(f"Average LTE CQI: {avg_cqi:.8f}")
            print(f"Date range: {df_single['date'].min()} to {df_single['date'].max()}")
        else:
            print("Single site test failed - no result returned")
            
    except Exception as e:
        print(f"Error during single site test: {e}")
        import traceback
        traceback.print_exc()
        