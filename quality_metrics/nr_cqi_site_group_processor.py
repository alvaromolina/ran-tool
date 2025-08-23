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
    Uses the same columns as nr_cqi_level_processor.py
    
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
        -- Ericsson aggregated counters
        SUM(e5g_acc_rrc_num_n) AS e5g_acc_rrc_num_n,
        SUM(e5g_s1_sr_num_n) AS e5g_s1_sr_num_n,
        SUM(e5g_nsa_acc_erab_sr_4gendc_num_n) AS e5g_nsa_acc_erab_sr_4gendc_num_n,
        SUM(e5g_acc_rrc_den_n) AS e5g_acc_rrc_den_n,
        SUM(e5g_s1_sr_den_n) AS e5g_s1_sr_den_n,
        SUM(e5g_nsa_acc_erab_sr_4gendc_den_n) AS e5g_nsa_acc_erab_sr_4gendc_den_n,
        SUM(e5g_nsa_acc_erab_succ_5gendc_5gleg_n) AS e5g_nsa_acc_erab_succ_5gendc_5gleg_n,
        SUM(e5g_nsa_acc_erab_att_5gendc_5gleg_n) AS e5g_nsa_acc_erab_att_5gendc_5gleg_n,
        SUM(e5g_nsa_ret_erab_drop_4gendc_n) AS e5g_nsa_ret_erab_drop_4gendc_n,
        SUM(e5g_nsa_ret_erab_att_4gendc_n) AS e5g_nsa_ret_erab_att_4gendc_n,
        SUM(e5g_nsa_ret_erab_drop_5gendc_4g5gleg_num_n) AS e5g_nsa_ret_erab_drop_5gendc_4g5gleg_num_n,
        SUM(e5g_nsa_ret_erab_drop_5gendc_4g5gleg_den_n) AS e5g_nsa_ret_erab_drop_5gendc_4g5gleg_den_n,
        SUM(e5g_nsa_thp_mn_num) AS e5g_nsa_thp_mn_num,
        SUM(e5g_nsa_thp_mn_den) AS e5g_nsa_thp_mn_den,
        SUM(e5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_num_n) AS e5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_num_n,
        SUM(e5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_denom_n) AS e5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_denom_n,
        
        -- Nokia aggregated counters
        SUM(n5g_acc_rrc_num_n) AS n5g_acc_rrc_num_n,
        SUM(n5g_s1_sr_num_n) AS n5g_s1_sr_num_n,
        SUM(n5g_nsa_acc_erab_sr_4gendc_num_n) AS n5g_nsa_acc_erab_sr_4gendc_num_n,
        SUM(n5g_acc_rrc_den_n) AS n5g_acc_rrc_den_n,
        SUM(n5g_s1_sr_den_n) AS n5g_s1_sr_den_n,
        SUM(n5g_nsa_acc_erab_sr_4gendc_den_n) AS n5g_nsa_acc_erab_sr_4gendc_den_n,
        SUM(n5g_nsa_acc_erab_succ_5gendc_5gleg_n) AS n5g_nsa_acc_erab_succ_5gendc_5gleg_n,
        SUM(n5g_nsa_acc_erab_att_5gendc_5gleg_n) AS n5g_nsa_acc_erab_att_5gendc_5gleg_n,
        SUM(n5g_nsa_ret_erab_drop_4gendc_n) AS n5g_nsa_ret_erab_drop_4gendc_n,
        SUM(n5g_nsa_ret_erab_att_4gendc_n) AS n5g_nsa_ret_erab_att_4gendc_n,
        SUM(n5g_nsa_ret_erab_drop_5gendc_4g5gleg_num_n) AS n5g_nsa_ret_erab_drop_5gendc_4g5gleg_num_n,
        SUM(n5g_nsa_ret_erab_drop_5gendc_4g5gleg_den_n) AS n5g_nsa_ret_erab_drop_5gendc_4g5gleg_den_n,
        SUM(n5g_nsa_thp_mn_num) AS n5g_nsa_thp_mn_num,
        SUM(n5g_nsa_thp_mn_den) AS n5g_nsa_thp_mn_den,
        SUM(n5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_num_n) AS n5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_num_n,
        SUM(n5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_denom_n) AS n5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_denom_n,
        
        -- Traffic aggregated counters
        SUM(e5g_nsa_traffic_pdcp_gb_5gendc_4glegn) AS e5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
        SUM(n5g_nsa_traffic_pdcp_gb_5gendc_4glegn) AS n5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
        SUM(e5g_nsa_traffic_pdcp_gb_5gendc_5gleg) AS e5g_nsa_traffic_pdcp_gb_5gendc_5gleg,
        SUM(n5g_nsa_traffic_pdcp_gb_5gendc_5gleg) AS n5g_nsa_traffic_pdcp_gb_5gendc_5gleg,
        SUM(e5g_nsa_traffic_mac_gb_5gendc_5gleg_n) AS e5g_nsa_traffic_mac_gb_5gendc_5gleg_n,
        SUM(n5g_nsa_traffic_mac_gb_5gendc_5gleg_n) AS n5g_nsa_traffic_mac_gb_5gendc_5gleg_n
        
        FROM public.nr_cqi_daily
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
    Uses the same columns as nr_cqi_level_processor.py
    
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
        SELECT 
        -- Ericsson aggregated counters
        SUM(e5g_acc_rrc_num_n) AS e5g_acc_rrc_num_n,
        SUM(e5g_s1_sr_num_n) AS e5g_s1_sr_num_n,
        SUM(e5g_nsa_acc_erab_sr_4gendc_num_n) AS e5g_nsa_acc_erab_sr_4gendc_num_n,
        SUM(e5g_acc_rrc_den_n) AS e5g_acc_rrc_den_n,
        SUM(e5g_s1_sr_den_n) AS e5g_s1_sr_den_n,
        SUM(e5g_nsa_acc_erab_sr_4gendc_den_n) AS e5g_nsa_acc_erab_sr_4gendc_den_n,
        SUM(e5g_nsa_acc_erab_succ_5gendc_5gleg_n) AS e5g_nsa_acc_erab_succ_5gendc_5gleg_n,
        SUM(e5g_nsa_acc_erab_att_5gendc_5gleg_n) AS e5g_nsa_acc_erab_att_5gendc_5gleg_n,
        SUM(e5g_nsa_ret_erab_drop_4gendc_n) AS e5g_nsa_ret_erab_drop_4gendc_n,
        SUM(e5g_nsa_ret_erab_att_4gendc_n) AS e5g_nsa_ret_erab_att_4gendc_n,
        SUM(e5g_nsa_ret_erab_drop_5gendc_4g5gleg_num_n) AS e5g_nsa_ret_erab_drop_5gendc_4g5gleg_num_n,
        SUM(e5g_nsa_ret_erab_drop_5gendc_4g5gleg_den_n) AS e5g_nsa_ret_erab_drop_5gendc_4g5gleg_den_n,
        SUM(e5g_nsa_thp_mn_num) AS e5g_nsa_thp_mn_num,
        SUM(e5g_nsa_thp_mn_den) AS e5g_nsa_thp_mn_den,
        SUM(e5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_num_n) AS e5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_num_n,
        SUM(e5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_denom_n) AS e5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_denom_n,
        
        -- Nokia aggregated counters
        SUM(n5g_acc_rrc_num_n) AS n5g_acc_rrc_num_n,
        SUM(n5g_s1_sr_num_n) AS n5g_s1_sr_num_n,
        SUM(n5g_nsa_acc_erab_sr_4gendc_num_n) AS n5g_nsa_acc_erab_sr_4gendc_num_n,
        SUM(n5g_acc_rrc_den_n) AS n5g_acc_rrc_den_n,
        SUM(n5g_s1_sr_den_n) AS n5g_s1_sr_den_n,
        SUM(n5g_nsa_acc_erab_sr_4gendc_den_n) AS n5g_nsa_acc_erab_sr_4gendc_den_n,
        SUM(n5g_nsa_acc_erab_succ_5gendc_5gleg_n) AS n5g_nsa_acc_erab_succ_5gendc_5gleg_n,
        SUM(n5g_nsa_acc_erab_att_5gendc_5gleg_n) AS n5g_nsa_acc_erab_att_5gendc_5gleg_n,
        SUM(n5g_nsa_ret_erab_drop_4gendc_n) AS n5g_nsa_ret_erab_drop_4gendc_n,
        SUM(n5g_nsa_ret_erab_att_4gendc_n) AS n5g_nsa_ret_erab_att_4gendc_n,
        SUM(n5g_nsa_ret_erab_drop_5gendc_4g5gleg_num_n) AS n5g_nsa_ret_erab_drop_5gendc_4g5gleg_num_n,
        SUM(n5g_nsa_ret_erab_drop_5gendc_4g5gleg_den_n) AS n5g_nsa_ret_erab_drop_5gendc_4g5gleg_den_n,
        SUM(n5g_nsa_thp_mn_num) AS n5g_nsa_thp_mn_num,
        SUM(n5g_nsa_thp_mn_den) AS n5g_nsa_thp_mn_den,
        SUM(n5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_num_n) AS n5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_num_n,
        SUM(n5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_denom_n) AS n5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_denom_n,
        
        -- Traffic aggregated counters
        SUM(e5g_nsa_traffic_pdcp_gb_5gendc_4glegn) AS e5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
        SUM(n5g_nsa_traffic_pdcp_gb_5gendc_4glegn) AS n5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
        SUM(e5g_nsa_traffic_pdcp_gb_5gendc_5gleg) AS e5g_nsa_traffic_pdcp_gb_5gendc_5gleg,
        SUM(n5g_nsa_traffic_pdcp_gb_5gendc_5gleg) AS n5g_nsa_traffic_pdcp_gb_5gendc_5gleg,
        SUM(e5g_nsa_traffic_mac_gb_5gendc_5gleg_n) AS e5g_nsa_traffic_mac_gb_5gendc_5gleg_n,
        SUM(n5g_nsa_traffic_mac_gb_5gendc_5gleg_n) AS n5g_nsa_traffic_mac_gb_5gendc_5gleg_n
        
        FROM public.nr_cqi_daily
        WHERE date >= %(min_date)s AND date <= %(max_date)s
        AND site_att IN ({site_placeholders})
    """
    
    # Build parameters dictionary
    params = {"min_date": min_date, "max_date": max_date}
    for i, site in enumerate(site_list):
        params[f"site_{i}"] = site
    
    return pd.read_sql_query(query, engine, params=params)


def apply_nr_group_calculations(df):
    """
    Apply NR calculations using vectorized numpy operations with safe wrapper functions.
    This uses the EXACT same logic as nr_cqi_level_processor.py to work with group data.
    
    Args:
        df (pandas.DataFrame): Aggregated raw data
        
    Returns:
        pandas.DataFrame: Data with calculated NR CQI metrics
    """
    # Safe wrapper functions to handle numpy compatibility issues
    def safe_divide(num, denom):
        """Safe division that handles scalar and array operations."""
        # Convert to numpy arrays with float64 dtype to avoid pandas compatibility issues
        num_array = np.asarray(num, dtype=np.float64)
        denom_array = np.asarray(denom, dtype=np.float64)
        
        # Create output array with proper dtype
        result = np.zeros_like(num_array, dtype=np.float64)
        
        # Use numpy's divide with where condition
        np.divide(num_array, denom_array, out=result, where=(denom_array != 0))
        
        # Return as scalar if input was scalar, otherwise return array
        if np.isscalar(num) and np.isscalar(denom):
            return float(result)
        else:
            return result
    
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
    
    def zn(value):
        """Zero if null - replace NaN/None with 0."""
        return np.where(pd.isna(value) | (value == None), 0, value)
    
    # Apply zn to all relevant columns at once
    vendor_cols = [col for col in df.columns if any(vendor in col for vendor in ['e5g_', 'n5g_'])]
    for col in vendor_cols:
        df[col] = zn(df[col])
    
    # Calculate intermediate totals - vectorized (EXACT same as nr_cqi_level_processor)
    # Accessibility metrics
    acc_rrc_num_total = df['e5g_acc_rrc_num_n'] + df['n5g_acc_rrc_num_n']
    acc_rrc_den_total = df['e5g_acc_rrc_den_n'] + df['n5g_acc_rrc_den_n']
    s1_sr_num_total = df['e5g_s1_sr_num_n'] + df['n5g_s1_sr_num_n']
    s1_sr_den_total = df['e5g_s1_sr_den_n'] + df['n5g_s1_sr_den_n']
    
    # ERAB accessibility for 4G EN-DC
    erab_4g_num_total = df['e5g_nsa_acc_erab_sr_4gendc_num_n'] + df['n5g_nsa_acc_erab_sr_4gendc_num_n']
    erab_4g_den_total = df['e5g_nsa_acc_erab_sr_4gendc_den_n'] + df['n5g_nsa_acc_erab_sr_4gendc_den_n']
    
    # ERAB accessibility for 5G leg
    erab_5g_num_total = df['e5g_nsa_acc_erab_succ_5gendc_5gleg_n'] + df['n5g_nsa_acc_erab_succ_5gendc_5gleg_n']
    erab_5g_den_total = df['e5g_nsa_acc_erab_att_5gendc_5gleg_n'] + df['n5g_nsa_acc_erab_att_5gendc_5gleg_n']
    
    # Retainability metrics
    ret_4g_drop_total = df['e5g_nsa_ret_erab_drop_4gendc_n'] + df['n5g_nsa_ret_erab_drop_4gendc_n']
    ret_4g_att_total = df['e5g_nsa_ret_erab_att_4gendc_n'] + df['n5g_nsa_ret_erab_att_4gendc_n']
    ret_5g_drop_total = df['e5g_nsa_ret_erab_drop_5gendc_4g5gleg_num_n'] + df['n5g_nsa_ret_erab_drop_5gendc_4g5gleg_num_n']
    ret_5g_den_total = df['e5g_nsa_ret_erab_drop_5gendc_4g5gleg_den_n'] + df['n5g_nsa_ret_erab_drop_5gendc_4g5gleg_den_n']
    
    # Throughput metrics
    thp_mn_num_total = df['e5g_nsa_thp_mn_num'] + df['n5g_nsa_thp_mn_num']
    thp_mn_den_total = df['e5g_nsa_thp_mn_den'] + df['n5g_nsa_thp_mn_den']
    thp_sn_num_total = df['e5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_num_n'] + df['n5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_num_n']
    thp_sn_den_total = df['e5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_denom_n'] + df['n5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_denom_n']
    
    # Traffic metrics
    traffic_4gleg_total = df['e5g_nsa_traffic_pdcp_gb_5gendc_4glegn'] + df['n5g_nsa_traffic_pdcp_gb_5gendc_4glegn']
    traffic_5gleg_total = df['e5g_nsa_traffic_pdcp_gb_5gendc_5gleg'] + df['n5g_nsa_traffic_pdcp_gb_5gendc_5gleg']
    traffic_mac_total = df['e5g_nsa_traffic_mac_gb_5gendc_5gleg_n'] + df['n5g_nsa_traffic_mac_gb_5gendc_5gleg_n']
    
    # CREATE ALL NEW COLUMNS AS A DICTIONARY TO AVOID FRAGMENTATION
    new_columns = {}
    
    # Total NR metrics (EXACT same formulas as nr_cqi_level_processor)
    new_columns['nr_acc_mn'] = safe_round(
        safe_divide(acc_rrc_num_total, acc_rrc_den_total) *
        safe_divide(s1_sr_num_total, s1_sr_den_total) *
        safe_divide(erab_4g_num_total, erab_4g_den_total) * 100, 8)
    
    new_columns['nr_acc_sn'] = safe_round(
        safe_divide(erab_5g_num_total, erab_5g_den_total) * 100, 8)
    
    new_columns['nr_ret_mn'] = safe_round(
        (1 - safe_divide(ret_4g_drop_total, ret_4g_att_total)) * 100, 8)
    
    new_columns['nr_endc_ret_tot'] = safe_round(
        (1 - safe_divide(ret_5g_drop_total, ret_5g_den_total)) * 100, 8)
    
    new_columns['nr_thp_mn'] = safe_round(safe_divide(thp_mn_num_total, thp_mn_den_total), 2)
    new_columns['nr_thp_sn'] = safe_round(safe_divide(thp_sn_num_total, thp_sn_den_total), 2)
    
    new_columns['nr_traffic_4gleg_gb'] = safe_round(traffic_4gleg_total, 4)
    new_columns['nr_traffic_5gleg_gb'] = safe_round(traffic_5gleg_total, 4)
    new_columns['nr_traffic_mac_gb'] = safe_round(traffic_mac_total, 4)
    
    # NR CQI Formula with VALIDATED weights and C coefficients:
    # W1*EXP((1-Acc Mn)*C1)+W2*EXP((1-Acc SN)*C2)+W3*EXP((1-Ret MN)*C3)+W4*EXP((1-Endc Ret Tot)*C4)+W5*(1-EXP(Thp MN*1000*C5))+W6*(1-EXP(Thp SN*1000*C6))
    # Weights: Acc MN 17%, Acc SN 13%, Ret MN 17%, ENDC Ret Tot 13%, Thp MN 20%, Thp SN 20%
    # C coefficients validated to match production system (difference <0.1%)
    new_columns['nr_cqi'] = safe_round(
        (0.17 * safe_exp((1 - new_columns['nr_acc_mn'] / 100) * -14.92648157) +
         0.13 * safe_exp((1 - new_columns['nr_acc_sn'] / 100) * -26.68090256) +
         0.17 * safe_exp((1 - new_columns['nr_ret_mn'] / 100) * -14.92648157) +
         0.13 * safe_exp((1 - new_columns['nr_endc_ret_tot'] / 100) * -26.68090256) +
         0.20 * (1 - safe_exp(new_columns['nr_thp_mn'] * 1000 * -0.0002006621)) +
         0.20 * (1 - safe_exp(new_columns['nr_thp_sn'] * 1000 * -0.0002006621))) * 100, 8)
    
    # CREATE DATAFRAME FROM DICTIONARY AND CONCATENATE - THIS AVOIDS FRAGMENTATION
    new_columns_df = pd.DataFrame(new_columns)
    result_df = pd.concat([df, new_columns_df], axis=1)
    
    return result_df


def get_nr_cqi_daily_for_site_group(site_list, date_range, csv_export=False):
    """
    Calculate daily NR CQI values for a specific group of sites.
    Returns a DataFrame with one row per date for plotting purposes.
    
    Args:
        site_list (list): List of site_att values to include in the group
        date_range (tuple): Tuple of (min_date, max_date) in 'YYYY-MM-DD' format
        csv_export (bool): Whether to export results to CSV
        
    Returns:
        pandas.DataFrame: DataFrame with columns: date, nr_cqi, nr_acc_mn, nr_acc_sn, 
                         nr_ret_mn, nr_endc_ret_tot, nr_thp_mn, nr_thp_sn, 
                         nr_traffic_4gleg_gb, nr_traffic_5gleg_gb, nr_traffic_mac_gb
    """
    print(f"Calculating daily NR CQI for site group: {site_list}")
    print(f"Date range: {date_range[0]} to {date_range[1]}")
    
    if not site_list:
        print("No sites provided")
        return pd.DataFrame()
    
    engine = get_engine()
    
    try:
        # Debug: First check if any data exists for these sites
        site_placeholders = ', '.join([f"%(site_{i})s" for i in range(len(site_list))])
        debug_query = f"""
            SELECT site_att, COUNT(*) as row_count, MIN(date) as min_date, MAX(date) as max_date
            FROM public.nr_cqi_daily
            WHERE site_att IN ({site_placeholders})
            GROUP BY site_att
        """
        debug_params = {}
        for i, site in enumerate(site_list):
            debug_params[f"site_{i}"] = site
        
        debug_df = pd.read_sql_query(debug_query, engine, params=debug_params)
        print(f"Debug - Site data availability:")
        print(debug_df)
        
        if debug_df.empty:
            print(f"Debug - No data found for any of these sites in nr_cqi_daily table")
            # Check what sites are actually available
            available_query = "SELECT site_att, COUNT(*) as row_count FROM public.nr_cqi_daily GROUP BY site_att ORDER BY site_att LIMIT 10"
            available_df = pd.read_sql_query(available_query, engine)
            print(f"Available NR sites (first 10):")
            print(available_df)
            return pd.DataFrame()
        
        # Get daily aggregated data for the site group
        df_daily = get_daily_aggregated_data_for_group(site_list, date_range[0], date_range[1], engine)
        
        if df_daily.empty:
            print(f"No data found for sites: {site_list}")
            return pd.DataFrame()
        
        print(f"Retrieved daily data for {len(site_list)} sites across {len(df_daily)} days")
        
        # Apply NR calculations to each day
        df_calculated = apply_nr_group_calculations(df_daily)
        
        # Select only the columns needed for plotting
        result_columns = ['date', 'nr_cqi', 'nr_acc_mn', 'nr_acc_sn', 'nr_ret_mn', 
                         'nr_endc_ret_tot', 'nr_thp_mn', 'nr_thp_sn', 
                         'nr_traffic_4gleg_gb', 'nr_traffic_5gleg_gb', 'nr_traffic_mac_gb']
        df_result = df_calculated[result_columns].copy()
        
        print(f"Calculated daily NR CQI for {len(df_result)} days")
        print(f"Average NR CQI: {df_result['nr_cqi'].mean():.8f}")
        
        # Export to CSV if requested
        if csv_export:
            sites_filename = '_'.join(site_list[:3])  # Use first 3 sites for filename
            if len(site_list) > 3:
                sites_filename += f"_and_{len(site_list)-3}_more"
            
            filename = f"nr_cqi_daily_{sites_filename}_{date_range[0]}_to_{date_range[1]}.csv"
            
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


def get_nr_cqi_for_single_site(site_att, date_range, csv_export=False):
    """
    Get NR CQI and components for a single site for a period of time.
    UPDATED: Now performs LIVE calculations using validated formula instead of pre-calculated values.
    
    Args:
        site_att (str): Site name to retrieve data for
        date_range (tuple): Tuple of (min_date, max_date) in 'YYYY-MM-DD' format
        csv_export (bool): Whether to export results to CSV
        
    Returns:
        pandas.DataFrame: DataFrame with daily metrics for the single site
    """
    print(f"Calculating NR CQI for single site (LIVE calculation): {site_att}")
    print(f"Date range: {date_range[0]} to {date_range[1]}")
    
    if not site_att:
        print("No site provided")
        return pd.DataFrame()
    
    engine = get_engine()
    
    try:
        # Get raw daily aggregated data for the single site (same as group function)
        df_raw = get_daily_aggregated_data_for_group([site_att], date_range[0], date_range[1], engine)
        
        if df_raw.empty:
            print(f"No raw data found for site: {site_att}")
            return pd.DataFrame()
        
        print(f"Retrieved raw data for {len(df_raw)} days")
        
        # Apply the same NR metrics calculation as the group function
        df_calculated = apply_nr_group_calculations(df_raw)
        
        if df_calculated.empty:
            print(f"Calculation failed for site: {site_att}")
            return pd.DataFrame()
        
        # Select the same columns as the original function for compatibility
        result_columns = ['date', 'nr_cqi', 'nr_acc_mn', 'nr_acc_sn', 'nr_ret_mn', 
                         'nr_endc_ret_tot', 'nr_thp_mn', 'nr_thp_sn', 
                         'nr_traffic_4gleg_gb', 'nr_traffic_5gleg_gb', 'nr_traffic_mac_gb']
        
        # Add missing columns if they don't exist
        for col in result_columns:
            if col not in df_calculated.columns:
                if col == 'date':
                    continue  # date should always be there
                else:
                    df_calculated[col] = 0.0
        
        # Add metadata columns for compatibility
        df_calculated['region'] = df_calculated.get('region', 'Unknown')
        df_calculated['province'] = df_calculated.get('province', 'Unknown')
        df_calculated['municipality'] = df_calculated.get('municipality', 'Unknown')
        df_calculated['site_att'] = site_att
        df_calculated['vendors'] = df_calculated.get('vendors', 'Unknown')
        df_calculated['group_level'] = 'site'
        
        # Select final columns in the expected order
        final_columns = ['date', 'region', 'province', 'municipality', 'site_att', 'vendors', 'group_level'] + result_columns[1:]
        df_result = df_calculated[final_columns].copy()
        
        print(f"Calculated NR CQI for {len(df_result)} days")
        print(f"Average NR CQI (LIVE calculated): {df_result['nr_cqi'].mean():.8f}")
        
        # Export to CSV if requested
        if csv_export:
            filename = f"nr_cqi_single_site_LIVE_{site_att}_{date_range[0]}_to_{date_range[1]}.csv"
            
            if ROOT_DIRECTORY:
                output_path = os.path.join(ROOT_DIRECTORY, "output", filename)
            else:
                output_path = os.path.join("output", filename)
            
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df_result.to_csv(output_path, index=False)
            print(f"Results exported to CSV: {output_path}")
        
        return df_result
        
    except SQLAlchemyError as e:
        print(f"SQLAlchemy Error during single site calculation: {e}")
        import traceback
        traceback.print_exc()
        raise
    except Exception as e:
        print(f"General Error during single site calculation: {e}")
        import traceback
        traceback.print_exc()
        raise


def get_nr_cqi_for_site_group(site_list, date_range, csv_export=False):
    """
    Calculate NR CQI for a specific group of sites using the proven methodology
    from nr_cqi_level_processor.py.
    
    Args:
        site_list (list): List of site_att values to include in the group
        date_range (tuple): Tuple of (min_date, max_date) in 'YYYY-MM-DD' format
        csv_export (bool): Whether to export results to CSV
        
    Returns:
        pandas.DataFrame: DataFrame with calculated NR CQI metrics for the site group
    """
    print(f"Calculating NR CQI for site group: {site_list}")
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
        
        # Apply NR calculations to get the CQI
        df_calculated = apply_nr_group_calculations(df_raw)
        
        # Add group metadata
        df_calculated['site_count'] = len(site_list)
        df_calculated['site_list'] = ', '.join(site_list)
        df_calculated['group_type'] = 'neighbor_group'
        
        print(f"Calculated NR CQI: {df_calculated['nr_cqi'].iloc[0]:.8f}")
        
        # Export to CSV if requested
        if csv_export:
            sites_filename = '_'.join(site_list[:3])  # Use first 3 sites for filename
            if len(site_list) > 3:
                sites_filename += f"_and_{len(site_list)-3}_more"
            
            filename = f"nr_cqi_group_{sites_filename}_{date_range[0]}_to_{date_range[1]}.csv"
            
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
    Process multiple neighbor groups and return their NR CQI values.
    
    Args:
        neighbor_groups_dict (dict): Dictionary where keys are group names and values are lists of site_att
        date_range (tuple): Tuple of (min_date, max_date) in 'YYYY-MM-DD' format
        csv_export (bool): Whether to export results to CSV
        
    Returns:
        pandas.DataFrame: DataFrame with NR CQI results for all groups
    """
    print(f"Processing {len(neighbor_groups_dict)} neighbor groups")
    
    results = []
    
    for group_name, site_list in neighbor_groups_dict.items():
        print(f"\nProcessing group: {group_name}")
        
        try:
            df_group = get_nr_cqi_for_site_group(site_list, date_range, csv_export=False)
            
            if not df_group.empty:
                # Add group name to the result
                df_group['group_name'] = group_name
                results.append(df_group)
                print(f"✓ Group '{group_name}': NR CQI = {df_group['nr_cqi'].iloc[0]:.8f}")
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
        filename = f"nr_cqi_all_neighbor_groups_{date_range[0]}_to_{date_range[1]}.csv"
        
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
    test_min_date = "2025-01-01"
    test_max_date = "2025-01-15"
    
    print("Testing nr_cqi_group_processor...")
    
    # Test 1: Aggregated NR CQI for the entire period
    print("\n1. Testing aggregated NR CQI calculation...")
    try:
        df_result = get_nr_cqi_for_site_group(
            site_list=test_sites,
            date_range=(test_min_date, test_max_date),
            csv_export=True
        )
        
        if not df_result.empty:
            result = df_result['nr_cqi'].iloc[0]
            print(f"Aggregated test completed successfully. NR CQI: {result:.8f}")
        else:
            print("Aggregated test failed - no result returned")
            
    except Exception as e:
        print(f"Error during aggregated test: {e}")
        import traceback
        traceback.print_exc()
    
    # Test 2: Daily NR CQI values for plotting
    print("\n2. Testing daily NR CQI calculation...")
    try:
        df_daily = get_nr_cqi_daily_for_site_group(
            site_list=test_sites,
            date_range=(test_min_date, test_max_date),
            csv_export=True
        )
        
        if not df_daily.empty:
            avg_cqi = df_daily['nr_cqi'].mean()
            print(f"Daily test completed successfully. {len(df_daily)} days processed.")
            print(f"Average daily NR CQI: {avg_cqi:.8f}")
            print(f"Date range: {df_daily['date'].min()} to {df_daily['date'].max()}")
        else:
            print("Daily test failed - no result returned")
            
    except Exception as e:
        print(f"Error during daily test: {e}")
        import traceback
        traceback.print_exc()
    
    # Test 3: Single site NR CQI retrieval
    print("\n3. Testing single site NR CQI retrieval...")
    try:
        test_single_site = test_sites[0]  # Use first site from the test group
        df_single = get_nr_cqi_for_single_site(
            site_att=test_single_site,
            date_range=(test_min_date, test_max_date),
            csv_export=True
        )
        
        if not df_single.empty:
            avg_cqi = df_single['nr_cqi'].mean()
            print(f"Single site test completed successfully. {len(df_single)} days processed.")
            print(f"Site: {test_single_site}")
            print(f"Average NR CQI: {avg_cqi:.8f}")
            print(f"Date range: {df_single['date'].min()} to {df_single['date'].max()}")
        else:
            print("Single site test failed - no result returned")
            
    except Exception as e:
        print(f"Error during single site test: {e}")
        import traceback
        traceback.print_exc()