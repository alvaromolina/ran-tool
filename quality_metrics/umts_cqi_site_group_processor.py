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
        SELECT 
        date,
        -- Huawei aggregated counters
        SUM(h3g_rrc_success_cs) AS h3g_rrc_success_cs,
        SUM(h3g_rrc_attempts_cs) AS h3g_rrc_attempts_cs,
        SUM(h3g_nas_success_cs) AS h3g_nas_success_cs,
        SUM(h3g_nas_attempts_cs) AS h3g_nas_attempts_cs,
        SUM(h3g_rab_success_cs) AS h3g_rab_success_cs,
        SUM(h3g_rab_attempts_cs) AS h3g_rab_attempts_cs,
        SUM(h3g_drop_num_cs) AS h3g_drop_num_cs,
        SUM(h3g_drop_denom_cs) AS h3g_drop_denom_cs,
        SUM(h3g_rrc_success_ps) AS h3g_rrc_success_ps,
        SUM(h3g_rrc_attempts_ps) AS h3g_rrc_attempts_ps,
        SUM(h3g_nas_success_ps) AS h3g_nas_success_ps,
        SUM(h3g_nas_attempts_ps) AS h3g_nas_attempts_ps,
        SUM(h3g_rab_success_ps) AS h3g_rab_success_ps,
        SUM(h3g_rab_attempts_ps) AS h3g_rab_attempts_ps,
        SUM(h3g_ps_retainability_num) AS h3g_ps_retainability_num,
        SUM(h3g_ps_retainability_denom) AS h3g_ps_retainability_denom,
        SUM(h3g_thpt_user_dl_kbps_num) AS h3g_thpt_user_dl_kbps_num,
        SUM(h3g_thpt_user_dl_kbps_denom) AS h3g_thpt_user_dl_kbps_denom,
        SUM(h3g_traffic_v_user_cs) AS h3g_traffic_v_user_cs,
        SUM(h3g_traffic_d_user_ps_gb) AS h3g_traffic_d_user_ps_gb,
        
        -- Ericsson aggregated counters
        SUM(e3g_rrc_success_cs) AS e3g_rrc_success_cs,
        SUM(e3g_rrc_attempts_cs) AS e3g_rrc_attempts_cs,
        SUM(e3g_nas_success_cs) AS e3g_nas_success_cs,
        SUM(e3g_nas_attempts_cs) AS e3g_nas_attempts_cs,
        SUM(e3g_rab_success_cs) AS e3g_rab_success_cs,
        SUM(e3g_rab_attempts_cs) AS e3g_rab_attempts_cs,
        SUM(e3g_drop_num_cs) AS e3g_drop_num_cs,
        SUM(e3g_drop_denom_cs) AS e3g_drop_denom_cs,
        SUM(e3g_rrc_success_ps) AS e3g_rrc_success_ps,
        SUM(e3g_rrc_attempts_ps) AS e3g_rrc_attempts_ps,
        SUM(e3g_nas_success_ps) AS e3g_nas_success_ps,
        SUM(e3g_nas_attempts_ps) AS e3g_nas_attempts_ps,
        SUM(e3g_rab_success_ps) AS e3g_rab_success_ps,
        SUM(e3g_rab_attempts_ps) AS e3g_rab_attempts_ps,
        SUM(e3g_ps_retainability_num) AS e3g_ps_retainability_num,
        SUM(e3g_ps_retainability_denom) AS e3g_ps_retainability_denom,
        SUM(e3g_thpt_user_dl_kbps_num) AS e3g_thpt_user_dl_kbps_num,
        SUM(e3g_thpt_user_dl_kbps_denom) AS e3g_thpt_user_dl_kbps_denom,
        SUM(e3g_traffic_v_user_cs) AS e3g_traffic_v_user_cs,
        SUM(e3g_traffic_d_user_ps_gb) AS e3g_traffic_d_user_ps_gb,
        
        -- Nokia aggregated counters
        SUM(n3g_rrc_success_cs) AS n3g_rrc_success_cs,
        SUM(n3g_rrc_attempts_cs) AS n3g_rrc_attempts_cs,
        SUM(n3g_nas_success_cs) AS n3g_nas_success_cs,
        SUM(n3g_nas_attempts_cs) AS n3g_nas_attempts_cs,
        SUM(n3g_rab_success_cs) AS n3g_rab_success_cs,
        SUM(n3g_rab_attempts_cs) AS n3g_rab_attempts_cs,
        SUM(n3g_drop_num_cs) AS n3g_drop_num_cs,
        SUM(n3g_drop_denom_cs) AS n3g_drop_denom_cs,
        SUM(n3g_rrc_success_ps) AS n3g_rrc_success_ps,
        SUM(n3g_rrc_attempts_ps) AS n3g_rrc_attempts_ps,
        SUM(n3g_nas_success_ps) AS n3g_nas_success_ps,
        SUM(n3g_nas_attempts_ps) AS n3g_nas_attempts_ps,
        SUM(n3g_rab_success_ps) AS n3g_rab_success_ps,
        SUM(n3g_rab_attempts_ps) AS n3g_rab_attempts_ps,
        SUM(n3g_ps_retainability_num) AS n3g_ps_retainability_num,
        SUM(n3g_ps_retainability_denom) AS n3g_ps_retainability_denom,
        SUM(n3g_thpt_user_dl_kbps_num) AS n3g_thpt_user_dl_kbps_num,
        SUM(n3g_thpt_user_dl_kbps_denom) AS n3g_thpt_user_dl_kbps_denom,
        SUM(n3g_traffic_v_user_cs) AS n3g_traffic_v_user_cs,
        SUM(n3g_traffic_d_user_ps_gb) AS n3g_traffic_d_user_ps_gb
        
        FROM public.umts_cqi_daily
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
        SELECT 
        -- Huawei aggregated counters
        SUM(h3g_rrc_success_cs) AS h3g_rrc_success_cs,
        SUM(h3g_rrc_attempts_cs) AS h3g_rrc_attempts_cs,
        SUM(h3g_nas_success_cs) AS h3g_nas_success_cs,
        SUM(h3g_nas_attempts_cs) AS h3g_nas_attempts_cs,
        SUM(h3g_rab_success_cs) AS h3g_rab_success_cs,
        SUM(h3g_rab_attempts_cs) AS h3g_rab_attempts_cs,
        SUM(h3g_drop_num_cs) AS h3g_drop_num_cs,
        SUM(h3g_drop_denom_cs) AS h3g_drop_denom_cs,
        SUM(h3g_rrc_success_ps) AS h3g_rrc_success_ps,
        SUM(h3g_rrc_attempts_ps) AS h3g_rrc_attempts_ps,
        SUM(h3g_nas_success_ps) AS h3g_nas_success_ps,
        SUM(h3g_nas_attempts_ps) AS h3g_nas_attempts_ps,
        SUM(h3g_rab_success_ps) AS h3g_rab_success_ps,
        SUM(h3g_rab_attempts_ps) AS h3g_rab_attempts_ps,
        SUM(h3g_ps_retainability_num) AS h3g_ps_retainability_num,
        SUM(h3g_ps_retainability_denom) AS h3g_ps_retainability_denom,
        SUM(h3g_thpt_user_dl_kbps_num) AS h3g_thpt_user_dl_kbps_num,
        SUM(h3g_thpt_user_dl_kbps_denom) AS h3g_thpt_user_dl_kbps_denom,
        SUM(h3g_traffic_v_user_cs) AS h3g_traffic_v_user_cs,
        SUM(h3g_traffic_d_user_ps_gb) AS h3g_traffic_d_user_ps_gb,
        
        -- Ericsson aggregated counters
        SUM(e3g_rrc_success_cs) AS e3g_rrc_success_cs,
        SUM(e3g_rrc_attempts_cs) AS e3g_rrc_attempts_cs,
        SUM(e3g_nas_success_cs) AS e3g_nas_success_cs,
        SUM(e3g_nas_attempts_cs) AS e3g_nas_attempts_cs,
        SUM(e3g_rab_success_cs) AS e3g_rab_success_cs,
        SUM(e3g_rab_attempts_cs) AS e3g_rab_attempts_cs,
        SUM(e3g_drop_num_cs) AS e3g_drop_num_cs,
        SUM(e3g_drop_denom_cs) AS e3g_drop_denom_cs,
        SUM(e3g_rrc_success_ps) AS e3g_rrc_success_ps,
        SUM(e3g_rrc_attempts_ps) AS e3g_rrc_attempts_ps,
        SUM(e3g_nas_success_ps) AS e3g_nas_success_ps,
        SUM(e3g_nas_attempts_ps) AS e3g_nas_attempts_ps,
        SUM(e3g_rab_success_ps) AS e3g_rab_success_ps,
        SUM(e3g_rab_attempts_ps) AS e3g_rab_attempts_ps,
        SUM(e3g_ps_retainability_num) AS e3g_ps_retainability_num,
        SUM(e3g_ps_retainability_denom) AS e3g_ps_retainability_denom,
        SUM(e3g_thpt_user_dl_kbps_num) AS e3g_thpt_user_dl_kbps_num,
        SUM(e3g_thpt_user_dl_kbps_denom) AS e3g_thpt_user_dl_kbps_denom,
        SUM(e3g_traffic_v_user_cs) AS e3g_traffic_v_user_cs,
        SUM(e3g_traffic_d_user_ps_gb) AS e3g_traffic_d_user_ps_gb,
        
        -- Nokia aggregated counters
        SUM(n3g_rrc_success_cs) AS n3g_rrc_success_cs,
        SUM(n3g_rrc_attempts_cs) AS n3g_rrc_attempts_cs,
        SUM(n3g_nas_success_cs) AS n3g_nas_success_cs,
        SUM(n3g_nas_attempts_cs) AS n3g_nas_attempts_cs,
        SUM(n3g_rab_success_cs) AS n3g_rab_success_cs,
        SUM(n3g_rab_attempts_cs) AS n3g_rab_attempts_cs,
        SUM(n3g_drop_num_cs) AS n3g_drop_num_cs,
        SUM(n3g_drop_denom_cs) AS n3g_drop_denom_cs,
        SUM(n3g_rrc_success_ps) AS n3g_rrc_success_ps,
        SUM(n3g_rrc_attempts_ps) AS n3g_rrc_attempts_ps,
        SUM(n3g_nas_success_ps) AS n3g_nas_success_ps,
        SUM(n3g_nas_attempts_ps) AS n3g_nas_attempts_ps,
        SUM(n3g_rab_success_ps) AS n3g_rab_success_ps,
        SUM(n3g_rab_attempts_ps) AS n3g_rab_attempts_ps,
        SUM(n3g_ps_retainability_num) AS n3g_ps_retainability_num,
        SUM(n3g_ps_retainability_denom) AS n3g_ps_retainability_denom,
        SUM(n3g_thpt_user_dl_kbps_num) AS n3g_thpt_user_dl_kbps_num,
        SUM(n3g_thpt_user_dl_kbps_denom) AS n3g_thpt_user_dl_kbps_denom,
        SUM(n3g_traffic_v_user_cs) AS n3g_traffic_v_user_cs,
        SUM(n3g_traffic_d_user_ps_gb) AS n3g_traffic_d_user_ps_gb
        
        FROM public.umts_cqi_daily
        WHERE date >= %(min_date)s AND date <= %(max_date)s
        AND site_att IN ({site_placeholders})
    """
    
    # Build parameters dictionary
    params = {"min_date": min_date, "max_date": max_date}
    for i, site in enumerate(site_list):
        params[f"site_{i}"] = site
    
    return pd.read_sql_query(query, engine, params=params)


def apply_umts_group_calculations(df):
    """
    Apply UMTS calculations using vectorized numpy operations with safe wrapper functions.
    This uses the exact same logic as umts_cqi_level_processor.py to work with group data.
    
    Args:
        df (pandas.DataFrame): Aggregated raw data
        
    Returns:
        pandas.DataFrame: Data with calculated UMTS CQI metrics
    """
    # Safe wrapper functions to handle numpy compatibility issues
    def safe_divide(num, denom):
        """Safe division that handles scalar and array operations."""
        try:
            return np.where(denom != 0, num / denom, 0)
        except:
            # Fallback for mixed scalar/array operations
            if np.isscalar(denom):
                return num / denom if denom != 0 else 0
            else:
                return np.divide(num, denom, out=np.zeros_like(num, dtype=float), where=(denom!=0))
    
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
    vendor_cols = [col for col in df.columns if any(vendor in col for vendor in ['h3g_', 'e3g_', 'n3g_'])]
    for col in vendor_cols:
        df[col] = zn(df[col])
    
    # Calculate intermediate totals - vectorized (same as umts_cqi_level_processor)
    rrc_success_cs_total = df['h3g_rrc_success_cs'] + df['e3g_rrc_success_cs'] + df['n3g_rrc_success_cs']
    rrc_attempts_cs_total = df['h3g_rrc_attempts_cs'] + df['e3g_rrc_attempts_cs'] + df['n3g_rrc_attempts_cs']
    nas_success_cs_total = df['h3g_nas_success_cs'] + df['e3g_nas_success_cs'] + df['n3g_nas_success_cs']
    nas_attempts_cs_total = df['h3g_nas_attempts_cs'] + df['e3g_nas_attempts_cs'] + df['n3g_nas_attempts_cs']
    rab_success_cs_total = df['h3g_rab_success_cs'] + df['e3g_rab_success_cs'] + df['n3g_rab_success_cs']
    rab_attempts_cs_total = df['h3g_rab_attempts_cs'] + df['e3g_rab_attempts_cs'] + df['n3g_rab_attempts_cs']
    
    rrc_success_ps_total = df['h3g_rrc_success_ps'] + df['e3g_rrc_success_ps'] + df['n3g_rrc_success_ps']
    rrc_attempts_ps_total = df['h3g_rrc_attempts_ps'] + df['e3g_rrc_attempts_ps'] + df['n3g_rrc_attempts_ps']
    nas_success_ps_total = df['h3g_nas_success_ps'] + df['e3g_nas_success_ps'] + df['n3g_nas_success_ps']
    nas_attempts_ps_total = df['h3g_nas_attempts_ps'] + df['e3g_nas_attempts_ps'] + df['n3g_nas_attempts_ps']
    rab_success_ps_total = df['h3g_rab_success_ps'] + df['e3g_rab_success_ps'] + df['n3g_rab_success_ps']
    rab_attempts_ps_total = df['h3g_rab_attempts_ps'] + df['e3g_rab_attempts_ps'] + df['n3g_rab_attempts_ps']
    
    drop_num_cs_total = df['h3g_drop_num_cs'] + df['e3g_drop_num_cs'] + df['n3g_drop_num_cs']
    drop_denom_cs_total = df['h3g_drop_denom_cs'] + df['e3g_drop_denom_cs'] + df['n3g_drop_denom_cs']
    
    ps_ret_num_total = df['h3g_ps_retainability_num'] + df['e3g_ps_retainability_num'] + df['n3g_ps_retainability_num']
    ps_ret_denom_total = df['h3g_ps_retainability_denom'] + df['e3g_ps_retainability_denom'] + df['n3g_ps_retainability_denom']
    
    thp_num_total = df['h3g_thpt_user_dl_kbps_num'] + df['e3g_thpt_user_dl_kbps_num'] + df['n3g_thpt_user_dl_kbps_num']
    thp_denom_total = df['h3g_thpt_user_dl_kbps_denom'] + df['e3g_thpt_user_dl_kbps_denom'] + df['n3g_thpt_user_dl_kbps_denom']
    
    traffic_voice_total = df['h3g_traffic_v_user_cs'] + df['e3g_traffic_v_user_cs'] + df['n3g_traffic_v_user_cs']
    traffic_data_total = df['h3g_traffic_d_user_ps_gb'] + df['e3g_traffic_d_user_ps_gb'] + df['n3g_traffic_d_user_ps_gb']
    
    # CREATE ALL NEW COLUMNS AS A DICTIONARY TO AVOID FRAGMENTATION
    new_columns = {}
    
    # Total UMTS metrics using safe functions (exact same formulas as umts_cqi_level_processor)
    new_columns['umts_acc_cs'] = safe_round(
        safe_divide(rrc_success_cs_total, rrc_attempts_cs_total) *
        safe_divide(nas_success_cs_total, nas_attempts_cs_total) *
        safe_divide(rab_success_cs_total, rab_attempts_cs_total) * 100, 8)
    
    new_columns['umts_acc_ps'] = safe_round(
        safe_divide(rrc_success_ps_total, rrc_attempts_ps_total) *
        safe_divide(nas_success_ps_total, nas_attempts_ps_total) *
        safe_divide(rab_success_ps_total, rab_attempts_ps_total) * 100, 8)
    
    new_columns['umts_ret_cs'] = safe_round((1 - safe_divide(drop_num_cs_total, drop_denom_cs_total)) * 100, 8)
    new_columns['umts_ret_ps'] = safe_round((1 - safe_divide(ps_ret_num_total, ps_ret_denom_total)) * 100, 8)
    new_columns['umts_thp_dl'] = safe_round(safe_divide(thp_num_total, thp_denom_total), 2)
    new_columns['umts_traff_voice'] = safe_round(traffic_voice_total, 4)
    new_columns['umts_traff_data'] = safe_round(traffic_data_total, 4)
    
    # Calculate total UMTS CQI using safe functions (exact same formula as umts_cqi_level_processor)
    new_columns['umts_cqi'] = safe_round(
        (0.25 * safe_exp((1 - new_columns['umts_acc_cs'] / 100) * -58.11779571) +
         0.25 * safe_exp((1 - new_columns['umts_ret_cs'] / 100) * -58.11779571) +
         0.15 * safe_exp((1 - new_columns['umts_acc_ps'] / 100) * -28.62016873) +
         0.15 * safe_exp((1 - new_columns['umts_ret_ps'] / 100) * -28.62016873) +
         0.20 * (1 - safe_exp(new_columns['umts_thp_dl'] * -0.00094856))) * 100, 8)
    
    # CREATE DATAFRAME FROM DICTIONARY AND CONCATENATE - THIS AVOIDS FRAGMENTATION
    new_columns_df = pd.DataFrame(new_columns)
    result_df = pd.concat([df, new_columns_df], axis=1)
    
    return result_df


def get_umts_cqi_daily_for_site_group(site_list, date_range, csv_export=False):
    """
    Calculate daily UMTS CQI values for a specific group of sites.
    Returns a DataFrame with one row per date for plotting purposes.
    
    Args:
        site_list (list): List of site_att values to include in the group
        date_range (tuple): Tuple of (min_date, max_date) in 'YYYY-MM-DD' format
        csv_export (bool): Whether to export results to CSV
        
    Returns:
        pandas.DataFrame: DataFrame with columns: date, umts_cqi, umts_acc_cs, umts_acc_ps, 
                         umts_ret_cs, umts_ret_ps, umts_thp_dl, umts_traff_voice, umts_traff_data
    """
    print(f"Calculating daily UMTS CQI for site group: {site_list}")
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
        
        # Apply UMTS calculations to each day
        df_calculated = apply_umts_group_calculations(df_daily)
        
        # Select only the columns needed for plotting
        result_columns = ['date', 'umts_cqi', 'umts_acc_cs', 'umts_acc_ps', 'umts_ret_cs', 
                         'umts_ret_ps', 'umts_thp_dl', 'umts_traff_voice', 'umts_traff_data']
        df_result = df_calculated[result_columns].copy()
        
        print(f"Calculated daily UMTS CQI for {len(df_result)} days")
        print(f"Average UMTS CQI: {df_result['umts_cqi'].mean():.8f}")
        
        # Export to CSV if requested
        if csv_export:
            sites_filename = '_'.join(site_list[:3])  # Use first 3 sites for filename
            if len(site_list) > 3:
                sites_filename += f"_and_{len(site_list)-3}_more"
            
            filename = f"umts_cqi_daily_{sites_filename}_{date_range[0]}_to_{date_range[1]}.csv"
            
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


def get_umts_cqi_for_single_site(site_att, date_range, csv_export=False):
    """
    Get UMTS CQI and components for a single site for a period of time.
    Uses pre-calculated daily metrics from umts_cqi_metrics_daily table.
    
    Args:
        site_att (str): Site name to retrieve data for
        date_range (tuple): Tuple of (min_date, max_date) in 'YYYY-MM-DD' format
        csv_export (bool): Whether to export results to CSV
        
    Returns:
        pandas.DataFrame: DataFrame with daily metrics for the single site
    """
    print(f"Retrieving UMTS CQI data for single site: {site_att}")
    print(f"Date range: {date_range[0]} to {date_range[1]}")
    
    if not site_att:
        print("No site provided")
        return pd.DataFrame()
    
    engine = get_engine()
    
    try:
        with engine.connect() as conn:
            query = """
                SELECT date, region, province, municipality, site_att, vendors, group_level,
                       umts_cqi, umts_acc_cs, umts_acc_ps, umts_ret_cs, umts_ret_ps, 
                       umts_thp_dl, umts_traff_voice, umts_traff_data
                FROM umts_cqi_metrics_daily
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
        print(f"Average UMTS CQI: {df['umts_cqi'].mean():.8f}")
        
        # Export to CSV if requested
        if csv_export:
            filename = f"umts_cqi_single_site_{site_att}_{date_range[0]}_to_{date_range[1]}.csv"
            
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


def get_umts_cqi_for_site_group(site_list, date_range, csv_export=False):
    """
    Calculate UMTS CQI for a specific group of sites using the proven methodology
    from umts_cqi_level_processor.py.
    
    Args:
        site_list (list): List of site_att values to include in the group
        date_range (tuple): Tuple of (min_date, max_date) in 'YYYY-MM-DD' format
        csv_export (bool): Whether to export results to CSV
        
    Returns:
        pandas.DataFrame: DataFrame with calculated UMTS CQI metrics for the site group
    """
    print(f"Calculating UMTS CQI for site group: {site_list}")
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
        
        # Apply UMTS calculations to get the CQI
        df_calculated = apply_umts_group_calculations(df_raw)
        
        # Add group metadata
        df_calculated['site_count'] = len(site_list)
        df_calculated['site_list'] = ', '.join(site_list)
        df_calculated['group_type'] = 'neighbor_group'
        
        print(f"Calculated UMTS CQI: {df_calculated['umts_cqi'].iloc[0]:.8f}")
        
        # Export to CSV if requested
        if csv_export:
            sites_filename = '_'.join(site_list[:3])  # Use first 3 sites for filename
            if len(site_list) > 3:
                sites_filename += f"_and_{len(site_list)-3}_more"
            
            filename = f"umts_cqi_group_{sites_filename}_{date_range[0]}_to_{date_range[1]}.csv"
            
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


if __name__ == "__main__":
    # Test with a small group of sites
    test_sites = ["MEXMET0396", "MEXMET0411", "MEXMET0403"]
    test_min_date = "2024-09-01"
    test_max_date = "2024-09-15"
    
    print("Testing umts_cqi_group_processor...")
    
    # Test 1: Aggregated UMTS CQI for the entire period
    print("\n1. Testing aggregated UMTS CQI calculation...")
    try:
        df_result = get_umts_cqi_for_site_group(
            site_list=test_sites,
            date_range=(test_min_date, test_max_date),
            csv_export=True
        )
        
        if not df_result.empty:
            result = df_result['umts_cqi'].iloc[0]
            print(f"Aggregated test completed successfully. UMTS CQI: {result:.8f}")
        else:
            print("Aggregated test failed - no result returned")
            
    except Exception as e:
        print(f"Error during aggregated test: {e}")
        import traceback
        traceback.print_exc()
    
    # Test 2: Daily UMTS CQI values for plotting
    print("\n2. Testing daily UMTS CQI calculation...")
    try:
        df_daily = get_umts_cqi_daily_for_site_group(
            site_list=test_sites,
            date_range=(test_min_date, test_max_date),
            csv_export=True
        )
        
        if not df_daily.empty:
            avg_cqi = df_daily['umts_cqi'].mean()
            print(f"Daily test completed successfully. {len(df_daily)} days processed.")
            print(f"Average daily UMTS CQI: {avg_cqi:.8f}")
            print(f"Date range: {df_daily['date'].min()} to {df_daily['date'].max()}")
        else:
            print("Daily test failed - no result returned")
            
    except Exception as e:
        print(f"Error during daily test: {e}")
        import traceback
        traceback.print_exc()
    
    # Test 3: Single site UMTS CQI retrieval
    print("\n3. Testing single site UMTS CQI retrieval...")
    try:
        test_single_site = test_sites[0]  # Use first site from the test group
        df_single = get_umts_cqi_for_single_site(
            site_att=test_single_site,
            date_range=(test_min_date, test_max_date),
            csv_export=True
        )
        
        if not df_single.empty:
            avg_cqi = df_single['umts_cqi'].mean()
            print(f"Single site test completed successfully. {len(df_single)} days processed.")
            print(f"Site: {test_single_site}")
            print(f"Average UMTS CQI: {avg_cqi:.8f}")
            print(f"Date range: {df_single['date'].min()} to {df_single['date'].max()}")
        else:
            print("Single site test failed - no result returned")
            
    except Exception as e:
        print(f"Error during single site test: {e}")
        import traceback
        traceback.print_exc()