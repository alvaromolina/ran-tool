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


def populate_umts_cqi_metrics_daily(min_date, max_date):
    """
    Populate the umts_cqi_metrics_daily table with hierarchical aggregation levels.
    Uses Python for calculations and SQL for data aggregation.
    
    Args:
        min_date (str): Start date in format 'YYYY-MM-DD'
        max_date (str): End date in format 'YYYY-MM-DD'
    """
    print(f"Processing UMTS CQI metrics for date range: {min_date} to {max_date}")
    
    engine = get_engine()
    
    # Define hierarchical levels and their field mappings
    levels = [
        {
            'name': 'region', 
            'level_fields': ['region'],
            'group_by': ['date', 'region'],
            'select_fields': ['date', 'region'],
            'null_fields': ['province', 'municipality', 'site_att']
        },
        {
            'name': 'province',
            'level_fields': ['region', 'province'],
            'group_by': ['date', 'region', 'province'],
            'select_fields': ['date', 'region', 'province'],
            'null_fields': ['municipality', 'site_att']
        },
        {
            'name': 'municipality',
            'level_fields': ['region', 'province', 'municipality'],
            'group_by': ['date', 'region', 'province', 'municipality'],
            'select_fields': ['date', 'region', 'province', 'municipality'],
            'null_fields': ['site_att']
        },
        {
            'name': 'site',
            'level_fields': ['region', 'province', 'municipality', 'site_att'],
            'group_by': ['date', 'region', 'province', 'municipality', 'site_att'],
            'select_fields': ['date', 'region', 'province', 'municipality', 'site_att'],
            'null_fields': []
        },
        {
            'name': 'network',
            'level_fields': [],
            'group_by': ['date'],
            'select_fields': ['date'],
            'null_fields': ['region', 'province', 'municipality', 'site_att']
        }
    ]
    
    try:
        with engine.begin() as conn:
            print(f"Deleting existing records for date range...")
            delete_query = """
                DELETE FROM umts_cqi_metrics_daily 
                WHERE date >= :min_date AND date <= :max_date
            """
            conn.execute(text(delete_query), {"min_date": min_date, "max_date": max_date})
            
            for i, level_config in enumerate(levels, 1):
                level_name = level_config['name']
                print(f"Processing Level {i}/5: {level_name}")
                
                df = get_aggregated_data_for_level(level_config, min_date, max_date, engine)
                
                if df.empty:
                    print(f"No data found for {level_name} level")
                    continue
                
                print(f"Retrieved {len(df)} rows for {level_name} level")
                
                df_calculated = apply_umts_calculations(df, level_config)
                
                insert_data_in_batches(df_calculated, conn, batch_size=1000)
                
                print(f"✓ Successfully populated {len(df_calculated)} records for {level_name} level")
            
            print(f"Successfully populated umts_cqi_metrics_daily table for date range {min_date} to {max_date}")
            
    except SQLAlchemyError as e:
        print(f"SQLAlchemy Error populating database: {e}")
        import traceback
        print(f"Full traceback:")
        traceback.print_exc()
        raise
    except Exception as e:
        print(f"General Error populating database: {e}")
        import traceback
        print(f"Full traceback:")
        traceback.print_exc()
        raise


def get_aggregated_data_for_level(level_config, min_date, max_date, engine):
    """
    Get aggregated raw data for a specific level using simple SQL grouping.
    
    Args:
        level_config (dict): Level configuration
        min_date (str): Start date
        max_date (str): End date
        engine: SQLAlchemy engine
        
    Returns:
        pandas.DataFrame: Aggregated raw data
    """
    group_by_clause = ", ".join(level_config['group_by'])
    select_fields = ", ".join(level_config['select_fields'])
    
    query = f"""
        SELECT {select_fields},
        
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
        GROUP BY {group_by_clause}
        ORDER BY {group_by_clause}
    """
    
    return pd.read_sql_query(query, engine, params={"min_date": min_date, "max_date": max_date})


def apply_umts_calculations(df, level_config):
    """
    Apply UMTS calculations using vectorized numpy operations with pd.concat() to avoid DataFrame fragmentation.
    
    Args:
        df (pandas.DataFrame): Aggregated raw data
        level_config (dict): Level configuration
        
    Returns:
        pandas.DataFrame: Data with calculated metrics ready for insertion
    """
    # Create a copy to avoid modifying the original DataFrame
    df = df.copy()
    
    # Set NULL values for fields not at this level
    for field in level_config['null_fields']:
        df[field] = None
    
    # Add level and vendors fields
    df['group_level'] = level_config['name']
    df['vendors'] = 'All'
    
    # Vectorized helper functions
    def safe_divide(num, denom):
        return np.where(denom != 0, num / denom, 0)
    
    def zn(value):
        return np.where(pd.isna(value) | (value == None), 0, value)
    
    # Apply zn to all relevant columns at once
    vendor_cols = [col for col in df.columns if any(vendor in col for vendor in ['h3g_', 'e3g_', 'n3g_'])]
    for col in vendor_cols:
        df[col] = zn(df[col])
    
    # Calculate intermediate totals - vectorized
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
    
    # Calculate all new columns using dictionary approach to avoid fragmentation
    new_columns = {}
    
    # Total UMTS metrics
    new_columns['umts_acc_cs'] = np.round(
        safe_divide(rrc_success_cs_total, rrc_attempts_cs_total) *
        safe_divide(nas_success_cs_total, nas_attempts_cs_total) *
        safe_divide(rab_success_cs_total, rab_attempts_cs_total) * 100, 8)
    
    new_columns['umts_acc_ps'] = np.round(
        safe_divide(rrc_success_ps_total, rrc_attempts_ps_total) *
        safe_divide(nas_success_ps_total, nas_attempts_ps_total) *
        safe_divide(rab_success_ps_total, rab_attempts_ps_total) * 100, 8)
    
    new_columns['umts_ret_cs'] = np.round((1 - safe_divide(drop_num_cs_total, drop_denom_cs_total)) * 100, 8)
    new_columns['umts_ret_ps'] = np.round((1 - safe_divide(ps_ret_num_total, ps_ret_denom_total)) * 100, 8)
    new_columns['umts_thp_dl'] = np.round(safe_divide(thp_num_total, thp_denom_total), 2)
    new_columns['umts_traff_voice'] = np.round(traffic_voice_total, 4)
    new_columns['umts_traff_data'] = np.round(traffic_data_total, 4)
    
    new_columns['umts_cqi'] = np.round(
        (0.25 * np.exp((1 - new_columns['umts_acc_cs'] / 100) * -58.11779571) +
         0.25 * np.exp((1 - new_columns['umts_ret_cs'] / 100) * -58.11779571) +
         0.15 * np.exp((1 - new_columns['umts_acc_ps'] / 100) * -28.62016873) +
         0.15 * np.exp((1 - new_columns['umts_ret_ps'] / 100) * -28.62016873) +
         0.20 * (1 - np.exp(new_columns['umts_thp_dl'] * -0.00094856))) * 100, 8)
    
    # Huawei-specific metrics
    new_columns['umts_acc_cs_h'] = np.round(
        safe_divide(df['h3g_rrc_success_cs'], df['h3g_rrc_attempts_cs']) *
        safe_divide(df['h3g_nas_success_cs'], df['h3g_nas_attempts_cs']) *
        safe_divide(df['h3g_rab_success_cs'], df['h3g_rab_attempts_cs']) * 100, 8)
    
    new_columns['umts_acc_ps_h'] = np.round(
        safe_divide(df['h3g_rrc_success_ps'], df['h3g_rrc_attempts_ps']) *
        safe_divide(df['h3g_nas_success_ps'], df['h3g_nas_attempts_ps']) *
        safe_divide(df['h3g_rab_success_ps'], df['h3g_rab_attempts_ps']) * 100, 8)
    
    new_columns['umts_ret_cs_h'] = np.round(
        (1 - safe_divide(df['h3g_drop_num_cs'], df['h3g_drop_denom_cs'])) * 100, 8)
    
    new_columns['umts_ret_ps_h'] = np.round(
        (1 - safe_divide(df['h3g_ps_retainability_num'], df['h3g_ps_retainability_denom'])) * 100, 8)
    
    new_columns['umts_thp_dl_h'] = np.round(
        safe_divide(df['h3g_thpt_user_dl_kbps_num'], df['h3g_thpt_user_dl_kbps_denom']), 2)
    
    new_columns['umts_traff_voice_h'] = np.round(df['h3g_traffic_v_user_cs'], 4)
    new_columns['umts_traff_data_h'] = np.round(df['h3g_traffic_d_user_ps_gb'], 4)
    
    new_columns['umts_cqi_h'] = np.round(
        (0.25 * np.exp((1 - new_columns['umts_acc_cs_h'] / 100) * -58.11779571) +
         0.25 * np.exp((1 - new_columns['umts_ret_cs_h'] / 100) * -58.11779571) +
         0.15 * np.exp((1 - new_columns['umts_acc_ps_h'] / 100) * -28.62016873) +
         0.15 * np.exp((1 - new_columns['umts_ret_ps_h'] / 100) * -28.62016873) +
         0.20 * (1 - np.exp(new_columns['umts_thp_dl_h'] * -0.00094856))) * 100, 8)
    
    # Ericsson-specific metrics
    new_columns['umts_acc_cs_e'] = np.round(
        safe_divide(df['e3g_rrc_success_cs'], df['e3g_rrc_attempts_cs']) *
        safe_divide(df['e3g_nas_success_cs'], df['e3g_nas_attempts_cs']) *
        safe_divide(df['e3g_rab_success_cs'], df['e3g_rab_attempts_cs']) * 100, 8)
    
    new_columns['umts_acc_ps_e'] = np.round(
        safe_divide(df['e3g_rrc_success_ps'], df['e3g_rrc_attempts_ps']) *
        safe_divide(df['e3g_nas_success_ps'], df['e3g_nas_attempts_ps']) *
        safe_divide(df['e3g_rab_success_ps'], df['e3g_rab_attempts_ps']) * 100, 8)
    
    new_columns['umts_ret_cs_e'] = np.round(
        (1 - safe_divide(df['e3g_drop_num_cs'], df['e3g_drop_denom_cs'])) * 100, 8)
    
    new_columns['umts_ret_ps_e'] = np.round(
        (1 - safe_divide(df['e3g_ps_retainability_num'], df['e3g_ps_retainability_denom'])) * 100, 8)
    
    new_columns['umts_thp_dl_e'] = np.round(
        safe_divide(df['e3g_thpt_user_dl_kbps_num'], df['e3g_thpt_user_dl_kbps_denom']), 2)
    
    new_columns['umts_traff_voice_e'] = np.round(df['e3g_traffic_v_user_cs'], 4)
    new_columns['umts_traff_data_e'] = np.round(df['e3g_traffic_d_user_ps_gb'], 4)
    
    new_columns['umts_cqi_e'] = np.round(
        (0.25 * np.exp((1 - new_columns['umts_acc_cs_e'] / 100) * -58.11779571) +
         0.25 * np.exp((1 - new_columns['umts_ret_cs_e'] / 100) * -58.11779571) +
         0.15 * np.exp((1 - new_columns['umts_acc_ps_e'] / 100) * -28.62016873) +
         0.15 * np.exp((1 - new_columns['umts_ret_ps_e'] / 100) * -28.62016873) +
         0.20 * (1 - np.exp(new_columns['umts_thp_dl_e'] * -0.00094856))) * 100, 8)
    
    # Nokia-specific metrics
    new_columns['umts_acc_cs_n'] = np.round(
        safe_divide(df['n3g_rrc_success_cs'], df['n3g_rrc_attempts_cs']) *
        safe_divide(df['n3g_nas_success_cs'], df['n3g_nas_attempts_cs']) *
        safe_divide(df['n3g_rab_success_cs'], df['n3g_rab_attempts_cs']) * 100, 8)
    
    new_columns['umts_acc_ps_n'] = np.round(
        safe_divide(df['n3g_rrc_success_ps'], df['n3g_rrc_attempts_ps']) *
        safe_divide(df['n3g_nas_success_ps'], df['n3g_nas_attempts_ps']) *
        safe_divide(df['n3g_rab_success_ps'], df['n3g_rab_attempts_ps']) * 100, 8)
    
    new_columns['umts_ret_cs_n'] = np.round(
        (1 - safe_divide(df['n3g_drop_num_cs'], df['n3g_drop_denom_cs'])) * 100, 8)
    
    new_columns['umts_ret_ps_n'] = np.round(
        (1 - safe_divide(df['n3g_ps_retainability_num'], df['n3g_ps_retainability_denom'])) * 100, 8)
    
    new_columns['umts_thp_dl_n'] = np.round(
        safe_divide(df['n3g_thpt_user_dl_kbps_num'], df['n3g_thpt_user_dl_kbps_denom']), 2)
    
    new_columns['umts_traff_voice_n'] = np.round(df['n3g_traffic_v_user_cs'], 4)
    new_columns['umts_traff_data_n'] = np.round(df['n3g_traffic_d_user_ps_gb'], 4)
    
    new_columns['umts_cqi_n'] = np.round(
        (0.25 * np.exp((1 - new_columns['umts_acc_cs_n'] / 100) * -58.11779571) +
         0.25 * np.exp((1 - new_columns['umts_ret_cs_n'] / 100) * -58.11779571) +
         0.15 * np.exp((1 - new_columns['umts_acc_ps_n'] / 100) * -28.62016873) +
         0.15 * np.exp((1 - new_columns['umts_ret_ps_n'] / 100) * -28.62016873) +
         0.20 * (1 - np.exp(new_columns['umts_thp_dl_n'] * -0.00094856))) * 100, 8)
    
    # Create new DataFrame with calculated columns using pd.concat() to avoid fragmentation
    new_df = pd.DataFrame(new_columns, index=df.index)
    df_with_calculations = pd.concat([df, new_df], axis=1)
    
    # Select only the columns needed for the target table
    target_columns = ['date', 'region', 'province', 'municipality', 'site_att', 'vendors', 'group_level',
                     'umts_cqi', 'umts_acc_cs', 'umts_acc_ps', 'umts_ret_cs', 'umts_ret_ps', 
                     'umts_thp_dl', 'umts_traff_voice', 'umts_traff_data',
                     'umts_cqi_h', 'umts_acc_cs_h', 'umts_acc_ps_h', 'umts_ret_cs_h', 'umts_ret_ps_h',
                     'umts_thp_dl_h', 'umts_traff_voice_h', 'umts_traff_data_h',
                     'umts_cqi_e', 'umts_acc_cs_e', 'umts_acc_ps_e', 'umts_ret_cs_e', 'umts_ret_ps_e',
                     'umts_thp_dl_e', 'umts_traff_voice_e', 'umts_traff_data_e',
                     'umts_cqi_n', 'umts_acc_cs_n', 'umts_acc_ps_n', 'umts_ret_cs_n', 'umts_ret_ps_n',
                     'umts_thp_dl_n', 'umts_traff_voice_n', 'umts_traff_data_n']
    
    return df_with_calculations[target_columns]


def insert_data_in_batches(df, conn, batch_size=1000):
    """
    Insert DataFrame data into umts_cqi_metrics_daily table in batches.
    
    Args:
        df (pandas.DataFrame): Data to insert
        conn: Database connection
        batch_size (int): Number of rows per batch
    """
    total_rows = len(df)
    total_batches = (total_rows + batch_size - 1) // batch_size
    
    print(f"Starting batch insertion: {total_rows:,} rows in {total_batches} batches")
    
    # Create column names and placeholders once
    columns = list(df.columns)
    placeholders = ', '.join([f':{col}' for col in columns])
    columns_str = ', '.join(columns)
    
    insert_query = f"""
        INSERT INTO umts_cqi_metrics_daily ({columns_str})
        VALUES ({placeholders})
    """
    
    for i in range(0, total_rows, batch_size):
        batch_df = df.iloc[i:i+batch_size]
        batch_num = i//batch_size + 1
        batch_end = min(i+batch_size, total_rows)
        batch_size_actual = len(batch_df)
        
        try:
            # Process each row in the current batch silently
            for _, row in batch_df.iterrows():
                # Convert row to dictionary, handling None values
                row_dict = {}
                for col in columns:
                    value = row[col]
                    # Convert pandas NaN/None to actual None for SQL
                    if pd.isna(value) or value is None:
                        row_dict[col] = None
                    else:
                        row_dict[col] = value
                
                # Execute the insert for this row
                conn.execute(text(insert_query), row_dict)
            
            # Show progress only after batch completion
            progress_pct = (batch_end / total_rows) * 100
            print(f"✓ Batch {batch_num}/{total_batches}: {batch_size_actual:,} rows inserted. Progress: {progress_pct:.1f}%")
            
        except Exception as batch_error:
            print(f"✗ ERROR in batch {batch_num}/{total_batches}: {batch_error}")
            import traceback
            traceback.print_exc()
            raise
    
    print(f" Completed! {total_rows:,} rows inserted successfully.")


def get_last_date_umts_cqi_metrics_daily():
    """
    Get the last date from umts_cqi_metrics_daily table.
    This value + 1 will be used as the init_date for processing.
    
    Returns:
        str: Last date in 'YYYY-MM-DD' format, or None if table is empty
    """
    engine = get_engine()
    
    try:
        with engine.connect() as conn:
            query = "SELECT MAX(date) FROM umts_cqi_metrics_daily"
            result = conn.execute(text(query))
            last_date = result.fetchone()[0]
            
            if last_date:
                return last_date.strftime('%Y-%m-%d')
            else:
                return None
                
    except SQLAlchemyError as e:
        print(f"Error getting last date from umts_cqi_metrics_daily: {e}")
        return None


def get_last_date_umts_cqi_daily():
    """
    Get the last date from umts_cqi_daily table.
    This value will be used as the max_date for processing.
    
    Returns:
        str: Last date in 'YYYY-MM-DD' format, or None if table is empty
    """
    engine = get_engine()
    
    try:
        with engine.connect() as conn:
            query = "SELECT MAX(date) FROM umts_cqi_daily"
            result = conn.execute(text(query))
            last_date = result.fetchone()[0]
            
            if last_date:
                return last_date.strftime('%Y-%m-%d')
            else:
                return None
                
    except SQLAlchemyError as e:
        print(f"Error getting last date from umts_cqi_daily: {e}")
        return None


def umts_cqi_level_export(init_date, max_date, group_level, level_list=None, csv_export=False):
    """
    Export UMTS CQI metrics data from umts_cqi_metrics_daily table to DataFrame and optionally to CSV.
    
    Args:
        init_date (str): Start date in format 'YYYY-MM-DD'
        max_date (str): End date in format 'YYYY-MM-DD'
        group_level (str): Level of aggregation ('network', 'region', 'province', 'municipality', 'site')
        level_list (list, optional): List of specific values to filter by for the group_level
        csv_export (bool): Whether to export the data to CSV file
        
    Returns:
        pandas.DataFrame: DataFrame containing the filtered UMTS CQI metrics data
    """
    print(f"umts_cqi_level_export called")
    print(f"Parameters:")
    print(f"  init_date: {init_date}")
    print(f"  max_date: {max_date}")
    print(f"  group_level: {group_level}")
    print(f"  level_list: {level_list}")
    print(f"  csv_export: {csv_export}")
    
    # Validate group_level parameter
    valid_levels = ['network', 'region', 'province', 'municipality', 'site']
    if group_level not in valid_levels:
        raise ValueError(f"group_level must be one of {valid_levels}, got: {group_level}")
    
    engine = get_engine()
    
    try:
        with engine.connect() as conn:
            # Build the base query
            base_query = """
                SELECT date, region, province, municipality, site_att, vendors, group_level,
                       umts_cqi, umts_acc_cs, umts_acc_ps, umts_ret_cs, umts_ret_ps,
                       umts_thp_dl, umts_traff_voice, umts_traff_data,
                       umts_cqi_h, umts_acc_cs_h, umts_acc_ps_h, umts_ret_cs_h, umts_ret_ps_h,
                       umts_thp_dl_h, umts_traff_voice_h, umts_traff_data_h,
                       umts_cqi_e, umts_acc_cs_e, umts_acc_ps_e, umts_ret_cs_e, umts_ret_ps_e,
                       umts_thp_dl_e, umts_traff_voice_e, umts_traff_data_e,
                       umts_cqi_n, umts_acc_cs_n, umts_acc_ps_n, umts_ret_cs_n, umts_ret_ps_n,
                       umts_thp_dl_n, umts_traff_voice_n, umts_traff_data_n
                FROM umts_cqi_metrics_daily
                WHERE date >= %(init_date)s AND date <= %(max_date)s
            """
            
            # Add group_level filter and specific level filters
            params = {"init_date": init_date, "max_date": max_date}
            
            if group_level == 'network':
                # For network level, we might want to aggregate all data or get a specific network view
                # Assuming we want all data for network level analysis
                query = base_query + " ORDER BY date, region, province, municipality, site_att"
                
            elif group_level == 'region':
                query = base_query + " AND group_level = 'region'"
                if level_list:
                    placeholders = ', '.join([f"%(region_{i})s" for i in range(len(level_list))])
                    query += f" AND region IN ({placeholders})"
                    for i, region in enumerate(level_list):
                        params[f"region_{i}"] = region
                query += " ORDER BY date, region"
                
            elif group_level == 'province':
                query = base_query + " AND group_level = 'province'"
                if level_list:
                    placeholders = ', '.join([f"%(province_{i})s" for i in range(len(level_list))])
                    query += f" AND province IN ({placeholders})"
                    for i, province in enumerate(level_list):
                        params[f"province_{i}"] = province
                query += " ORDER BY date, region, province"
                
            elif group_level == 'municipality':
                query = base_query + " AND group_level = 'municipality'"
                if level_list:
                    placeholders = ', '.join([f"%(municipality_{i})s" for i in range(len(level_list))])
                    query += f" AND municipality IN ({placeholders})"
                    for i, municipality in enumerate(level_list):
                        params[f"municipality_{i}"] = municipality
                query += " ORDER BY date, region, province, municipality"
                
            elif group_level == 'site':
                query = base_query + " AND group_level = 'site'"
                if level_list:
                    placeholders = ', '.join([f"%(site_{i})s" for i in range(len(level_list))])
                    query += f" AND site_att IN ({placeholders})"
                    for i, site in enumerate(level_list):
                        params[f"site_{i}"] = site
                query += " ORDER BY date, region, province, municipality, site_att"
            
            print(f"Executing query for {group_level} level...")
            print(f"Query parameters: {params}")
            
            # Execute query and get DataFrame
            df = pd.read_sql_query(query, conn, params=params)
            
            print(f"Retrieved {len(df)} rows for {group_level} level")
            print(f"Date range: {df['date'].min()} to {df['date'].max()}" if not df.empty else "No data found")
            
            # Export to CSV if requested
            if csv_export and not df.empty:
                # Create filename based on parameters
                level_filter = ""
                if level_list:
                    level_filter = f"_{'_'.join(str(item).replace(' ', '_') for item in level_list[:3])}"
                    if len(level_list) > 3:
                        level_filter += f"_and_{len(level_list)-3}_more"
                
                filename = f"umts_cqi_export_{group_level}_{init_date}_to_{max_date}{level_filter}.csv"
                
                # Use ROOT_DIRECTORY if available, otherwise use output folder
                if ROOT_DIRECTORY:
                    output_path = os.path.join(ROOT_DIRECTORY, "output", filename)
                else:
                    output_path = os.path.join("output", filename)
                
                # Create output directory if it doesn't exist
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                
                # Export to CSV
                df.to_csv(output_path, index=False)
                print(f"Data exported to CSV: {output_path}")
            
            elif csv_export and df.empty:
                print("No data to export to CSV")
            
            return df
            
    except SQLAlchemyError as e:
        print(f"SQLAlchemy Error during export: {e}")
        import traceback
        print(f"Full traceback:")
        traceback.print_exc()
        raise
    except Exception as e:
        print(f"General Error during export: {e}")
        import traceback
        print(f"Full traceback:")
        traceback.print_exc()
        raise


if __name__ == "__main__":
    """
    Main execution block for testing or running the UMTS CQI level processor.
    """
    print("UMTS CQI Level Processor")
    
    try:
        # Get date ranges
        min_date = get_last_date_umts_cqi_metrics_daily()
        max_date = get_last_date_umts_cqi_daily()
        
        print(f"Last processed date in metrics_daily: {min_date}")
        print(f"Latest available date in daily: {max_date}")
        
        # If no data in metrics_daily, start from a reasonable date or use fixed test dates
        if min_date is None:
            min_date = "2024-01-01"  # Set a reasonable start date or use earliest date from daily table
            print(f"No previous data found, starting from: {min_date}")
        
        if max_date is None:
            print("No data available in nr_cqi_daily table")
        else:
            populate_umts_cqi_metrics_daily(min_date, max_date)
            print("Processing completed successfully!")


    except Exception as e:
        print(f"Error during execution: {e}")
        import traceback
        traceback.print_exc()

