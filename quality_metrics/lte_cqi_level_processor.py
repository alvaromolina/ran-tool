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
    
    #for var_name, var_value in required_vars.items():
    #    if var_value is None or var_value == 'None' or var_value == '':
    #        raise ValueError(f"Environment variable {var_name} is not set or is None")
    
    try:
        int(POSTGRES_PORT)
    except (ValueError, TypeError) as e:
        raise ValueError(f"POSTGRES_PORT must be a valid integer, got: {POSTGRES_PORT}")
    
    connection_string = f"postgresql+psycopg2://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    return create_engine(connection_string)


def populate_lte_cqi_metrics_daily(min_date, max_date):
    """
    Populate the lte_cqi_metrics_daily table with hierarchical aggregation levels.
    Process data in chunks for better memory management.
    """
    print(f"Processing LTE CQI metrics for date range: {min_date} to {max_date}")
    
    engine = get_engine()
    
    # Define hierarchical levels (same as before)
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
                DELETE FROM lte_cqi_metrics_daily 
                WHERE date >= :min_date AND date <= :max_date
            """
            conn.execute(text(delete_query), {"min_date": min_date, "max_date": max_date})
            
            for i, level_config in enumerate(levels, 1):
                level_name = level_config['name']
                print(f"Processing Level {i}/5: {level_name}")
                
                # Step 1: Get ALL raw data for this level
                df_raw = get_aggregated_data_for_level(level_config, min_date, max_date, engine)
                
                if df_raw.empty:
                    print(f"No data found for {level_name} level")
                    continue
                
                print(f"Retrieved {len(df_raw)} rows for {level_name} level")
                
                # Step 2: Process in chunks of 1000
                process_data_in_chunks(df_raw, level_config, conn, chunk_size=1000)
                
                print(f"✓ Successfully populated {len(df_raw)} records for {level_name} level")
    
    except SQLAlchemyError as e:
        print(f"SQLAlchemy Error during population: {e}")
        import traceback
        print(f"Full traceback:")
        traceback.print_exc()
        raise
    except Exception as e:
        print(f"General Error during population: {e}")
        import traceback
        print(f"Full traceback:")
        traceback.print_exc()
        raise


def process_data_in_chunks(df_raw, level_config, conn, chunk_size=1000):
    """
    Process raw data in chunks: calculate metrics + insert immediately.
    
    Args:
        df_raw (pandas.DataFrame): All raw data for the level
        level_config (dict): Level configuration
        conn: Database connection
        chunk_size (int): Number of rows to process per chunk
    """
    total_rows = len(df_raw)
    total_chunks = (total_rows + chunk_size - 1) // chunk_size
    
    print(f"Processing in chunks: {total_rows:,} rows in {total_chunks} chunks of {chunk_size}")
    
    for i in range(0, total_rows, chunk_size):
        chunk_end = min(i + chunk_size, total_rows)
        chunk_num = i // chunk_size + 1
        
        # Get chunk of raw data
        df_chunk = df_raw.iloc[i:chunk_end].copy()
        
        print(f"Processing chunk {chunk_num}/{total_chunks} (rows {i+1}-{chunk_end})...")
        
        # Calculate metrics for this chunk (using vectorized version)
        df_calculated = apply_lte_calculations(df_chunk, level_config)
        
        # Insert this chunk immediately
        insert_chunk_data(df_calculated, conn)
        
        progress_pct = (chunk_end / total_rows) * 100
        print(f"✓ Chunk {chunk_num}/{total_chunks} completed. Progress: {progress_pct:.1f}%")    


def insert_chunk_data(df, conn):
    """
    Insert a single chunk of calculated data.
    
    Args:
        df (pandas.DataFrame): Calculated data chunk
        conn: Database connection
    """
    columns = list(df.columns)
    placeholders = ', '.join([f':{col}' for col in columns])
    columns_str = ', '.join(columns)
    
    insert_query = f"""
        INSERT INTO lte_cqi_metrics_daily ({columns_str})
        VALUES ({placeholders})
    """
    
    for _, row in df.iterrows():
        row_dict = {}
        for col in columns:
            value = row[col]
            if pd.isna(value) or value is None:
                row_dict[col] = None
            else:
                row_dict[col] = value
        
        conn.execute(text(insert_query), row_dict)


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
        GROUP BY {group_by_clause}
        ORDER BY {group_by_clause}
    """
    
    return pd.read_sql_query(query, engine, params={"min_date": min_date, "max_date": max_date})


def apply_lte_calculations(df, level_config):
    """
    Apply LTE calculations using vectorized numpy operations.
    
    Args:
        df (pandas.DataFrame): Aggregated raw data
        level_config (dict): Level configuration
        
    Returns:
        pandas.DataFrame: Data with calculated metrics ready for insertion
    """
    for field in level_config['null_fields']:
        df[field] = None
    
    df['group_level'] = level_config['name']
    df['vendors'] = 'All'
    
    # Vectorized helper functions
    def safe_divide(num, denom):
        return np.where(denom != 0, num / denom, 0)
    
    def zn(value):
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
    
    # Total metrics
    new_columns['lte_acc'] = np.round(
        safe_divide(erab_success_total, erabs_attemps_total) *
        safe_divide(rrc_success_total, rrc_attemps_total) *
        safe_divide(s1_success_total, s1_attemps_total) * 100, 8)
    
    new_columns['lte_ret'] = np.round((1 - safe_divide(retainability_num_total, retainability_denom_total)) * 100, 8)
    
    new_columns['lte_irat'] = np.round(safe_divide(irat_events_total, erab_success_total) * 100, 8)
    
    new_columns['lte_thp_user_dl'] = np.round(safe_divide(thpt_num_total, thpt_denom_total), 8)
    
    new_columns['lte_4g_on_3g'] = np.round(safe_divide(time3g_total, time3g_total + time4g_total) * 100, 8)
    
    new_columns['lte_ookla_lat'] = np.round(safe_divide(sumavg_latency_total, summuestras_total), 8)
    
    new_columns['lte_ookla_thp'] = np.round(safe_divide(sumavg_dl_kbps_total, summuestras_total), 8)
    
    new_columns['lte_traff'] = np.round(traffic_total / 1024, 8)
    
    # Calculate total LTE CQI (all vendors) - vectorized
    new_columns['lte_cqi'] = np.round(
        (0.25 * np.exp((1 - new_columns['lte_acc'] / 100) * -63.91668575) +
         0.25 * np.exp((1 - new_columns['lte_ret'] / 100) * -63.91668575) +
         0.05 * np.exp((new_columns['lte_irat'] / 100) * -22.31435513) +
         0.30 * (1 - np.exp(new_columns['lte_thp_user_dl'] * -0.000282742)) +
         0.05 * np.minimum(1, np.exp((new_columns['lte_4g_on_3g'] / 100 - 10 / 100) * -11.15717757)) +
         0.05 * np.exp((new_columns['lte_ookla_lat'] - 20) * -0.00526802578289131) +
         0.05 * (1 - np.exp(new_columns['lte_ookla_thp'] * -0.00005364793041447))) * 100, 8)
    
    # Huawei-specific metrics
    new_columns['lte_acc_h'] = np.round(
        safe_divide(df['h4g_erab_success'], df['h4g_erabs_attemps']) *
        safe_divide(df['h4g_rrc_success_all'], df['h4g_rrc_attemps_all']) *
        safe_divide(df['h4g_s1_success'], df['h4g_s1_attemps']) * 100, 8)
    
    new_columns['lte_ret_h'] = np.round(
        (1 - safe_divide(df['h4g_retainability_num'], df['h4g_retainability_denom'])) * 100, 8)
    
    new_columns['lte_irat_h'] = np.round(
        safe_divide(df['h4g_irat_4g_to_3g_events'], df['h4g_erab_success']) * 100, 8)
    
    new_columns['lte_thp_user_dl_h'] = np.round(
        safe_divide(df['h4g_thpt_user_dl_kbps_num'], df['h4g_thpt_user_dl_kbps_denom']), 8)
    
    new_columns['lte_4g_on_3g_h'] = np.round(
        safe_divide(df['h4g_time3g'], df['h4g_time3g'] + df['h4g_time4g']) * 100, 8)
    
    new_columns['lte_ookla_lat_h'] = np.round(
        safe_divide(df['h4g_sumavg_latency'], df['h4g_summuestras']), 8)
    
    new_columns['lte_ookla_thp_h'] = np.round(
        safe_divide(df['h4g_sumavg_dl_kbps'], df['h4g_summuestras']), 8)
    
    new_columns['lte_traff_h'] = np.round(df['h4g_traffic_d_user_ps_gb'] / 1024, 8)
    
    new_columns['lte_cqi_h'] = np.round(
        (0.25 * np.exp((1 - new_columns['lte_acc_h'] / 100) * -63.91668575) +
         0.25 * np.exp((1 - new_columns['lte_ret_h'] / 100) * -63.91668575) +
         0.05 * np.exp((new_columns['lte_irat_h'] / 100) * -22.31435513) +
         0.30 * (1 - np.exp(new_columns['lte_thp_user_dl_h'] * -0.000282742)) +
         0.05 * np.minimum(1, np.exp((new_columns['lte_4g_on_3g_h'] / 100 - 10 / 100) * -11.15717757)) +
         0.05 * np.exp((new_columns['lte_ookla_lat_h'] - 20) * -0.00526802578289131) +
         0.05 * (1 - np.exp(new_columns['lte_ookla_thp_h'] * -0.00005364793041447))) * 100, 8)
    
    # Ericsson-specific metrics
    new_columns['lte_acc_e'] = np.round(
        safe_divide(df['e4g_erab_success'], df['e4g_erabs_attemps']) *
        safe_divide(df['e4g_rrc_success_all'], df['e4g_rrc_attemps_all']) *
        safe_divide(df['e4g_s1_success'], df['e4g_s1_attemps']) * 100, 8)
    
    new_columns['lte_ret_e'] = np.round(
        (1 - safe_divide(df['e4g_retainability_num'], df['e4g_retainability_denom'])) * 100, 8)
    
    new_columns['lte_irat_e'] = np.round(
        safe_divide(df['e4g_irat_4g_to_3g_events'], df['e4g_erab_success']) * 100, 8)
    
    new_columns['lte_thp_user_dl_e'] = np.round(
        safe_divide(df['e4g_thpt_user_dl_kbps_num'], df['e4g_thpt_user_dl_kbps_denom']), 8)
    
    new_columns['lte_4g_on_3g_e'] = np.round(
        safe_divide(df['e4g_time3g'], df['e4g_time3g'] + df['e4g_time4g']) * 100, 8)
    
    new_columns['lte_ookla_lat_e'] = np.round(
        safe_divide(df['e4g_sumavg_latency'], df['e4g_summuestras']), 8)
    
    new_columns['lte_ookla_thp_e'] = np.round(
        safe_divide(df['e4g_sumavg_dl_kbps'], df['e4g_summuestras']), 8)
    
    new_columns['lte_traff_e'] = np.round(df['e4g_traffic_d_user_ps_gb'] / 1024, 8)
    
    new_columns['lte_cqi_e'] = np.round(
        (0.25 * np.exp((1 - new_columns['lte_acc_e'] / 100) * -63.91668575) +
         0.25 * np.exp((1 - new_columns['lte_ret_e'] / 100) * -63.91668575) +
         0.05 * np.exp((new_columns['lte_irat_e'] / 100) * -22.31435513) +
         0.30 * (1 - np.exp(new_columns['lte_thp_user_dl_e'] * -0.000282742)) +
         0.05 * np.minimum(1, np.exp((new_columns['lte_4g_on_3g_e'] / 100 - 10 / 100) * -11.15717757)) +
         0.05 * np.exp((new_columns['lte_ookla_lat_e'] - 20) * -0.00526802578289131) +
         0.05 * (1 - np.exp(new_columns['lte_ookla_thp_e'] * -0.00005364793041447))) * 100, 8)
    
    # Nokia-specific metrics
    new_columns['lte_acc_n'] = np.round(
        safe_divide(df['n4g_erab_success'], df['n4g_erabs_attemps']) *
        safe_divide(df['n4g_rrc_success_all'], df['n4g_rrc_attemps_all']) *
        safe_divide(df['n4g_s1_success'], df['n4g_s1_attemps']) * 100, 8)
    
    new_columns['lte_ret_n'] = np.round(
        (1 - safe_divide(df['n4g_retainability_num'], df['n4g_retainability_denom'])) * 100, 8)
    
    new_columns['lte_irat_n'] = np.round(
        safe_divide(df['n4g_irat_4g_to_3g_events'], df['n4g_erab_success']) * 100, 8)
    
    new_columns['lte_thp_user_dl_n'] = np.round(
        safe_divide(df['n4g_thpt_user_dl_kbps_num'], df['n4g_thpt_user_dl_kbps_denom']), 8)
    
    new_columns['lte_4g_on_3g_n'] = np.round(
        safe_divide(df['n4g_time3g'], df['n4g_time3g'] + df['n4g_time4g']) * 100, 8)
    
    new_columns['lte_ookla_lat_n'] = np.round(
        safe_divide(df['n4g_sumavg_latency'], df['n4g_summuestras']), 8)
    
    new_columns['lte_ookla_thp_n'] = np.round(
        safe_divide(df['n4g_sumavg_dl_kbps'], df['n4g_summuestras']), 8)
    
    new_columns['lte_traff_n'] = np.round(df['n4g_traffic_d_user_ps_gb'] / 1024, 8)
    
    new_columns['lte_cqi_n'] = np.round(
        (0.25 * np.exp((1 - new_columns['lte_acc_n'] / 100) * -63.91668575) +
         0.25 * np.exp((1 - new_columns['lte_ret_n'] / 100) * -63.91668575) +
         0.05 * np.exp((new_columns['lte_irat_n'] / 100) * -22.31435513) +
         0.30 * (1 - np.exp(new_columns['lte_thp_user_dl_n'] * -0.000282742)) +
         0.05 * np.minimum(1, np.exp((new_columns['lte_4g_on_3g_n'] / 100 - 10 / 100) * -11.15717757)) +
         0.05 * np.exp((new_columns['lte_ookla_lat_n'] - 20) * -0.00526802578289131) +
         0.05 * (1 - np.exp(new_columns['lte_ookla_thp_n'] * -0.00005364793041447))) * 100, 8)
    
    # Samsung-specific metrics
    new_columns['lte_acc_s'] = np.round(
        safe_divide(df['s4g_erab_success'], df['s4g_erabs_attemps']) *
        safe_divide(df['s4g_rrc_success_all'], df['s4g_rrc_attemps_all']) *
        safe_divide(df['s4g_s1_success'], df['s4g_s1_attemps']) * 100, 8)
    
    new_columns['lte_ret_s'] = np.round(
        (1 - safe_divide(df['s4g_retainability_num'], df['s4g_retainability_denom'])) * 100, 8)
    
    new_columns['lte_irat_s'] = np.round(
        safe_divide(df['s4g_irat_4g_to_3g_events'], df['s4g_erab_success']) * 100, 8)
    
    new_columns['lte_thp_user_dl_s'] = np.round(
        safe_divide(df['s4g_thpt_user_dl_kbps_num'], df['s4g_thpt_user_dl_kbps_denom']), 8)
    
    new_columns['lte_4g_on_3g_s'] = np.round(
        safe_divide(df['s4g_time3g'], df['s4g_time3g'] + df['s4g_time4g']) * 100, 8)
    
    new_columns['lte_ookla_lat_s'] = np.round(
        safe_divide(df['s4g_sumavg_latency'], df['s4g_summuestras']), 8)
    
    new_columns['lte_ookla_thp_s'] = np.round(
        safe_divide(df['s4g_sumavg_dl_kbps'], df['s4g_summuestras']), 8)
    
    new_columns['lte_traff_s'] = np.round(df['s4g_traffic_d_user_ps_gb'] / 1024, 8)
    
    new_columns['lte_cqi_s'] = np.round(
        (0.25 * np.exp((1 - new_columns['lte_acc_s'] / 100) * -63.91668575) +
         0.25 * np.exp((1 - new_columns['lte_ret_s'] / 100) * -63.91668575) +
         0.05 * np.exp((new_columns['lte_irat_s'] / 100) * -22.31435513) +
         0.30 * (1 - np.exp(new_columns['lte_thp_user_dl_s'] * -0.000282742)) +
         0.05 * np.minimum(1, np.exp((new_columns['lte_4g_on_3g_s'] / 100 - 10 / 100) * -11.15717757)) +
         0.05 * np.exp((new_columns['lte_ookla_lat_s'] - 20) * -0.00526802578289131) +
         0.05 * (1 - np.exp(new_columns['lte_ookla_thp_s'] * -0.00005364793041447))) * 100, 8)
    
    # CREATE DATAFRAME FROM DICTIONARY AND CONCATENATE - THIS AVOIDS FRAGMENTATION
    new_columns_df = pd.DataFrame(new_columns)
    result_df = pd.concat([df, new_columns_df], axis=1)
    
    target_columns = ['date', 'region', 'province', 'municipality', 'site_att', 'vendors', 'group_level',
                     'lte_cqi', 'lte_acc', 'lte_ret', 'lte_irat', 'lte_thp_user_dl', 'lte_4g_on_3g', 
                     'lte_ookla_lat', 'lte_ookla_thp', 'lte_traff',
                     'lte_cqi_h', 'lte_acc_h', 'lte_ret_h', 'lte_irat_h', 'lte_thp_user_dl_h', 
                     'lte_4g_on_3g_h', 'lte_ookla_lat_h', 'lte_ookla_thp_h', 'lte_traff_h',
                     'lte_cqi_e', 'lte_acc_e', 'lte_ret_e', 'lte_irat_e', 'lte_thp_user_dl_e', 
                     'lte_4g_on_3g_e', 'lte_ookla_lat_e', 'lte_ookla_thp_e', 'lte_traff_e',
                     'lte_cqi_n', 'lte_acc_n', 'lte_ret_n', 'lte_irat_n', 'lte_thp_user_dl_n', 
                     'lte_4g_on_3g_n', 'lte_ookla_lat_n', 'lte_ookla_thp_n', 'lte_traff_n',
                     'lte_cqi_s', 'lte_acc_s', 'lte_ret_s', 'lte_irat_s', 'lte_thp_user_dl_s', 
                     'lte_4g_on_3g_s', 'lte_ookla_lat_s', 'lte_ookla_thp_s', 'lte_traff_s']
    
    return result_df[target_columns]


def insert_data_in_batches(df, conn, batch_size=1000):
    """
    Insert DataFrame data into lte_cqi_metrics_daily table in batches.
    
    Args:
        df (pandas.DataFrame): Data to insert
        conn: Database connection
        batch_size (int): Number of rows per batch
    """
    total_rows = len(df)
    total_batches = (total_rows + batch_size - 1) // batch_size
    
    print(f"Starting batch insertion: {total_rows:,} rows in {total_batches} batches")
    
    columns = list(df.columns)
    placeholders = ', '.join([f':{col}' for col in columns])
    columns_str = ', '.join(columns)
    
    insert_query = f"""
        INSERT INTO lte_cqi_metrics_daily ({columns_str})
        VALUES ({placeholders})
    """
    
    for i in range(0, total_rows, batch_size):
        batch_df = df.iloc[i:i+batch_size]
        batch_num = i//batch_size + 1
        batch_end = min(i+batch_size, total_rows)
        batch_size_actual = len(batch_df)
        
        try:
            for _, row in batch_df.iterrows():
                row_dict = {}
                for col in columns:
                    value = row[col]
                    if pd.isna(value) or value is None:
                        row_dict[col] = None
                    else:
                        row_dict[col] = value
                
                conn.execute(text(insert_query), row_dict)
            
            progress_pct = (batch_end / total_rows) * 100
            print(f"✓ Batch {batch_num}/{total_batches}: {batch_size_actual:,} rows inserted. Progress: {progress_pct:.1f}%")
            
        except Exception as batch_error:
            print(f"✗ ERROR in batch {batch_num}/{total_batches}: {batch_error}")
            import traceback
            traceback.print_exc()
            raise
    
    print(f" Completed! {total_rows:,} rows inserted successfully.")


def get_last_date_lte_cqi_metrics_daily():
    """
    Get the last date from lte_cqi_metrics_daily table.
    This value + 1 will be used as the init_date for processing.
    
    Returns:
        str: Last date in 'YYYY-MM-DD' format, or None if table is empty
    """
    engine = get_engine()
    
    try:
        with engine.connect() as conn:
            query = "SELECT MAX(date) FROM lte_cqi_metrics_daily"
            result = conn.execute(text(query))
            last_date = result.fetchone()[0]
            
            if last_date:
                return last_date.strftime('%Y-%m-%d')
            else:
                return None
                
    except SQLAlchemyError as e:
        print(f"Error getting last date from lte_cqi_metrics_daily: {e}")
        return None


def get_last_date_lte_cqi_daily():
    """
    Get the last date from lte_cqi_daily table.
    This value will be used as the max_date for processing.
    
    Returns:
        str: Last date in 'YYYY-MM-DD' format, or None if table is empty
    """
    engine = get_engine()
    
    try:
        with engine.connect() as conn:
            query = "SELECT MAX(date) FROM lte_cqi_daily"
            result = conn.execute(text(query))
            last_date = result.fetchone()[0]
            
            if last_date:
                return last_date.strftime('%Y-%m-%d')
            else:
                return None
                
    except SQLAlchemyError as e:
        print(f"Error getting last date from lte_cqi_daily: {e}")
        return None


def lte_cqi_level_export(init_date, max_date, group_level, level_list=None, csv_export=False):
    """
    Export LTE CQI metrics data from lte_cqi_metrics_daily table to DataFrame and optionally to CSV.
    
    Args:
        init_date (str): Start date in format 'YYYY-MM-DD'
        max_date (str): End date in format 'YYYY-MM-DD'
        group_level (str): Level of aggregation ('network', 'region', 'province', 'municipality', 'site')
        level_list (list, optional): List of specific values to filter by for the group_level
        csv_export (bool): Whether to export the data to CSV file
        
    Returns:
        pandas.DataFrame: DataFrame containing the filtered LTE CQI metrics data
    """
    print(f"lte_cqi_level_export called")
    print(f"Parameters:")
    print(f"  init_date: {init_date}")
    print(f"  max_date: {max_date}")
    print(f"  group_level: {group_level}")
    print(f"  level_list: {level_list}")
    print(f"  csv_export: {csv_export}")
    
    valid_levels = ['network', 'region', 'province', 'municipality', 'site']
    if group_level not in valid_levels:
        raise ValueError(f"group_level must be one of {valid_levels}, got: {group_level}")
    
    engine = get_engine()
    
    try:
        with engine.connect() as conn:
            base_query = """
                SELECT date, region, province, municipality, site_att, vendors, group_level,
                       lte_cqi, lte_acc, lte_ret, lte_irat, lte_thp_user_dl, lte_4g_on_3g, 
                       lte_ookla_lat, lte_ookla_thp, lte_traff,
                       lte_cqi_h, lte_acc_h, lte_ret_h, lte_irat_h, lte_thp_user_dl_h, 
                       lte_4g_on_3g_h, lte_ookla_lat_h, lte_ookla_thp_h, lte_traff_h,
                       lte_cqi_e, lte_acc_e, lte_ret_e, lte_irat_e, lte_thp_user_dl_e, 
                       lte_4g_on_3g_e, lte_ookla_lat_e, lte_ookla_thp_e, lte_traff_e,
                       lte_cqi_n, lte_acc_n, lte_ret_n, lte_irat_n, lte_thp_user_dl_n, 
                       lte_4g_on_3g_n, lte_ookla_lat_n, lte_ookla_thp_n, lte_traff_n,
                       lte_cqi_s, lte_acc_s, lte_ret_s, lte_irat_s, lte_thp_user_dl_s, 
                       lte_4g_on_3g_s, lte_ookla_lat_s, lte_ookla_thp_s, lte_traff_s
                FROM lte_cqi_metrics_daily
                WHERE date >= %(init_date)s AND date <= %(max_date)s
            """
            
            params = {"init_date": init_date, "max_date": max_date}
            
            if group_level == 'network':
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
            
            df = pd.read_sql_query(query, conn, params=params)
            
            print(f"Retrieved {len(df)} rows for {group_level} level")
            print(f"Date range: {df['date'].min()} to {df['date'].max()}" if not df.empty else "No data found")
            
            if csv_export and not df.empty:
                level_filter = ""
                if level_list:
                    level_filter = f"_{'_'.join(str(item).replace(' ', '_') for item in level_list[:3])}"
                    if len(level_list) > 3:
                        level_filter += f"_and_{len(level_list)-3}_more"
                
                filename = f"lte_cqi_export_{group_level}_{init_date}_to_{max_date}{level_filter}.csv"
                
                if ROOT_DIRECTORY:
                    output_path = os.path.join(ROOT_DIRECTORY, "output", filename)
                else:
                    output_path = os.path.join("output", filename)
                
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                
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
    Main execution block for testing or running the lte CQI level processor.
    """
    print("lte CQI Level Processor")
    
    try:
        # Get date ranges
        min_date = get_last_date_lte_cqi_metrics_daily()
        max_date = get_last_date_lte_cqi_daily()
        
        print(f"Last processed date in metrics_daily: {min_date}")
        print(f"Latest available date in daily: {max_date}")
        
        # If no data in metrics_daily, start from a reasonable date or use fixed test dates
        if min_date is None:
            min_date = "2024-01-01"  # Set a reasonable start date or use earliest date from daily table
            print(f"No previous data found, starting from: {min_date}")
        
        if max_date is None:
            print("No data available in nr_cqi_daily table")
        else:
            populate_lte_cqi_metrics_daily(min_date, max_date)
            print("Processing completed successfully!")


    except Exception as e:
        print(f"Error during execution: {e}")
        import traceback
        traceback.print_exc()