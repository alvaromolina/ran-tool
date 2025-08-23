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


def populate_nr_cqi_metrics_daily(min_date, max_date):
    """
    Populate the nr_cqi_metrics_daily table with hierarchical aggregation levels.
    Uses Python for calculations and SQL for data aggregation.
    
    Args:
        min_date (str): Start date in format 'YYYY-MM-DD'
        max_date (str): End date in format 'YYYY-MM-DD'
    """
    print(f"Processing NR (5G) CQI metrics for date range: {min_date} to {max_date}")
    
    engine = get_engine()
    
    # Define hierarchical levels (from general to specific)
    levels = [
        {
            'name': 'network',
            'level_fields': [],
            'group_by': ['date'],
            'select_fields': ['date'],
            'null_fields': ['region', 'province', 'municipality', 'city', 'site_att']
        },
        {
            'name': 'region', 
            'level_fields': ['region'],
            'group_by': ['date', 'region'],
            'select_fields': ['date', 'region'],
            'null_fields': ['province', 'municipality', 'city', 'site_att']
        },
        {
            'name': 'province',
            'level_fields': ['region', 'province'],
            'group_by': ['date', 'region', 'province'],
            'select_fields': ['date', 'region', 'province'],
            'null_fields': ['municipality', 'city', 'site_att']
        },
        {
            'name': 'municipality',
            'level_fields': ['region', 'province', 'municipality'],
            'group_by': ['date', 'region', 'province', 'municipality'],
            'select_fields': ['date', 'region', 'province', 'municipality'],
            'null_fields': ['city', 'site_att']
        },
        {
            'name': 'city',
            'level_fields': ['region', 'province', 'municipality', 'city'],
            'group_by': ['date', 'region', 'province', 'municipality', 'city'],
            'select_fields': ['date', 'region', 'province', 'municipality', 'city'],
            'null_fields': ['site_att']
        },
        {
            'name': 'site',
            'level_fields': ['region', 'province', 'municipality', 'city', 'site_att'],
            'group_by': ['date', 'region', 'province', 'municipality', 'city', 'site_att'],
            'select_fields': ['date', 'region', 'province', 'municipality', 'city', 'site_att'],
            'null_fields': []
        }
    ]
    
    try:
        with engine.begin() as conn:
            print(f"Deleting existing records for date range...")
            delete_query = """
                DELETE FROM nr_cqi_metrics_daily 
                WHERE date >= :min_date AND date <= :max_date
            """
            conn.execute(text(delete_query), {"min_date": min_date, "max_date": max_date})
            
            for i, level_config in enumerate(levels, 1):
                level_name = level_config['name']
                print(f"Processing Level {i}/6: {level_name}")
                
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
        GROUP BY {group_by_clause}
        ORDER BY {group_by_clause}
    """
    
    return pd.read_sql_query(query, engine, params={"min_date": min_date, "max_date": max_date})


def process_data_in_chunks(df_raw, level_config, conn, chunk_size=1000):
    """
    Process DataFrame in chunks to avoid memory issues and apply fragmentation-free calculations.
    
    Args:
        df_raw (pandas.DataFrame): Raw aggregated data
        level_config (dict): Level configuration
        conn: Database connection
        chunk_size (int): Number of rows per chunk
    """
    total_rows = len(df_raw)
    total_chunks = (total_rows + chunk_size - 1) // chunk_size
    
    print(f"Processing {total_rows:,} rows in {total_chunks} chunks of {chunk_size}")
    
    for i in range(0, total_rows, chunk_size):
        chunk_df = df_raw.iloc[i:i+chunk_size].copy()
        chunk_num = i//chunk_size + 1
        chunk_end = min(i+chunk_size, total_rows)
        chunk_size_actual = len(chunk_df)
        
        # Apply calculations to this chunk
        df_calculated = apply_nr_calculations(chunk_df, level_config)
        
        # Insert this chunk
        insert_data_in_batches(df_calculated, conn, batch_size=500)
        
        # Show progress
        progress_pct = (chunk_end / total_rows) * 100
        print(f"  ✓ Chunk {chunk_num}/{total_chunks}: {chunk_size_actual:,} rows processed. Progress: {progress_pct:.1f}%")


def apply_nr_calculations(df, level_config):
    """
    Apply NR (5G) calculations using vectorized numpy operations with pd.concat() to avoid DataFrame fragmentation.
    
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
    vendor_cols = [col for col in df.columns if any(vendor in col for vendor in ['e5g_', 'n5g_'])]
    for col in vendor_cols:
        df[col] = zn(df[col])
    
    # Calculate intermediate totals - vectorized
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
    
    # Calculate all new columns using dictionary approach to avoid fragmentation
    new_columns = {}
    
    # Total NR metrics (based on aggregated values from nr_cqi_daily)
    new_columns['nr_acc_mn'] = np.round(
        safe_divide(acc_rrc_num_total, acc_rrc_den_total) *
        safe_divide(s1_sr_num_total, s1_sr_den_total) *
        safe_divide(erab_4g_num_total, erab_4g_den_total) * 100, 8)
    
    new_columns['nr_acc_sn'] = np.round(
        safe_divide(erab_5g_num_total, erab_5g_den_total) * 100, 8)
    
    new_columns['nr_ret_mn'] = np.round(
        (1 - safe_divide(ret_4g_drop_total, ret_4g_att_total)) * 100, 8)
    
    new_columns['nr_endc_ret_tot'] = np.round(
        (1 - safe_divide(ret_5g_drop_total, ret_5g_den_total)) * 100, 8)
    
    new_columns['nr_thp_mn'] = np.round(safe_divide(thp_mn_num_total, thp_mn_den_total), 2)
    new_columns['nr_thp_sn'] = np.round(safe_divide(thp_sn_num_total, thp_sn_den_total), 2)
    
    new_columns['nr_traffic_4gleg_gb'] = np.round(traffic_4gleg_total, 4)
    new_columns['nr_traffic_5gleg_gb'] = np.round(traffic_5gleg_total, 4)
    new_columns['nr_traffic_mac_gb'] = np.round(traffic_mac_total, 4)
    
    # NR CQI Formula with VALIDATED weights and C coefficients:
    # W1*EXP((1-Acc Mn)*C1)+W2*EXP((1-Acc SN)*C2)+W3*EXP((1-Ret MN)*C3)+W4*EXP((1-Endc Ret Tot)*C4)+W5*(1-EXP(Thp MN*1000*C5))+W6*(1-EXP(Thp SN*1000*C6))
    # Weights: Acc MN 17%, Acc SN 13%, Ret MN 17%, ENDC Ret Tot 13%, Thp MN 20%, Thp SN 20%
    # C coefficients validated to match production system (difference <0.1%)
    new_columns['nr_cqi'] = np.round(
        (0.17 * np.exp((1 - new_columns['nr_acc_mn'] / 100) * -14.92648157) +
         0.13 * np.exp((1 - new_columns['nr_acc_sn'] / 100) * -26.68090256) +
         0.17 * np.exp((1 - new_columns['nr_ret_mn'] / 100) * -14.92648157) +
         0.13 * np.exp((1 - new_columns['nr_endc_ret_tot'] / 100) * -26.68090256) +
         0.20 * (1 - np.exp(new_columns['nr_thp_mn'] * 1000 * -0.0002006621)) +
         0.20 * (1 - np.exp(new_columns['nr_thp_sn'] * 1000 * -0.0002006621))) * 100, 8)
    
    # Ericsson-specific metrics
    new_columns['nr_acc_mn_e'] = np.round(
        safe_divide(df['e5g_acc_rrc_num_n'], df['e5g_acc_rrc_den_n']) *
        safe_divide(df['e5g_s1_sr_num_n'], df['e5g_s1_sr_den_n']) *
        safe_divide(df['e5g_nsa_acc_erab_sr_4gendc_num_n'], df['e5g_nsa_acc_erab_sr_4gendc_den_n']) * 100, 8)
    
    new_columns['nr_acc_sn_e'] = np.round(
        safe_divide(df['e5g_nsa_acc_erab_succ_5gendc_5gleg_n'], df['e5g_nsa_acc_erab_att_5gendc_5gleg_n']) * 100, 8)
    
    new_columns['nr_ret_mn_e'] = np.round(
        (1 - safe_divide(df['e5g_nsa_ret_erab_drop_4gendc_n'], df['e5g_nsa_ret_erab_att_4gendc_n'])) * 100, 8)
    
    new_columns['nr_endc_ret_tot_e'] = np.round(
        (1 - safe_divide(df['e5g_nsa_ret_erab_drop_5gendc_4g5gleg_num_n'], df['e5g_nsa_ret_erab_drop_5gendc_4g5gleg_den_n'])) * 100, 8)
    
    new_columns['nr_thp_mn_e'] = np.round(safe_divide(df['e5g_nsa_thp_mn_num'], df['e5g_nsa_thp_mn_den']), 2)
    new_columns['nr_thp_sn_e'] = np.round(safe_divide(df['e5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_num_n'], df['e5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_denom_n']), 2)
    
    new_columns['nr_traffic_4gleg_gb_e'] = np.round(df['e5g_nsa_traffic_pdcp_gb_5gendc_4glegn'], 4)
    new_columns['nr_traffic_5gleg_gb_e'] = np.round(df['e5g_nsa_traffic_pdcp_gb_5gendc_5gleg'], 4)
    new_columns['nr_traffic_mac_gb_e'] = np.round(df['e5g_nsa_traffic_mac_gb_5gendc_5gleg_n'], 4)
    
    new_columns['nr_cqi_e'] = np.round(
        (0.17 * np.exp((1 - new_columns['nr_acc_mn_e'] / 100) * -14.92648157) +
         0.13 * np.exp((1 - new_columns['nr_acc_sn_e'] / 100) * -26.68090256) +
         0.17 * np.exp((1 - new_columns['nr_ret_mn_e'] / 100) * -14.92648157) +
         0.13 * np.exp((1 - new_columns['nr_endc_ret_tot_e'] / 100) * -26.68090256) +
         0.20 * (1 - np.exp(new_columns['nr_thp_mn_e'] * 1000 * -0.0002006621)) +
         0.20 * (1 - np.exp(new_columns['nr_thp_sn_e'] * 1000 * -0.0002006621))) * 100, 8)
    
    # Nokia-specific metrics
    new_columns['nr_acc_mn_n'] = np.round(
        safe_divide(df['n5g_acc_rrc_num_n'], df['n5g_acc_rrc_den_n']) *
        safe_divide(df['n5g_s1_sr_num_n'], df['n5g_s1_sr_den_n']) *
        safe_divide(df['n5g_nsa_acc_erab_sr_4gendc_num_n'], df['n5g_nsa_acc_erab_sr_4gendc_den_n']) * 100, 8)
    
    new_columns['nr_acc_sn_n'] = np.round(
        safe_divide(df['n5g_nsa_acc_erab_succ_5gendc_5gleg_n'], df['n5g_nsa_acc_erab_att_5gendc_5gleg_n']) * 100, 8)
    
    new_columns['nr_ret_mn_n'] = np.round(
        (1 - safe_divide(df['n5g_nsa_ret_erab_drop_4gendc_n'], df['n5g_nsa_ret_erab_att_4gendc_n'])) * 100, 8)
    
    new_columns['nr_endc_ret_tot_n'] = np.round(
        (1 - safe_divide(df['n5g_nsa_ret_erab_drop_5gendc_4g5gleg_num_n'], df['n5g_nsa_ret_erab_drop_5gendc_4g5gleg_den_n'])) * 100, 8)
    
    new_columns['nr_thp_mn_n'] = np.round(safe_divide(df['n5g_nsa_thp_mn_num'], df['n5g_nsa_thp_mn_den']), 2)
    new_columns['nr_thp_sn_n'] = np.round(safe_divide(df['n5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_num_n'], df['n5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_denom_n']), 2)
    
    new_columns['nr_traffic_4gleg_gb_n'] = np.round(df['n5g_nsa_traffic_pdcp_gb_5gendc_4glegn'], 4)
    new_columns['nr_traffic_5gleg_gb_n'] = np.round(df['n5g_nsa_traffic_pdcp_gb_5gendc_5gleg'], 4)
    new_columns['nr_traffic_mac_gb_n'] = np.round(df['n5g_nsa_traffic_mac_gb_5gendc_5gleg_n'], 4)
    
    new_columns['nr_cqi_n'] = np.round(
        (0.17 * np.exp((1 - new_columns['nr_acc_mn_n'] / 100) * -14.92648157) +
         0.13 * np.exp((1 - new_columns['nr_acc_sn_n'] / 100) * -26.68090256) +
         0.17 * np.exp((1 - new_columns['nr_ret_mn_n'] / 100) * -14.92648157) +
         0.13 * np.exp((1 - new_columns['nr_endc_ret_tot_n'] / 100) * -26.68090256) +
         0.20 * (1 - np.exp(new_columns['nr_thp_mn_n'] * 1000 * -0.0002006621)) +
         0.20 * (1 - np.exp(new_columns['nr_thp_sn_n'] * 1000 * -0.0002006621))) * 100, 8)
    
    # Create new DataFrame with calculated columns using pd.concat() to avoid fragmentation
    new_df = pd.DataFrame(new_columns, index=df.index)
    df_with_calculations = pd.concat([df, new_df], axis=1)
    
    # Select only the columns needed for the target table
    target_columns = ['date', 'region', 'province', 'municipality', 'city', 'site_att', 'vendors', 'group_level',
                     'nr_cqi', 'nr_acc_mn', 'nr_acc_sn', 'nr_ret_mn', 'nr_endc_ret_tot', 'nr_thp_mn', 'nr_thp_sn',
                     'nr_traffic_4gleg_gb', 'nr_traffic_5gleg_gb', 'nr_traffic_mac_gb',
                     'nr_cqi_e', 'nr_acc_mn_e', 'nr_acc_sn_e', 'nr_ret_mn_e', 'nr_endc_ret_tot_e', 'nr_thp_mn_e', 'nr_thp_sn_e',
                     'nr_traffic_4gleg_gb_e', 'nr_traffic_5gleg_gb_e', 'nr_traffic_mac_gb_e',
                     'nr_cqi_n', 'nr_acc_mn_n', 'nr_acc_sn_n', 'nr_ret_mn_n', 'nr_endc_ret_tot_n', 'nr_thp_mn_n', 'nr_thp_sn_n',
                     'nr_traffic_4gleg_gb_n', 'nr_traffic_5gleg_gb_n', 'nr_traffic_mac_gb_n']
    
    return df_with_calculations[target_columns]


def insert_data_in_batches(df, conn, batch_size=1000):
    """
    Insert DataFrame data into nr_cqi_metrics_daily table in batches.
    
    Args:
        df (pandas.DataFrame): Data to insert
        conn: Database connection
        batch_size (int): Number of rows per batch
    """
    total_rows = len(df)
    total_batches = (total_rows + batch_size - 1) // batch_size
    
    print(f"    Starting batch insertion: {total_rows:,} rows in {total_batches} batches")
    
    # Create column names and placeholders once
    columns = list(df.columns)
    placeholders = ', '.join([f':{col}' for col in columns])
    columns_str = ', '.join(columns)
    
    insert_query = f"""
        INSERT INTO nr_cqi_metrics_daily ({columns_str})
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
            print(f"    ✓ Batch {batch_num}/{total_batches}: {batch_size_actual:,} rows inserted. Progress: {progress_pct:.1f}%")
            
        except Exception as batch_error:
            print(f"    ✗ ERROR in batch {batch_num}/{total_batches}: {batch_error}")
            import traceback
            traceback.print_exc()
            raise
    
    print(f"    Completed! {total_rows:,} rows inserted successfully.")


def get_last_date_nr_cqi_metrics_daily():
    """
    Get the last date from nr_cqi_metrics_daily table.
    This value + 1 will be used as the init_date for processing.
    
    Returns:
        str: Last date in 'YYYY-MM-DD' format, or None if table is empty
    """
    engine = get_engine()
    
    try:
        with engine.connect() as conn:
            query = "SELECT MAX(date) FROM nr_cqi_metrics_daily"
            result = conn.execute(text(query))
            last_date = result.fetchone()[0]
            
            if last_date:
                return last_date.strftime('%Y-%m-%d')
            else:
                return None
                
    except SQLAlchemyError as e:
        print(f"Error getting last date from nr_cqi_metrics_daily: {e}")
        return None


def get_last_date_nr_cqi_daily():
    """
    Get the last date from nr_cqi_daily table.
    This value will be used as the max_date for processing.
    
    Returns:
        str: Last date in 'YYYY-MM-DD' format, or None if table is empty
    """
    engine = get_engine()
    
    try:
        with engine.connect() as conn:
            query = "SELECT MAX(date) FROM nr_cqi_daily"
            result = conn.execute(text(query))
            last_date = result.fetchone()[0]
            
            if last_date:
                return last_date.strftime('%Y-%m-%d')
            else:
                return None
                
    except SQLAlchemyError as e:
        print(f"Error getting last date from nr_cqi_daily: {e}")
        return None


def nr_cqi_level_export(init_date, max_date, group_level, level_list=None, csv_export=False):
    """
    Export NR CQI metrics data from nr_cqi_metrics_daily table to DataFrame and optionally to CSV.
    
    Args:
        init_date (str): Start date in format 'YYYY-MM-DD'
        max_date (str): End date in format 'YYYY-MM-DD'
        group_level (str): Level of aggregation ('network', 'region', 'province', 'municipality', 'city', 'site')
        level_list (list, optional): List of specific values to filter by for the group_level
        csv_export (bool): Whether to export the data to CSV file
        
    Returns:
        pandas.DataFrame: DataFrame containing the filtered NR CQI metrics data
    """
    print(f"nr_cqi_level_export called")
    print(f"Parameters:")
    print(f"  init_date: {init_date}")
    print(f"  max_date: {max_date}")
    print(f"  group_level: {group_level}")
    print(f"  level_list: {level_list}")
    print(f"  csv_export: {csv_export}")
    
    # Validate group_level parameter
    valid_levels = ['network', 'region', 'province', 'municipality', 'city', 'site']
    if group_level not in valid_levels:
        raise ValueError(f"group_level must be one of {valid_levels}, got: {group_level}")
    
    engine = get_engine()
    
    try:
        with engine.connect() as conn:
            # Build the base query
            base_query = """
                SELECT date, region, province, municipality, city, site_att, vendors, group_level,
                       nr_cqi, nr_acc_mn, nr_acc_sn, nr_ret_mn, nr_endc_ret_tot, nr_thp_mn, nr_thp_sn,
                       nr_traffic_4gleg_gb, nr_traffic_5gleg_gb, nr_traffic_mac_gb,
                       nr_cqi_e, nr_acc_mn_e, nr_acc_sn_e, nr_ret_mn_e, nr_endc_ret_tot_e, nr_thp_mn_e, nr_thp_sn_e,
                       nr_traffic_4gleg_gb_e, nr_traffic_5gleg_gb_e, nr_traffic_mac_gb_e,
                       nr_cqi_n, nr_acc_mn_n, nr_acc_sn_n, nr_ret_mn_n, nr_endc_ret_tot_n, nr_thp_mn_n, nr_thp_sn_n,
                       nr_traffic_4gleg_gb_n, nr_traffic_5gleg_gb_n, nr_traffic_mac_gb_n
                FROM nr_cqi_metrics_daily
                WHERE date >= %(init_date)s AND date <= %(max_date)s
            """
            
            # Add group_level filter and specific level filters
            params = {"init_date": init_date, "max_date": max_date}
            
            if group_level == 'network':
                query = base_query + " AND group_level = 'network'"
                query += " ORDER BY date"
                
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
                
            elif group_level == 'city':
                query = base_query + " AND group_level = 'city'"
                if level_list:
                    placeholders = ', '.join([f"%(city_{i})s" for i in range(len(level_list))])
                    query += f" AND city IN ({placeholders})"
                    for i, city in enumerate(level_list):
                        params[f"city_{i}"] = city
                query += " ORDER BY date, region, province, municipality, city"
                
            elif group_level == 'site':
                query = base_query + " AND group_level = 'site'"
                if level_list:
                    placeholders = ', '.join([f"%(site_{i})s" for i in range(len(level_list))])
                    query += f" AND site_att IN ({placeholders})"
                    for i, site in enumerate(level_list):
                        params[f"site_{i}"] = site
                query += " ORDER BY date, region, province, municipality, city, site_att"
            
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
                
                filename = f"nr_cqi_export_{group_level}_{init_date}_to_{max_date}{level_filter}.csv"
                
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
    Main execution block for testing or running the NR (5G) CQI level processor.
    """
    print("NR (5G) CQI Level Processor")
    
    try:
        # Get date ranges
        min_date = get_last_date_nr_cqi_metrics_daily()
        max_date = get_last_date_nr_cqi_daily()
        
        print(f"Last processed date in metrics_daily: {min_date}")
        print(f"Latest available date in daily: {max_date}")
        
        # If no data in metrics_daily, start from a reasonable date or use fixed test dates
        if min_date is None:
            min_date = "2024-01-01"  # Set a reasonable start date or use earliest date from daily table
            print(f"No previous data found, starting from: {min_date}")
        
        if max_date is None:
            print("No data available in nr_cqi_daily table")
        else:
            populate_nr_cqi_metrics_daily(min_date, max_date)
            print("Processing completed successfully!")

    except Exception as e:
        print(f"Error during execution: {e}")
        import traceback
        traceback.print_exc()