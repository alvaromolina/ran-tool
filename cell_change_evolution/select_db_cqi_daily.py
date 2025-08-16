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