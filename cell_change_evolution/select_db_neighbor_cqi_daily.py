import os
import dotenv
import pandas as pd
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

def get_neighbor_sites(site_list, radius_km=5):
    """Get neighbor sites within radius using PostGIS"""
    engine = create_connection()
    if engine is None:
        return []
    
    if isinstance(site_list, str):
        site_list = [site_list]
    
    if not site_list:
        print("No sites provided")
        return []
    
    try:
        # Create placeholders for individual sites
        site_placeholders = ', '.join([f"'{site}'" for site in site_list])
        radius_meters = radius_km * 1000
        
        neighbor_query = text(f"""
        WITH center_sites AS (
            SELECT latitude, longitude, att_name
            FROM public.master_node_total 
            WHERE att_name IN ({site_placeholders})
            AND latitude IS NOT NULL 
            AND longitude IS NOT NULL
        )
        SELECT DISTINCT
            m.att_name
        FROM public.master_node_total m
        CROSS JOIN center_sites c
        WHERE m.att_name IS NOT NULL
        AND m.latitude IS NOT NULL 
        AND m.longitude IS NOT NULL
        AND m.att_name != c.att_name
        AND m.att_name NOT IN ({site_placeholders})
        AND ST_DWithin(
            ST_GeogFromText('POINT(' || c.longitude || ' ' || c.latitude || ')'),
            ST_GeogFromText('POINT(' || m.longitude || ' ' || m.latitude || ')'),
            {radius_meters}
        )
        ORDER BY m.att_name;
        """)
        
        df = pd.read_sql_query(neighbor_query, engine)
        neighbor_sites = df['att_name'].tolist()
        
        print(f"Found {len(neighbor_sites)} unique neighbor sites within {radius_km}km of {len(site_list)} center sites")
        return neighbor_sites
        
    except Exception as e:
        print(f"Error fetching neighbor sites for '{site_list}': {e}")
        return []
    finally:
        engine.dispose()

def get_neighbor_cqi_daily(site_list, min_date=None, max_date=None, technology=None, radius_km=5):
    """Get CQI data for neighbor sites within radius using direct SQL, aggregated daily across neighbors."""
    engine = create_connection()
    if engine is None:
        return None
    
    if isinstance(site_list, str):
        site_list = [site_list]
    
    if not site_list:
        print("No sites provided")
        return pd.DataFrame()
    
    try:
        site_placeholders = ', '.join([f"'{site}'" for site in site_list])
        radius_meters = radius_km * 1000

        # Build per-technology SELECTs with date pushdown
        selects = []

        # 4G branch
        if technology in (None, '4G'):
            lte_where = [
                "l.site_att IN (SELECT att_name FROM neighbor_sites)"
            ]
            if min_date:
                lte_where.append(f"l.date >= '{min_date}'")
            if max_date:
                lte_where.append(f"l.date <= '{max_date}'")
            selects.append(f"""
                SELECT 
                    l.date AS time,
                    CAST(l.f4g_composite_quality AS DOUBLE PRECISION) AS lte_cqi,
                    NULL::DOUBLE PRECISION AS nr_cqi,
                    NULL::DOUBLE PRECISION AS umts_cqi
                FROM lte_cqi_daily l
                WHERE {' AND '.join(lte_where)}
            """)

        # 5G branch
        if technology in (None, '5G'):
            nr_where = [
                "n.site_att IN (SELECT att_name FROM neighbor_sites)"
            ]
            if min_date:
                nr_where.append(f"n.date >= '{min_date}'")
            if max_date:
                nr_where.append(f"n.date <= '{max_date}'")
            selects.append(f"""
                SELECT 
                    n.date AS time,
                    NULL::DOUBLE PRECISION AS lte_cqi,
                    CAST(n.nr_composite_quality AS DOUBLE PRECISION) AS nr_cqi,
                    NULL::DOUBLE PRECISION AS umts_cqi
                FROM nr_cqi_daily n
                WHERE {' AND '.join(nr_where)}
            """)

        # 3G branch
        if technology in (None, '3G'):
            umts_where = [
                "u.site_att IN (SELECT att_name FROM neighbor_sites)"
            ]
            if min_date:
                umts_where.append(f"u.date >= '{min_date}'")
            if max_date:
                umts_where.append(f"u.date <= '{max_date}'")
            selects.append(f"""
                SELECT 
                    u.date AS time,
                    NULL::DOUBLE PRECISION AS lte_cqi,
                    NULL::DOUBLE PRECISION AS nr_cqi,
                    CAST(u.umts_composite_quality AS DOUBLE PRECISION) AS umts_cqi
                FROM umts_cqi_daily u
                WHERE {' AND '.join(umts_where)}
            """)

        union_block = "\nUNION ALL\n".join(selects) if selects else "SELECT NULL::timestamp AS time, NULL::double precision AS lte_cqi, NULL::double precision AS nr_cqi, NULL::double precision AS umts_cqi LIMIT 0"

        neighbor_cqi_query = text(f"""
        WITH center_sites AS (
            SELECT latitude, longitude, att_name
            FROM public.master_node_total 
            WHERE att_name IN ({site_placeholders})
              AND latitude IS NOT NULL 
              AND longitude IS NOT NULL
        ),
        neighbor_sites AS (
            SELECT DISTINCT m.att_name
            FROM public.master_node_total m
            CROSS JOIN center_sites c
            WHERE m.att_name IS NOT NULL
              AND m.latitude IS NOT NULL 
              AND m.longitude IS NOT NULL
              AND m.att_name != c.att_name
              AND m.att_name NOT IN ({site_placeholders})
              AND ST_DWithin(
                ST_GeogFromText('POINT(' || c.longitude || ' ' || c.latitude || ')'),
                ST_GeogFromText('POINT(' || m.longitude || ' ' || m.latitude || ')'),
                {radius_meters}
              )
        )
        SELECT time,
               AVG(lte_cqi)  AS lte_cqi,
               AVG(nr_cqi)   AS nr_cqi,
               AVG(umts_cqi) AS umts_cqi
        FROM (
            {union_block}
        ) norm
        GROUP BY time
        ORDER BY time ASC
        """)

        result_df = pd.read_sql_query(neighbor_cqi_query, engine)
        print(f"Retrieved neighbor CQI data for {len(site_list)} center sites: {result_df.shape[0]} records")
        return result_df
        
    except Exception as e:
        print(f"Error executing neighbor CQI query: {e}")
        return None
    finally:
        engine.dispose()

def get_neighbor_traffic_data(site_list, min_date=None, max_date=None, technology=None, radius_km=5, vendor=None):
    """Get traffic data for neighbor sites within radius using direct SQL, aggregated daily across neighbors.

    Adds aggregated columns:
      - ps_gb_uldl (GB): total PS traffic for 3G/4G
      - traffic_dlul_tb (TB): total PDCP traffic for 5G (converted from GB)
    """
    engine = create_connection()
    if engine is None:
        return None
    
    if isinstance(site_list, str):
        site_list = [site_list]
    
    if not site_list:
        print("No sites provided")
        return pd.DataFrame()
    
    try:
        site_placeholders = ', '.join([f"'{site}'" for site in site_list])
        radius_meters = radius_km * 1000
        
        # Build technology-specific query
        if technology == '3G':
            select_cols = """
                u.date AS time,
                u.site_att,
                NULL::DOUBLE PRECISION as h4g_traffic_d_user_ps_gb,
                NULL::DOUBLE PRECISION as s4g_traffic_d_user_ps_gb,
                NULL::DOUBLE PRECISION as e4g_traffic_d_user_ps_gb,
                NULL::DOUBLE PRECISION as n4g_traffic_d_user_ps_gb,
                NULL::DOUBLE PRECISION as e5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
                NULL::DOUBLE PRECISION as n5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
                NULL::DOUBLE PRECISION as e5g_nsa_traffic_pdcp_gb_5gendc_5gleg,
                NULL::DOUBLE PRECISION as n5g_nsa_traffic_pdcp_gb_5gendc_5gleg,
                CAST(u.h3g_traffic_d_user_ps_gb AS DOUBLE PRECISION) AS h3g_traffic_d_user_ps_gb,
                CAST(u.e3g_traffic_d_user_ps_gb AS DOUBLE PRECISION) AS e3g_traffic_d_user_ps_gb,
                CAST(u.n3g_traffic_d_user_ps_gb AS DOUBLE PRECISION) AS n3g_traffic_d_user_ps_gb
            """
            data_join = "LEFT JOIN umts_cqi_daily u ON ns.att_name = u.site_att"
            tech_condition = "(u.h3g_traffic_d_user_ps_gb IS NOT NULL OR u.e3g_traffic_d_user_ps_gb IS NOT NULL OR u.n3g_traffic_d_user_ps_gb IS NOT NULL)"
            
        elif technology == '4G':
            select_cols = """
                l.date AS time,
                l.site_att,
                CAST(l.h4g_traffic_d_user_ps_gb AS DOUBLE PRECISION) AS h4g_traffic_d_user_ps_gb,
                CAST(l.s4g_traffic_d_user_ps_gb AS DOUBLE PRECISION) AS s4g_traffic_d_user_ps_gb,
                CAST(l.e4g_traffic_d_user_ps_gb AS DOUBLE PRECISION) AS e4g_traffic_d_user_ps_gb,
                CAST(l.n4g_traffic_d_user_ps_gb AS DOUBLE PRECISION) AS n4g_traffic_d_user_ps_gb,
                NULL::DOUBLE PRECISION as e5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
                NULL::DOUBLE PRECISION as n5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
                NULL::DOUBLE PRECISION as e5g_nsa_traffic_pdcp_gb_5gendc_5gleg,
                NULL::DOUBLE PRECISION as n5g_nsa_traffic_pdcp_gb_5gendc_5gleg,
                NULL::DOUBLE PRECISION as h3g_traffic_d_user_ps_gb,
                NULL::DOUBLE PRECISION as e3g_traffic_d_user_ps_gb,
                NULL::DOUBLE PRECISION as n3g_traffic_d_user_ps_gb
            """
            data_join = "LEFT JOIN lte_cqi_daily l ON ns.att_name = l.site_att"
            tech_condition = "(l.h4g_traffic_d_user_ps_gb IS NOT NULL OR l.s4g_traffic_d_user_ps_gb IS NOT NULL OR l.e4g_traffic_d_user_ps_gb IS NOT NULL OR l.n4g_traffic_d_user_ps_gb IS NOT NULL)"
            
        elif technology == '5G':
            select_cols = """
                n.date AS time,
                n.site_att,
                NULL::DOUBLE PRECISION as h4g_traffic_d_user_ps_gb,
                NULL::DOUBLE PRECISION as s4g_traffic_d_user_ps_gb,
                NULL::DOUBLE PRECISION as e4g_traffic_d_user_ps_gb,
                NULL::DOUBLE PRECISION as n4g_traffic_d_user_ps_gb,
                CAST(n.e5g_nsa_traffic_pdcp_gb_5gendc_4glegn AS DOUBLE PRECISION) AS e5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
                CAST(n.n5g_nsa_traffic_pdcp_gb_5gendc_4glegn AS DOUBLE PRECISION) AS n5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
                CAST(n.e5g_nsa_traffic_pdcp_gb_5gendc_5gleg AS DOUBLE PRECISION) AS e5g_nsa_traffic_pdcp_gb_5gendc_5gleg,
                CAST(n.n5g_nsa_traffic_pdcp_gb_5gendc_5gleg AS DOUBLE PRECISION) AS n5g_nsa_traffic_pdcp_gb_5gendc_5gleg,
                NULL::DOUBLE PRECISION as h3g_traffic_d_user_ps_gb,
                NULL::DOUBLE PRECISION as e3g_traffic_d_user_ps_gb,
                NULL::DOUBLE PRECISION as n3g_traffic_d_user_ps_gb
            """
            data_join = "LEFT JOIN nr_cqi_daily n ON ns.att_name = n.site_att"
            tech_condition = "(n.e5g_nsa_traffic_pdcp_gb_5gendc_4glegn IS NOT NULL OR n.n5g_nsa_traffic_pdcp_gb_5gendc_4glegn IS NOT NULL OR n.e5g_nsa_traffic_pdcp_gb_5gendc_5gleg IS NOT NULL OR n.n5g_nsa_traffic_pdcp_gb_5gendc_5gleg IS NOT NULL)"
            
        else:  # ALL technologies
            select_cols = """
                COALESCE(u.date, l.date, n.date) AS time,
                COALESCE(u.site_att, l.site_att, n.site_att) AS site_att,
                CAST(l.h4g_traffic_d_user_ps_gb AS DOUBLE PRECISION) AS h4g_traffic_d_user_ps_gb,
                CAST(l.s4g_traffic_d_user_ps_gb AS DOUBLE PRECISION) AS s4g_traffic_d_user_ps_gb,
                CAST(l.e4g_traffic_d_user_ps_gb AS DOUBLE PRECISION) AS e4g_traffic_d_user_ps_gb,
                CAST(l.n4g_traffic_d_user_ps_gb AS DOUBLE PRECISION) AS n4g_traffic_d_user_ps_gb,
                CAST(n.e5g_nsa_traffic_pdcp_gb_5gendc_4glegn AS DOUBLE PRECISION) AS e5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
                CAST(n.n5g_nsa_traffic_pdcp_gb_5gendc_4glegn AS DOUBLE PRECISION) AS n5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
                CAST(n.e5g_nsa_traffic_pdcp_gb_5gendc_5gleg AS DOUBLE PRECISION) AS e5g_nsa_traffic_pdcp_gb_5gendc_5gleg,
                CAST(n.n5g_nsa_traffic_pdcp_gb_5gendc_5gleg AS DOUBLE PRECISION) AS n5g_nsa_traffic_pdcp_gb_5gendc_5gleg,
                CAST(u.h3g_traffic_d_user_ps_gb AS DOUBLE PRECISION) AS h3g_traffic_d_user_ps_gb,
                CAST(u.e3g_traffic_d_user_ps_gb AS DOUBLE PRECISION) AS e3g_traffic_d_user_ps_gb,
                CAST(u.n3g_traffic_d_user_ps_gb AS DOUBLE PRECISION) AS n3g_traffic_d_user_ps_gb
            """
            data_join = """
                LEFT JOIN umts_cqi_daily u ON ns.att_name = u.site_att
                FULL OUTER JOIN lte_cqi_daily l ON u.date = l.date AND u.site_att = l.site_att
                FULL OUTER JOIN nr_cqi_daily n ON COALESCE(u.date, l.date) = n.date AND COALESCE(u.site_att, l.site_att) = n.site_att
            """
            tech_condition = "1=1"  # No technology filter for ALL
        
        # Build date and vendor conditions
        conditions = [tech_condition]
        
        if min_date:
            if technology in ['3G', '4G', '5G']:
                conditions.append(f"date >= '{min_date}'")
            else:
                conditions.append(f"COALESCE(u.date, l.date, n.date) >= '{min_date}'")
                
        if max_date:
            if technology in ['3G', '4G', '5G']:
                conditions.append(f"date <= '{max_date}'")
            else:
                conditions.append(f"COALESCE(u.date, l.date, n.date) <= '{max_date}'")
        
        if vendor and technology in ['3G', '4G']:
            vendor_map = {'huawei': 'h', 'ericsson': 'e', 'nokia': 'n', 'samsung': 's'}
            vendor_prefix = vendor_map.get(vendor.lower())
            if vendor_prefix:
                if technology == '3G':
                    conditions.append(f"u.{vendor_prefix}3g_traffic_d_user_ps_gb IS NOT NULL")
                elif technology == '4G':
                    conditions.append(f"l.{vendor_prefix}4g_traffic_d_user_ps_gb IS NOT NULL")
        
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions != ["1=1"] else ""
        
        neighbor_traffic_query = text(f"""
        WITH center_sites AS (
            SELECT latitude, longitude, att_name
            FROM public.master_node_total 
            WHERE att_name IN ({site_placeholders})
            AND latitude IS NOT NULL 
            AND longitude IS NOT NULL
        ),
        neighbor_sites AS (
            SELECT DISTINCT m.att_name
            FROM public.master_node_total m
            CROSS JOIN center_sites c
            WHERE m.att_name IS NOT NULL
            AND m.latitude IS NOT NULL 
            AND m.longitude IS NOT NULL
            AND m.att_name != c.att_name
            AND m.att_name NOT IN ({site_placeholders})
            AND ST_DWithin(
                ST_GeogFromText('POINT(' || c.longitude || ' ' || c.latitude || ')'),
                ST_GeogFromText('POINT(' || m.longitude || ' ' || m.latitude || ')'),
                {radius_meters}
            )
        )
        SELECT time,
               -- aggregated raw columns (nullable depending on technology)
               AVG(h3g_traffic_d_user_ps_gb) AS h3g_traffic_d_user_ps_gb,
               AVG(e3g_traffic_d_user_ps_gb) AS e3g_traffic_d_user_ps_gb,
               AVG(n3g_traffic_d_user_ps_gb) AS n3g_traffic_d_user_ps_gb,
               AVG(h4g_traffic_d_user_ps_gb) AS h4g_traffic_d_user_ps_gb,
               AVG(s4g_traffic_d_user_ps_gb) AS s4g_traffic_d_user_ps_gb,
               AVG(e4g_traffic_d_user_ps_gb) AS e4g_traffic_d_user_ps_gb,
               AVG(n4g_traffic_d_user_ps_gb) AS n4g_traffic_d_user_ps_gb,
               AVG(e5g_nsa_traffic_pdcp_gb_5gendc_4glegn) AS e5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
               AVG(n5g_nsa_traffic_pdcp_gb_5gendc_4glegn) AS n5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
               AVG(e5g_nsa_traffic_pdcp_gb_5gendc_5gleg) AS e5g_nsa_traffic_pdcp_gb_5gendc_5gleg,
               AVG(n5g_nsa_traffic_pdcp_gb_5gendc_5gleg) AS n5g_nsa_traffic_pdcp_gb_5gendc_5gleg,
               -- aggregated totals for evaluator convenience
               (
                 COALESCE(AVG(h3g_traffic_d_user_ps_gb),0) + COALESCE(AVG(e3g_traffic_d_user_ps_gb),0) + COALESCE(AVG(n3g_traffic_d_user_ps_gb),0) +
                 COALESCE(AVG(h4g_traffic_d_user_ps_gb),0) + COALESCE(AVG(s4g_traffic_d_user_ps_gb),0) + COALESCE(AVG(e4g_traffic_d_user_ps_gb),0) + COALESCE(AVG(n4g_traffic_d_user_ps_gb),0)
               ) AS ps_gb_uldl,
               (
                 COALESCE(AVG(e5g_nsa_traffic_pdcp_gb_5gendc_4glegn),0) + COALESCE(AVG(n5g_nsa_traffic_pdcp_gb_5gendc_4glegn),0) +
                 COALESCE(AVG(e5g_nsa_traffic_pdcp_gb_5gendc_5gleg),0) + COALESCE(AVG(n5g_nsa_traffic_pdcp_gb_5gendc_5gleg),0)
               ) / 1024.0 AS traffic_dlul_tb
        FROM (
            SELECT {select_cols}
            FROM neighbor_sites ns
            {data_join}
            {where_clause}
        ) s
        GROUP BY time
        ORDER BY time ASC
        """)
        
        result_df = pd.read_sql_query(neighbor_traffic_query, engine)
        print(f"Retrieved neighbor traffic data for {len(site_list)} center sites: {result_df.shape[0]} records")
        return result_df
        
    except Exception as e:
        print(f"Error executing neighbor traffic query: {e}")
        return None
    finally:
        engine.dispose()

def get_neighbor_traffic_voice(site_list, min_date=None, max_date=None, technology=None, radius_km=5, vendor=None):
    """Get voice traffic data for neighbor sites within radius using direct SQL, aggregated daily across neighbors.

    Adds aggregated column:
      - traffic_voice: total voice traffic across technologies/vendors.
    """
    engine = create_connection()
    if engine is None:
        return None
    
    if isinstance(site_list, str):
        site_list = [site_list]
    
    if not site_list:
        print("No sites provided")
        return pd.DataFrame()
    
    try:
        site_placeholders = ', '.join([f"'{site}'" for site in site_list])
        radius_meters = radius_km * 1000
        
        # Build technology-specific query
        if technology == '3G':
            select_cols = """
                u.date AS time,
                u.site_att,
                NULL::DOUBLE PRECISION as user_traffic_volte_e,
                NULL::DOUBLE PRECISION as user_traffic_volte_h,
                NULL::DOUBLE PRECISION as user_traffic_volte_n,
                NULL::DOUBLE PRECISION as user_traffic_volte_s,
                CAST(u.h3g_traffic_v_user_cs AS DOUBLE PRECISION) AS h3g_traffic_v_user_cs,
                CAST(u.e3g_traffic_v_user_cs AS DOUBLE PRECISION) AS e3g_traffic_v_user_cs,
                CAST(u.n3g_traffic_v_user_cs AS DOUBLE PRECISION) AS n3g_traffic_v_user_cs
            """
            data_join = "LEFT JOIN umts_cqi_daily u ON ns.att_name = u.site_att"
            tech_condition = "(u.h3g_traffic_v_user_cs IS NOT NULL OR u.e3g_traffic_v_user_cs IS NOT NULL OR u.n3g_traffic_v_user_cs IS NOT NULL)"
            
        elif technology == '4G':
            select_cols = """
                v.date AS time,
                v.site_att,
                CAST(v.user_traffic_volte_e AS DOUBLE PRECISION) AS user_traffic_volte_e,
                CAST(v.user_traffic_volte_h AS DOUBLE PRECISION) AS user_traffic_volte_h,
                CAST(v.user_traffic_volte_n AS DOUBLE PRECISION) AS user_traffic_volte_n,
                CAST(v.user_traffic_volte_s AS DOUBLE PRECISION) AS user_traffic_volte_s,
                NULL::DOUBLE PRECISION as h3g_traffic_v_user_cs,
                NULL::DOUBLE PRECISION as e3g_traffic_v_user_cs,
                NULL::DOUBLE PRECISION as n3g_traffic_v_user_cs
            """
            data_join = "LEFT JOIN volte_cqi_vendor_daily v ON ns.att_name = v.site_att"
            tech_condition = "(v.user_traffic_volte_e IS NOT NULL OR v.user_traffic_volte_h IS NOT NULL OR v.user_traffic_volte_n IS NOT NULL OR v.user_traffic_volte_s IS NOT NULL)"
            
        else:  # ALL technologies
            select_cols = """
                COALESCE(v.date, u.date) AS time,
                COALESCE(v.site_att, u.site_att) AS site_att,
                CAST(v.user_traffic_volte_e AS DOUBLE PRECISION) AS user_traffic_volte_e,
                CAST(v.user_traffic_volte_h AS DOUBLE PRECISION) AS user_traffic_volte_h,
                CAST(v.user_traffic_volte_n AS DOUBLE PRECISION) AS user_traffic_volte_n,
                CAST(v.user_traffic_volte_s AS DOUBLE PRECISION) AS user_traffic_volte_s,
                CAST(u.h3g_traffic_v_user_cs AS DOUBLE PRECISION) AS h3g_traffic_v_user_cs,
                CAST(u.e3g_traffic_v_user_cs AS DOUBLE PRECISION) AS e3g_traffic_v_user_cs,
                CAST(u.n3g_traffic_v_user_cs AS DOUBLE PRECISION) AS n3g_traffic_v_user_cs
            """
            data_join = """
                LEFT JOIN volte_cqi_vendor_daily v ON ns.att_name = v.site_att
                FULL OUTER JOIN umts_cqi_daily u ON v.date = u.date AND v.site_att = u.site_att
            """
            tech_condition = "1=1"  # No technology filter for ALL
        
        # Build date and vendor conditions
        conditions = [tech_condition]
        
        if min_date:
            if technology in ['3G', '4G']:
                conditions.append(f"date >= '{min_date}'")
            else:
                conditions.append(f"COALESCE(v.date, u.date) >= '{min_date}'")
                
        if max_date:
            if technology in ['3G', '4G']:
                conditions.append(f"date <= '{max_date}'")
            else:
                conditions.append(f"COALESCE(v.date, u.date) <= '{max_date}'")
        
        if vendor:
            vendor_map = {'huawei': 'h', 'ericsson': 'e', 'nokia': 'n', 'samsung': 's'}
            vendor_prefix = vendor_map.get(vendor.lower())
            if vendor_prefix and technology:
                if technology == '3G':
                    conditions.append(f"u.{vendor_prefix}3g_traffic_v_user_cs IS NOT NULL")
                elif technology == '4G':
                    conditions.append(f"v.user_traffic_volte_{vendor_prefix} IS NOT NULL")
        
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions != ["1=1"] else ""
        
        neighbor_voice_query = text(f"""
        WITH center_sites AS (
            SELECT latitude, longitude, att_name
            FROM public.master_node_total 
            WHERE att_name IN ({site_placeholders})
            AND latitude IS NOT NULL 
            AND longitude IS NOT NULL
        ),
        neighbor_sites AS (
            SELECT DISTINCT m.att_name
            FROM public.master_node_total m
            CROSS JOIN center_sites c
            WHERE m.att_name IS NOT NULL
            AND m.latitude IS NOT NULL 
            AND m.longitude IS NOT NULL
            AND m.att_name != c.att_name
            AND m.att_name NOT IN ({site_placeholders})
            AND ST_DWithin(
                ST_GeogFromText('POINT(' || c.longitude || ' ' || c.latitude || ')'),
                ST_GeogFromText('POINT(' || m.longitude || ' ' || m.latitude || ')'),
                {radius_meters}
            )
        )
        SELECT time,
               AVG(user_traffic_volte_e) AS user_traffic_volte_e,
               AVG(user_traffic_volte_h) AS user_traffic_volte_h,
               AVG(user_traffic_volte_n) AS user_traffic_volte_n,
               AVG(user_traffic_volte_s) AS user_traffic_volte_s,
               AVG(h3g_traffic_v_user_cs) AS h3g_traffic_v_user_cs,
               AVG(e3g_traffic_v_user_cs) AS e3g_traffic_v_user_cs,
               AVG(n3g_traffic_v_user_cs) AS n3g_traffic_v_user_cs,
               (
                 COALESCE(AVG(user_traffic_volte_e),0) + COALESCE(AVG(user_traffic_volte_h),0) + COALESCE(AVG(user_traffic_volte_n),0) + COALESCE(AVG(user_traffic_volte_s),0) +
                 COALESCE(AVG(h3g_traffic_v_user_cs),0) + COALESCE(AVG(e3g_traffic_v_user_cs),0) + COALESCE(AVG(n3g_traffic_v_user_cs),0)
               ) AS traffic_voice
        FROM (
            SELECT {select_cols}
            FROM neighbor_sites ns
            {data_join}
            {where_clause}
        ) s
        GROUP BY time
        ORDER BY time ASC
        """)
        
        result_df = pd.read_sql_query(neighbor_voice_query, engine)
        print(f"Retrieved neighbor voice traffic data for {len(site_list)} center sites: {result_df.shape[0]} records")
        return result_df
        
    except Exception as e:
        print(f"Error executing neighbor voice traffic query: {e}")
        return None
    finally:
        engine.dispose()

if __name__ == "__main__":
    site_att = 'DIFALO0001'
    
    print("Testing Neighbor sites:")
    neighbor_sites = get_neighbor_sites(site_att, radius_km=10)
    print(f"Single site neighbors: {neighbor_sites}")
    
    multiple_sites = ['DIFALO0001', 'DIFALO0002']
    neighbor_sites_multi = get_neighbor_sites(multiple_sites, radius_km=10)
    print(f"Multiple sites neighbors: {neighbor_sites_multi}")

    print("\nTesting Neighbor CQI data:")
    neighbor_cqi = get_neighbor_cqi_daily(site_att, min_date='2024-01-01', max_date='2024-12-31', technology='4G', radius_km=10)
    if neighbor_cqi is not None:
        print(f"Neighbor CQI data shape: {neighbor_cqi.shape}")
        print(neighbor_cqi.head())

    print("\nTesting Neighbor Traffic data:")
    neighbor_traffic = get_neighbor_traffic_data(site_att, min_date='2024-01-01', max_date='2024-12-31', technology='4G', radius_km=10)
    if neighbor_traffic is not None:
        print(f"Neighbor Traffic data shape: {neighbor_traffic.shape}")
        print(neighbor_traffic.head())

    print("\nTesting Neighbor Voice data:")
    neighbor_voice = get_neighbor_traffic_voice(site_att, min_date='2024-01-01', max_date='2024-12-31', technology='4G', radius_km=10)
    if neighbor_voice is not None:
        print(f"Neighbor Voice data shape: {neighbor_voice.shape}")
        print(neighbor_voice.head())