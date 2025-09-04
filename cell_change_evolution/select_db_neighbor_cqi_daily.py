import os
import dotenv
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import create_engine, text
from cell_change_evolution.select_db_cqi_daily import (
    calculate_unified_cqi_umts_row,
    calculate_unified_cqi_lte_row,
    calculate_unified_cqi_nr_row,
    sanitize_df,
)

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

def get_neighbor_sites(site_list, radius_km=5, vecinos=""):
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
        radius_meters = radius_km * 1000
        params = {"sites": site_list, "radius_meters": radius_meters}
        if radius_km > 0.1:
            neighbor_query = text(
                """
                WITH center_sites AS (
                    SELECT latitude, longitude, att_name
                    FROM public.master_node_total 
                    WHERE att_name = ANY(:sites)
                      AND latitude IS NOT NULL 
                      AND longitude IS NOT NULL
                )
                SELECT DISTINCT m.att_name
                FROM public.master_node_total m
                CROSS JOIN center_sites c
                WHERE m.att_name IS NOT NULL
                  AND m.latitude IS NOT NULL 
                  AND m.longitude IS NOT NULL
                  AND m.att_name <> c.att_name
                  AND NOT (m.att_name = ANY(:sites))
                  AND ST_DWithin(
                        ST_SetSRID(ST_MakePoint(c.longitude, c.latitude), 4326)::geography,
                        ST_SetSRID(ST_MakePoint(m.longitude, m.latitude), 4326)::geography,
                        :radius_meters
                  )
                ORDER BY m.att_name
                """
            )
        else:
            vec_list = [v.strip() for v in (vecinos or '').split(',') if v.strip()]
            if not vec_list:
                return []
            params["vecinos"] = vec_list
            neighbor_query = text(
                """
                SELECT att_name
                FROM public.master_node_total 
                WHERE att_name = ANY(:vecinos)
                  AND latitude IS NOT NULL 
                  AND longitude IS NOT NULL
                ORDER BY att_name
                """
            )
        
        df = pd.read_sql_query(neighbor_query, engine, params=params)
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
        params = {"sites": site_list, "radius_meters": radius_km * 1000}

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

        neighbor_cqi_query = text(
            f"""
            WITH center_sites AS (
                SELECT latitude, longitude, att_name
                FROM public.master_node_total 
                WHERE att_name = ANY(:sites)
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
                  AND m.att_name <> c.att_name
                  AND NOT (m.att_name = ANY(:sites))
                  AND ST_DWithin(
                    ST_SetSRID(ST_MakePoint(c.longitude, c.latitude), 4326)::geography,
                    ST_SetSRID(ST_MakePoint(m.longitude, m.latitude), 4326)::geography,
                    :radius_meters
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
            """
        )

        result_df = pd.read_sql_query(neighbor_cqi_query, engine, params=params)
        print(f"Retrieved neighbor CQI data for {len(site_list)} center sites: {result_df.shape[0]} records")
        return result_df
        
    except Exception as e:
        print(f"Error executing neighbor CQI query: {e}")
        return None
    finally:
        engine.dispose()

def get_neighbor_traffic_data(site, min_date=None, max_date=None, technology=None, radius_km=5, vendor=None, vecinos=''):
    """Get traffic data for neighbor sites within radius using direct SQL, aggregated daily across neighbors.

    Adds aggregated columns:
      - ps_gb_uldl (GB): total PS traffic for 3G/4G
      - traffic_dlul_tb (TB): total PDCP traffic for 5G (converted from GB)
    """
    try:
        neighbors = get_neighbor_sites(site, radius_km=radius_km, vecinos=vecinos)
    except Exception:
        neighbors = []
    if not neighbors:
        return pd.DataFrame()

    engine = create_connection()
    if engine is None:
        return None
    
    try:
        params: dict = {
            'neighbors': neighbors,
        }

        # Optional vendor shortcuts
        vendor_map = {'huawei': 'h', 'ericsson': 'e', 'nokia': 'n', 'samsung': 's'}
        vendor_prefix = vendor_map.get(vendor.lower()) if isinstance(vendor, str) else None

        # Helper fragments for date filters per table
        def date_filter(alias: str) -> str:
            conds = []
            if min_date:
                params[f'min_{alias}'] = min_date
                conds.append(f"{alias}.date >= :min_{alias}")
            if max_date:
                params[f'max_{alias}'] = max_date
                conds.append(f"{alias}.date <= :max_{alias}")
            return (" AND ".join(conds)) if conds else ""

        if technology in ('3G', '4G', '5G'):
            # Single-table branch with INNER JOINs and targeted filters
            if technology == '3G':
                where_vendor = f" AND u.{vendor_prefix}3g_traffic_d_user_ps_gb IS NOT NULL" if vendor_prefix else ""
                dt = date_filter('u')
                neighbor_traffic_sql = f"""
                SELECT u.date AS time,
                       AVG(CAST(u.h3g_traffic_d_user_ps_gb AS DOUBLE PRECISION)) AS h3g_traffic_d_user_ps_gb,
                       AVG(CAST(u.e3g_traffic_d_user_ps_gb AS DOUBLE PRECISION)) AS e3g_traffic_d_user_ps_gb,
                       AVG(CAST(u.n3g_traffic_d_user_ps_gb AS DOUBLE PRECISION)) AS n3g_traffic_d_user_ps_gb,
                       NULL::DOUBLE PRECISION AS h4g_traffic_d_user_ps_gb,
                       NULL::DOUBLE PRECISION AS s4g_traffic_d_user_ps_gb,
                       NULL::DOUBLE PRECISION AS e4g_traffic_d_user_ps_gb,
                       NULL::DOUBLE PRECISION AS n4g_traffic_d_user_ps_gb,
                       NULL::DOUBLE PRECISION AS e5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
                       NULL::DOUBLE PRECISION AS n5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
                       NULL::DOUBLE PRECISION AS e5g_nsa_traffic_pdcp_gb_5gendc_5gleg,
                       NULL::DOUBLE PRECISION AS n5g_nsa_traffic_pdcp_gb_5gendc_5gleg
                FROM umts_cqi_daily u
                WHERE u.site_att = ANY(:neighbors)
                {('AND ' + dt) if dt else ''}
                {where_vendor}
                GROUP BY u.date
                ORDER BY u.date ASC
                """
            elif technology == '4G':
                where_vendor = f" AND l.{vendor_prefix}4g_traffic_d_user_ps_gb IS NOT NULL" if vendor_prefix else ""
                dt = date_filter('l')
                neighbor_traffic_sql = f"""
                SELECT l.date AS time,
                       NULL::DOUBLE PRECISION AS h3g_traffic_d_user_ps_gb,
                       NULL::DOUBLE PRECISION AS e3g_traffic_d_user_ps_gb,
                       NULL::DOUBLE PRECISION AS n3g_traffic_d_user_ps_gb,
                       AVG(CAST(l.h4g_traffic_d_user_ps_gb AS DOUBLE PRECISION)) AS h4g_traffic_d_user_ps_gb,
                       AVG(CAST(l.s4g_traffic_d_user_ps_gb AS DOUBLE PRECISION)) AS s4g_traffic_d_user_ps_gb,
                       AVG(CAST(l.e4g_traffic_d_user_ps_gb AS DOUBLE PRECISION)) AS e4g_traffic_d_user_ps_gb,
                       AVG(CAST(l.n4g_traffic_d_user_ps_gb AS DOUBLE PRECISION)) AS n4g_traffic_d_user_ps_gb,
                       NULL::DOUBLE PRECISION AS e5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
                       NULL::DOUBLE PRECISION AS n5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
                       NULL::DOUBLE PRECISION AS e5g_nsa_traffic_pdcp_gb_5gendc_5gleg,
                       NULL::DOUBLE PRECISION AS n5g_nsa_traffic_pdcp_gb_5gendc_5gleg
                FROM lte_cqi_daily l
                WHERE l.site_att = ANY(:neighbors)
                {('AND ' + dt) if dt else ''}
                {where_vendor}
                GROUP BY l.date
                ORDER BY l.date ASC
                """
            else:  # 5G
                dt = date_filter('n')
                neighbor_traffic_sql = f"""
                SELECT n.date AS time,
                       NULL::DOUBLE PRECISION AS h3g_traffic_d_user_ps_gb,
                       NULL::DOUBLE PRECISION AS e3g_traffic_d_user_ps_gb,
                       NULL::DOUBLE PRECISION AS n3g_traffic_d_user_ps_gb,
                       NULL::DOUBLE PRECISION AS h4g_traffic_d_user_ps_gb,
                       NULL::DOUBLE PRECISION AS s4g_traffic_d_user_ps_gb,
                       NULL::DOUBLE PRECISION AS e4g_traffic_d_user_ps_gb,
                       NULL::DOUBLE PRECISION AS n4g_traffic_d_user_ps_gb,
                       AVG(CAST(n.e5g_nsa_traffic_pdcp_gb_5gendc_4glegn AS DOUBLE PRECISION)) AS e5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
                       AVG(CAST(n.n5g_nsa_traffic_pdcp_gb_5gendc_4glegn AS DOUBLE PRECISION)) AS n5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
                       AVG(CAST(n.e5g_nsa_traffic_pdcp_gb_5gendc_5gleg AS DOUBLE PRECISION)) AS e5g_nsa_traffic_pdcp_gb_5gendc_5gleg,
                       AVG(CAST(n.n5g_nsa_traffic_pdcp_gb_5gendc_5gleg AS DOUBLE PRECISION)) AS n5g_nsa_traffic_pdcp_gb_5gendc_5gleg
                FROM nr_cqi_daily n
                WHERE n.site_att = ANY(:neighbors)
                {('AND ' + dt) if dt else ''}
                GROUP BY n.date
                ORDER BY n.date ASC
                """

            result_df = pd.read_sql_query(text(neighbor_traffic_sql), engine, params=params)
        else:
            # ALL technologies: UNION ALL three streams then aggregate once
            dt_u = date_filter('u')
            dt_l = date_filter('l')
            dt_n = date_filter('n')
            neighbor_traffic_sql = f"""
            SELECT time,
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
                   (
                     COALESCE(AVG(h3g_traffic_d_user_ps_gb),0) + COALESCE(AVG(e3g_traffic_d_user_ps_gb),0) + COALESCE(AVG(n3g_traffic_d_user_ps_gb),0) +
                     COALESCE(AVG(h4g_traffic_d_user_ps_gb),0) + COALESCE(AVG(s4g_traffic_d_user_ps_gb),0) + COALESCE(AVG(e4g_traffic_d_user_ps_gb),0) + COALESCE(AVG(n4g_traffic_d_user_ps_gb),0)
                   ) AS ps_gb_uldl,
                   (
                     COALESCE(AVG(e5g_nsa_traffic_pdcp_gb_5gendc_4glegn),0) + COALESCE(AVG(n5g_nsa_traffic_pdcp_gb_5gendc_4glegn),0) +
                     COALESCE(AVG(e5g_nsa_traffic_pdcp_gb_5gendc_5gleg),0) + COALESCE(AVG(n5g_nsa_traffic_pdcp_gb_5gendc_5gleg),0)
                   ) / 1024.0 AS traffic_dlul_tb
            FROM (
              SELECT u.date AS time,
                     CAST(u.h3g_traffic_d_user_ps_gb AS DOUBLE PRECISION) AS h3g_traffic_d_user_ps_gb,
                     CAST(u.e3g_traffic_d_user_ps_gb AS DOUBLE PRECISION) AS e3g_traffic_d_user_ps_gb,
                     CAST(u.n3g_traffic_d_user_ps_gb AS DOUBLE PRECISION) AS n3g_traffic_d_user_ps_gb,
                     NULL::DOUBLE PRECISION AS h4g_traffic_d_user_ps_gb,
                     NULL::DOUBLE PRECISION AS s4g_traffic_d_user_ps_gb,
                     NULL::DOUBLE PRECISION AS e4g_traffic_d_user_ps_gb,
                     NULL::DOUBLE PRECISION AS n4g_traffic_d_user_ps_gb,
                     NULL::DOUBLE PRECISION AS e5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
                     NULL::DOUBLE PRECISION AS n5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
                     NULL::DOUBLE PRECISION AS e5g_nsa_traffic_pdcp_gb_5gendc_5gleg,
                     NULL::DOUBLE PRECISION AS n5g_nsa_traffic_pdcp_gb_5gendc_5gleg
              FROM umts_cqi_daily u
              WHERE u.site_att = ANY(:neighbors)
              {('AND ' + dt_u) if dt_u else ''}

              UNION ALL

              SELECT l.date AS time,
                     NULL::DOUBLE PRECISION AS h3g_traffic_d_user_ps_gb,
                     NULL::DOUBLE PRECISION AS e3g_traffic_d_user_ps_gb,
                     NULL::DOUBLE PRECISION AS n3g_traffic_d_user_ps_gb,
                     CAST(l.h4g_traffic_d_user_ps_gb AS DOUBLE PRECISION) AS h4g_traffic_d_user_ps_gb,
                     CAST(l.s4g_traffic_d_user_ps_gb AS DOUBLE PRECISION) AS s4g_traffic_d_user_ps_gb,
                     CAST(l.e4g_traffic_d_user_ps_gb AS DOUBLE PRECISION) AS e4g_traffic_d_user_ps_gb,
                     CAST(l.n4g_traffic_d_user_ps_gb AS DOUBLE PRECISION) AS n4g_traffic_d_user_ps_gb,
                     NULL::DOUBLE PRECISION AS e5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
                     NULL::DOUBLE PRECISION AS n5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
                     NULL::DOUBLE PRECISION AS e5g_nsa_traffic_pdcp_gb_5gendc_5gleg,
                     NULL::DOUBLE PRECISION AS n5g_nsa_traffic_pdcp_gb_5gendc_5gleg
              FROM lte_cqi_daily l
              WHERE l.site_att = ANY(:neighbors)
              {('AND ' + dt_l) if dt_l else ''}

              UNION ALL

              SELECT n.date AS time,
                     NULL::DOUBLE PRECISION AS h3g_traffic_d_user_ps_gb,
                     NULL::DOUBLE PRECISION AS e3g_traffic_d_user_ps_gb,
                     NULL::DOUBLE PRECISION AS n3g_traffic_d_user_ps_gb,
                     NULL::DOUBLE PRECISION AS h4g_traffic_d_user_ps_gb,
                     NULL::DOUBLE PRECISION AS s4g_traffic_d_user_ps_gb,
                     NULL::DOUBLE PRECISION AS e4g_traffic_d_user_ps_gb,
                     NULL::DOUBLE PRECISION AS n4g_traffic_d_user_ps_gb,
                     CAST(n.e5g_nsa_traffic_pdcp_gb_5gendc_4glegn AS DOUBLE PRECISION) AS e5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
                     CAST(n.n5g_nsa_traffic_pdcp_gb_5gendc_4glegn AS DOUBLE PRECISION) AS n5g_nsa_traffic_pdcp_gb_5gendc_4glegn,
                     CAST(n.e5g_nsa_traffic_pdcp_gb_5gendc_5gleg AS DOUBLE PRECISION) AS e5g_nsa_traffic_pdcp_gb_5gendc_5gleg,
                     CAST(n.n5g_nsa_traffic_pdcp_gb_5gendc_5gleg AS DOUBLE PRECISION) AS n5g_nsa_traffic_pdcp_gb_5gendc_5gleg
              FROM nr_cqi_daily n
              WHERE n.site_att = ANY(:neighbors)
              {('AND ' + dt_n) if dt_n else ''}
            ) s
            GROUP BY time
            ORDER BY time ASC
            """

            result_df = pd.read_sql_query(text(neighbor_traffic_sql), engine, params=params)
        print(f"Retrieved neighbor traffic data for {len(neighbors)} neighbor sites: {result_df.shape[0]} records")
        return result_df
        
    except Exception as e:
        print(f"Error executing neighbor traffic query: {e}")
        return None
    finally:
        engine.dispose()

def get_neighbor_traffic_voice(site, min_date=None, max_date=None, technology=None, radius_km=5, vendor=None, vecinos=''):
    """Get voice traffic data for neighbor sites within radius using direct SQL, aggregated daily across neighbors.

    Adds aggregated column:
      - traffic_voice: total voice traffic across technologies/vendors.
    """
    try:
        neighbors = get_neighbor_sites(site, radius_km=radius_km, vecinos=vecinos)
    except Exception:
        neighbors = []
    if not neighbors:
        return pd.DataFrame()

    engine = create_connection()
    if engine is None:
        return None
    
    try:
        params: dict = {
            'neighbors': neighbors,
        }

        # Optional vendor mapping
        vendor_map = {'huawei': 'h', 'ericsson': 'e', 'nokia': 'n', 'samsung': 's'}
        vendor_prefix = vendor_map.get(vendor.lower()) if isinstance(vendor, str) else None

        # Helper for date filters by table alias
        def date_filter(alias: str) -> str:
            conds = []
            if min_date:
                params[f'min_{alias}'] = min_date
                conds.append(f"{alias}.date >= :min_{alias}")
            if max_date:
                params[f'max_{alias}'] = max_date
                conds.append(f"{alias}.date <= :max_{alias}")
            return (" AND ".join(conds)) if conds else ""

        if technology in ('3G', '4G'):
            # Single-tech optimized path
            if technology == '3G':
                where_vendor = f" AND u.{vendor_prefix}3g_traffic_v_user_cs IS NOT NULL" if vendor_prefix else ""
                dt = date_filter('u')
                sql = f"""
                SELECT u.date AS time,
                       NULL::DOUBLE PRECISION AS user_traffic_volte_e,
                       NULL::DOUBLE PRECISION AS user_traffic_volte_h,
                       NULL::DOUBLE PRECISION AS user_traffic_volte_n,
                       NULL::DOUBLE PRECISION AS user_traffic_volte_s,
                       AVG(CAST(u.h3g_traffic_v_user_cs AS DOUBLE PRECISION)) AS h3g_traffic_v_user_cs,
                       AVG(CAST(u.e3g_traffic_v_user_cs AS DOUBLE PRECISION)) AS e3g_traffic_v_user_cs,
                       AVG(CAST(u.n3g_traffic_v_user_cs AS DOUBLE PRECISION)) AS n3g_traffic_v_user_cs
                FROM umts_cqi_daily u
                WHERE u.site_att = ANY(:neighbors)
                {('AND ' + dt) if dt else ''}
                {where_vendor}
                GROUP BY u.date
                ORDER BY u.date ASC
                """
            else:  # 4G
                where_vendor = f" AND v.user_traffic_volte_{vendor_prefix} IS NOT NULL" if vendor_prefix else ""
                dt = date_filter('v')
                sql = f"""
                SELECT v.date AS time,
                       AVG(CAST(v.user_traffic_volte_e AS DOUBLE PRECISION)) AS user_traffic_volte_e,
                       AVG(CAST(v.user_traffic_volte_h AS DOUBLE PRECISION)) AS user_traffic_volte_h,
                       AVG(CAST(v.user_traffic_volte_n AS DOUBLE PRECISION)) AS user_traffic_volte_n,
                       AVG(CAST(v.user_traffic_volte_s AS DOUBLE PRECISION)) AS user_traffic_volte_s,
                       NULL::DOUBLE PRECISION AS h3g_traffic_v_user_cs,
                       NULL::DOUBLE PRECISION AS e3g_traffic_v_user_cs,
                       NULL::DOUBLE PRECISION AS n3g_traffic_v_user_cs
                FROM volte_cqi_vendor_daily v
                WHERE v.site_att = ANY(:neighbors)
                {('AND ' + dt) if dt else ''}
                {where_vendor}
                GROUP BY v.date
                ORDER BY v.date ASC
                """

            result_df = pd.read_sql_query(text(sql), engine, params=params)
        else:
            # ALL technologies: UNION ALL per-table selects with date pushdown
            dt_u = date_filter('u')
            dt_v = date_filter('v')
            sql = f"""
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
                SELECT u.date AS time,
                       NULL::DOUBLE PRECISION AS user_traffic_volte_e,
                       NULL::DOUBLE PRECISION AS user_traffic_volte_h,
                       NULL::DOUBLE PRECISION AS user_traffic_volte_n,
                       NULL::DOUBLE PRECISION AS user_traffic_volte_s,
                       CAST(u.h3g_traffic_v_user_cs AS DOUBLE PRECISION) AS h3g_traffic_v_user_cs,
                       CAST(u.e3g_traffic_v_user_cs AS DOUBLE PRECISION) AS e3g_traffic_v_user_cs,
                       CAST(u.n3g_traffic_v_user_cs AS DOUBLE PRECISION) AS n3g_traffic_v_user_cs
                FROM umts_cqi_daily u
                WHERE u.site_att = ANY(:neighbors)
                {('AND ' + dt_u) if dt_u else ''}

                UNION ALL

                SELECT v.date AS time,
                       CAST(v.user_traffic_volte_e AS DOUBLE PRECISION) AS user_traffic_volte_e,
                       CAST(v.user_traffic_volte_h AS DOUBLE PRECISION) AS user_traffic_volte_h,
                       CAST(v.user_traffic_volte_n AS DOUBLE PRECISION) AS user_traffic_volte_n,
                       CAST(v.user_traffic_volte_s AS DOUBLE PRECISION) AS user_traffic_volte_s,
                       NULL::DOUBLE PRECISION AS h3g_traffic_v_user_cs,
                       NULL::DOUBLE PRECISION AS e3g_traffic_v_user_cs,
                       NULL::DOUBLE PRECISION AS n3g_traffic_v_user_cs
                FROM volte_cqi_vendor_daily v
                WHERE v.site_att = ANY(:neighbors)
                {('AND ' + dt_v) if dt_v else ''}
            ) s
            GROUP BY time
            ORDER BY time ASC
            """

            result_df = pd.read_sql_query(text(sql), engine, params=params)
        print(f"Retrieved neighbor voice traffic data for {len(neighbors)} neighbor sites: {result_df.shape[0]} records")
        return result_df
        
    except Exception as e:
        print(f"Error executing neighbor voice traffic query: {e}")
        return None
    finally:
        engine.dispose()

def _neighbor_base_cte() -> str:
    return (
        """
        WITH center_sites AS (
            SELECT att_name, longitude, latitude
            FROM public.master_node_total
            WHERE att_name = ANY(:sites)
              AND latitude IS NOT NULL
              AND longitude IS NOT NULL
        ),
        neighbor_sites AS (
            SELECT DISTINCT m.att_name
            FROM public.master_node_total m
            JOIN center_sites c ON TRUE
            WHERE m.att_name IS NOT NULL
              AND m.latitude IS NOT NULL
              AND m.longitude IS NOT NULL
              AND m.att_name <> c.att_name
              AND NOT (m.att_name = ANY(:sites))
              AND ST_DWithin(
                    ST_SetSRID(ST_MakePoint(c.longitude, c.latitude), 4326)::geography,
                    ST_SetSRID(ST_MakePoint(m.longitude, m.latitude), 4326)::geography,
                    :radius_meters
              )
        )
        """
    )

def _date_filter_params(min_date, max_date, alias: str, params: dict) -> str:
    conds = []
    if min_date:
        params[f"min_{alias}"] = min_date
        conds.append(f"{alias}.date >= :min_{alias}")
    if max_date:
        params[f"max_{alias}"] = max_date
        conds.append(f"{alias}.date <= :max_{alias}")
    return (" AND ".join(conds)) if conds else ""

def get_neighbor_umts_cqi_daily_calculated(site, min_date=None, max_date=None, radius_km=5, vecinos='', neighbors=None):
    """Compute UMTS (3G) unified CQI for neighbors per day using row-based formulas.

    Input is a center site name; neighbors are derived via get_neighbor_sites(). The center
    site is excluded. Returns columns [time, umts_cqi].
    """
    try:
        if neighbors is None:
            neighbors = get_neighbor_sites(site, radius_km=radius_km, vecinos=vecinos)
    except Exception:
        neighbors = []
    if not neighbors:
        return pd.DataFrame(columns=["time", "umts_cqi"])

    engine = create_connection()
    if engine is None:
        return None
    try:
        params = {"neighbors": neighbors}
        dt = _date_filter_params(min_date, max_date, 'u', params)
        sql = f"""
        SELECT
          u.date AS time,
          -- Only required fields for calculate_unified_cqi_umts_row
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
        WHERE u.site_att = ANY(:neighbors)
        {('AND ' + dt) if dt else ''}
        ORDER BY u.date ASC, u.site_att ASC
        """
        print("umts_cqi_daily start")
        df = pd.read_sql_query(text(sql), engine, params=params)
        if df is None or df.empty:
            # Ensure expected columns even when no rows
            return pd.DataFrame(columns=["time", "umts_cqi"]) 
        print("umts_cqi_daily end")
        df['umts_cqi'] = df.apply(calculate_unified_cqi_umts_row, axis=1)
        out = df.groupby('time', as_index=False)['umts_cqi'].mean()
        print("umts_cqi_daily calculated end")

        return sanitize_df(out)
    except Exception as e:
        print(f"Error computing neighbor UMTS unified CQI: {e}")
        return None
    finally:
        engine.dispose()

def get_neighbor_lte_cqi_daily_calculated(site, min_date=None, max_date=None, radius_km=5, vecinos='', neighbors=None):
    """Compute LTE (4G) unified CQI for neighbors per day using row-based formulas.

    Input is a center site name; neighbors are derived via get_neighbor_sites(). The center
    site is excluded. Returns columns [time, lte_cqi].
    """
    try:
        if neighbors is None:
            neighbors = get_neighbor_sites(site, radius_km=radius_km, vecinos=vecinos)
    except Exception:
        neighbors = []
    if not neighbors:
        return pd.DataFrame(columns=["time", "lte_cqi"])

    engine = create_connection()
    if engine is None:
        return None
    try:
        params = {"neighbors": neighbors}
        dt = _date_filter_params(min_date, max_date, 'l', params)
        sql = f"""
        SELECT
          l.date AS time,
          -- Only required fields for calculate_unified_cqi_lte_row
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
        WHERE l.site_att = ANY(:neighbors)
        {('AND ' + dt) if dt else ''}
        ORDER BY l.date ASC, l.site_att ASC
        """
        print("umts_cqi_daily start")

        df = pd.read_sql_query(text(sql), engine, params=params)
        if df is None or df.empty:
            # Ensure expected columns even when no rows
            return pd.DataFrame(columns=["time", "lte_cqi"]) 
        print("lte_cqi_daily end")
        df['lte_cqi'] = df.apply(calculate_unified_cqi_lte_row, axis=1)
        out = df.groupby('time', as_index=False)['lte_cqi'].mean()
        print("lte_cqi_daily calculated end")
        return sanitize_df(out)
    except Exception as e:
        print(f"Error computing neighbor LTE unified CQI: {e}")
        return None
    finally:
        engine.dispose()

def get_neighbor_nr_cqi_daily_calculated(site, min_date=None, max_date=None, radius_km=5, vecinos='', neighbors=None):
    """Compute NR (5G) unified CQI for neighbors per day using row-based formulas.

    Input is a center site name; neighbors are derived via get_neighbor_sites(). The center
    site is excluded. Returns columns [time, nr_cqi].
    """
    try:
        if neighbors is None:
            neighbors = get_neighbor_sites(site, radius_km=radius_km, vecinos=vecinos)
    except Exception:
        neighbors = []
    if not neighbors:
        return pd.DataFrame(columns=["time", "nr_cqi"])

    engine = create_connection()
    if engine is None:
        return None
    try:
        params = {"neighbors": neighbors}
        dt = _date_filter_params(min_date, max_date, 'n', params)
        sql = f"""
        SELECT
          n.date AS time,
          -- Only required fields for calculate_unified_cqi_nr_row
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
        WHERE n.site_att = ANY(:neighbors)
        {('AND ' + dt) if dt else ''}
        ORDER BY n.date ASC, n.site_att ASC
        """
        print("nr_cqi_daily start")
        df = pd.read_sql_query(text(sql), engine, params=params)
        if df is None or df.empty:
            # Ensure expected columns even when no rows
            return pd.DataFrame(columns=["time", "nr_cqi"]) 
        print("nr_cqi_daily end")
        df['nr_cqi'] = df.apply(calculate_unified_cqi_nr_row, axis=1)
        out = df.groupby('time', as_index=False)['nr_cqi'].mean()
        print("nr_cqi_daily calculated end")
        return sanitize_df(out)
    except Exception as e:
        print(f"Error computing neighbor NR unified CQI: {e}")
        return None
    finally:
        engine.dispose()

def get_neighbor_cqi_daily_calculated(site, min_date=None, max_date=None, technology=None, radius_km=5, vecinos=''):
    """Neighbor version of calculated CQI.

    Accepts a center site name (str) or list of centers. Internally, the per-technology
    functions will build the neighbor set using geospatial criteria and compute CQI
    from detailed counters (via row-based functions), excluding the input center site(s).

    - If technology in ('3G','4G','5G'), return [time, <tech>_cqi]
    - If technology is None, merge three techs on time: [time, lte_cqi, nr_cqi, umts_cqi]
    """
    # Precompute neighbors once and reuse
    try:
        neighbors = get_neighbor_sites(site, radius_km=radius_km, vecinos=vecinos)
    except Exception as e:
        print(f"Error computing neighbors: {e}")
        neighbors = []

    if technology == '3G':
        return get_neighbor_umts_cqi_daily_calculated(site, min_date=min_date, max_date=max_date, radius_km=radius_km, vecinos=vecinos, neighbors=neighbors)
    if technology == '4G':
        return get_neighbor_lte_cqi_daily_calculated(site, min_date=min_date, max_date=max_date, radius_km=radius_km, vecinos=vecinos, neighbors=neighbors)
    if technology == '5G':
        return get_neighbor_nr_cqi_daily_calculated(site, min_date=min_date, max_date=max_date, radius_km=radius_km, vecinos=vecinos, neighbors=neighbors)

    # Run three tech calculations in parallel
    with ThreadPoolExecutor(max_workers=3) as ex:
        f_umts = ex.submit(get_neighbor_umts_cqi_daily_calculated, site, min_date, max_date, radius_km, vecinos=vecinos, neighbors=neighbors)
        f_lte  = ex.submit(get_neighbor_lte_cqi_daily_calculated, site, min_date, max_date, radius_km, vecinos=vecinos, neighbors=neighbors)
        f_nr   = ex.submit(get_neighbor_nr_cqi_daily_calculated, site, min_date, max_date, radius_km, vecinos=vecinos, neighbors=neighbors)
        try:
            df3 = f_umts.result()
        except Exception as e:
            print(f"UMTS neighbor CQI parallel error: {e}")
            df3 = None
        try:
            df4 = f_lte.result()
        except Exception as e:
            print(f"LTE neighbor CQI parallel error: {e}")
            df4 = None
        try:
            df5 = f_nr.result()
        except Exception as e:
            print(f"NR neighbor CQI parallel error: {e}")
            df5 = None

    if df3 is None and df4 is None and df5 is None:
        return None
    if df3 is None:
        df3 = pd.DataFrame(columns=['time', 'umts_cqi'])
    if df4 is None:
        df4 = pd.DataFrame(columns=['time', 'lte_cqi'])
    if df5 is None:
        df5 = pd.DataFrame(columns=['time', 'nr_cqi'])

    out = pd.merge(df4, df5, on=['time'], how='outer')
    out = pd.merge(out, df3, on=['time'], how='outer')
    out = out.sort_values(by=['time'])
    out = out[['time', 'lte_cqi', 'nr_cqi', 'umts_cqi']]
    return sanitize_df(out)

if __name__ == "__main__":
    site_att = 'DIFALO0001'
    vecinos = ''
    
    print("Testing Neighbor sites:")
    neighbor_sites = get_neighbor_sites(site_att, radius_km=10, vecinos=vecinos)
    print(f"Single site neighbors: {neighbor_sites}")
    
    multiple_sites = ['DIFALO0001', 'DIFALO0002']
    neighbor_sites_multi = get_neighbor_sites(multiple_sites, radius_km=10, vecinos=vecinos)
    print(f"Multiple sites neighbors: {neighbor_sites_multi}")

    print("\nTesting Neighbor CQI data:")
    neighbor_cqi = get_neighbor_cqi_daily(site_att, min_date='2024-01-01', max_date='2024-12-31', technology='4G', radius_km=10, vecinos=vecinos)
    if neighbor_cqi is not None:
        print(f"Neighbor CQI data shape: {neighbor_cqi.shape}")
        print(neighbor_cqi.head())

    print("\nTesting Neighbor Traffic data:")
    neighbor_traffic = get_neighbor_traffic_data(site_att, min_date='2024-01-01', max_date='2024-12-31', technology='4G', radius_km=10, vecinos=vecinos)
    if neighbor_traffic is not None:
        print(f"Neighbor Traffic data shape: {neighbor_traffic.shape}")
        print(neighbor_traffic.head())

    print("\nTesting Neighbor Voice data:")
    neighbor_voice = get_neighbor_traffic_voice(site_att, min_date='2024-01-01', max_date='2024-12-31', technology='4G', radius_km=10, vecinos=vecinos)
    if neighbor_voice is not None:
        print(f"Neighbor Voice data shape: {neighbor_voice.shape}")
        print(neighbor_voice.head())
