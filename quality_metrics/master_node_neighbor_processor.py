import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import os
import dotenv

# Load environment variables
dotenv.load_dotenv()
ROOT_DIRECTORY = os.getenv('ROOT_DIRECTORY')
POSTGRES_USERNAME = os.getenv('POSTGRES_USERNAME')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_DB = os.getenv('POSTGRES_DB')

def get_engine():
    connection_string = f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    return create_engine(connection_string)

def get_master_node_list_neighbor(site_list, radius_km=5):
    """
    For a given list of site names (att_name), return a dataframe with 
    unique neighbors within specified radius using PostGIS spatial functions.
    
    Args:
        site_list (list): List of att_name values to find neighbors for
        radius_km (float): Search radius in kilometers (default: 5)
        
    Returns:
        pandas.DataFrame: DataFrame with unique neighbors within specified radius
    """
    if not site_list:
        return pd.DataFrame()
    
    engine = get_engine()
    radius_meters = radius_km * 1000
    
    try:
        # Convert to list if it's not already and ensure it's properly formatted
        if isinstance(site_list, str):
            site_list = [site_list]
        
        query = """
        WITH target_sites AS (
            SELECT 
                att_name, 
                ST_Transform(ST_SetSRID(ST_Point(longitude, latitude), 4326), 3857) as geom
            FROM master_node_total 
            WHERE att_name = ANY(%(site_list)s)
            AND latitude IS NOT NULL 
            AND longitude IS NOT NULL
        )
        SELECT DISTINCT
            n.att_name,
            n.region,
            n.province, 
            n.municipality,
            n.vendor,
            n.latitude,
            n.longitude,
            ROUND(
                CAST(ST_Distance(
                    t.geom,
                    ST_Transform(ST_SetSRID(ST_Point(n.longitude, n.latitude), 4326), 3857)
                ) / 1000.0 AS numeric), 3
            ) as distance_km
        FROM target_sites t
        CROSS JOIN master_node_total n
        WHERE n.att_name != ALL(%(site_list)s)
        AND n.latitude IS NOT NULL 
        AND n.longitude IS NOT NULL
        AND ST_DWithin(
            t.geom,
            ST_Transform(ST_SetSRID(ST_Point(n.longitude, n.latitude), 4326), 3857),
            %(radius_meters)s
        )
        ORDER BY att_name, distance_km
        """
        
        # Pass the list directly, not as a tuple
        df = pd.read_sql(query, engine, params={
            "site_list": site_list,  # Pass as list, not tuple
            "radius_meters": radius_meters
        })
        
        print(f"Found {len(df)} unique neighbors within {radius_km}km")
        return df.reset_index(drop=True)
        
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error in get_master_node_list_neighbor: {e}")
        return pd.DataFrame()

def get_master_node_neighbor(site_att, radius_km=5):
    """
    For a single site name (att_name), return a dataframe with 
    unique neighbors within specified radius using PostGIS spatial functions.
    
    Args:
        site_att (str): Single att_name value to find neighbors for
        radius_km (float): Search radius in kilometers (default: 5)
        
    Returns:
        pandas.DataFrame: DataFrame with unique neighbors within specified radius
    """
    if not site_att or site_att.strip() == "":
        return pd.DataFrame()
    
    engine = get_engine()
    radius_meters = radius_km * 1000
    
    try:
        query = """
        WITH target_site AS (
            SELECT 
                att_name, 
                ST_Transform(ST_SetSRID(ST_Point(longitude, latitude), 4326), 3857) as geom
            FROM master_node_total 
            WHERE att_name = %(site_att)s
            AND latitude IS NOT NULL 
            AND longitude IS NOT NULL
        )
        SELECT DISTINCT
            n.att_name,
            n.region,
            n.province, 
            n.municipality,
            n.vendor,
            n.latitude,
            n.longitude,
            ROUND(
                CAST(ST_Distance(
                    t.geom,
                    ST_Transform(ST_SetSRID(ST_Point(n.longitude, n.latitude), 4326), 3857)
                ) / 1000.0 AS numeric), 3
            ) as distance_km
        FROM target_site t
        CROSS JOIN master_node_total n
        WHERE n.att_name != %(site_att)s
        AND n.latitude IS NOT NULL 
        AND n.longitude IS NOT NULL
        AND ST_DWithin(
            t.geom,
            ST_Transform(ST_SetSRID(ST_Point(n.longitude, n.latitude), 4326), 3857),
            %(radius_meters)s
        )
        ORDER BY distance_km, att_name
        """
        
        df = pd.read_sql(query, engine, params={
            "site_att": site_att,
            "radius_meters": radius_meters
        })
        
        print(f"Found {len(df)} unique neighbors within {radius_km}km for site {site_att}")
        return df.reset_index(drop=True)
        
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error in get_master_node_neighbor: {e}")
        return pd.DataFrame()

# Example usage
if __name__ == "__main__":
    # Test with a single site
    test_site = 'MEXMET0396'
    neighbors_single = get_master_node_neighbor(test_site, radius_km=5)
    
    if not neighbors_single.empty:
        print(f"\nNeighbors of {test_site} within 5km:")
        print(neighbors_single[['att_name', 'region', 'province', 'municipality', 'vendor', 'distance_km']])
    else:
        print(f"No neighbors found for {test_site}")
    
    print("\n" + "="*50)
    
    # Test with a list of sites
    test_sites = ['MEXMET0396', 'SITE002', 'SITE003']
    neighbors_list = get_master_node_list_neighbor(test_sites, radius_km=5)
    
    if not neighbors_list.empty:
        print(f"\nNeighbors of {test_sites} within 5km:")
        print(neighbors_list[['att_name', 'region', 'province', 'municipality', 'vendor', 'distance_km']])
    else:
        print(f"No neighbors found for {test_sites}")

def get_master_node_with_neighbors_grouped(site_att, radius=5, csv_export=True):
    """
    Get target site and neighbors data, group by region/province/municipality to avoid duplicates,
    and optionally export to CSV.
    
    Args:
        site_att (str): Target site name
        radius (float): Search radius in kilometers (default: 5)
        csv_export (bool): Whether to export results to CSV (default: True)
        
    Returns:
        pandas.DataFrame: Grouped DataFrame with unique region/province/municipality combinations
    """
    if not site_att or site_att.strip() == "":
        return pd.DataFrame()
    
    engine = get_engine()
    radius_meters = radius * 1000
    
    try:
        query = """
        WITH target_site AS (
            SELECT 
                att_name, 
                region,
                province,
                municipality,
                vendor,
                latitude,
                longitude,
                ST_Transform(ST_SetSRID(ST_Point(longitude, latitude), 4326), 3857) as geom
            FROM master_node_total 
            WHERE att_name = %(site_att)s
            AND latitude IS NOT NULL 
            AND longitude IS NOT NULL
        ),
        all_sites AS (
            -- Target site with distance 0
            SELECT 
                t.att_name,
                t.region,
                t.province,
                t.municipality,
                t.vendor,
                t.latitude,
                t.longitude,
                0.0 as distance_km,
                'TARGET' as site_type
            FROM target_site t
            
            UNION ALL
            
            -- Neighbor sites
            SELECT DISTINCT
                n.att_name,
                n.region,
                n.province,
                n.municipality,
                n.vendor,
                n.latitude,
                n.longitude,
                ROUND(
                    CAST(ST_Distance(
                        t.geom,
                        ST_Transform(ST_SetSRID(ST_Point(n.longitude, n.latitude), 4326), 3857)
                    ) / 1000.0 AS numeric), 3
                ) as distance_km,
                'NEIGHBOR' as site_type
            FROM target_site t
            CROSS JOIN master_node_total n
            WHERE n.att_name != %(site_att)s
            AND n.latitude IS NOT NULL 
            AND n.longitude IS NOT NULL
            AND ST_DWithin(
                t.geom,
                ST_Transform(ST_SetSRID(ST_Point(n.longitude, n.latitude), 4326), 3857),
                %(radius_meters)s
            )
        )
        SELECT 
            region,
            province,
            municipality,
            STRING_AGG(DISTINCT att_name, ', ' ORDER BY att_name) as nodes,
            STRING_AGG(DISTINCT vendor, ', ' ORDER BY vendor) as vendors,
            COUNT(*) as node_count,
            MIN(distance_km) as min_distance_km,
            MAX(distance_km) as max_distance_km,
            ROUND(AVG(distance_km)::numeric, 3) as avg_distance_km
        FROM all_sites
        GROUP BY region, province, municipality
        ORDER BY min_distance_km, region, province, municipality
        """
        
        df = pd.read_sql(query, engine, params={
            "site_att": site_att,
            "radius_meters": radius_meters
        })
        
        if df.empty:
            print(f"No data found for site: {site_att}")
            return pd.DataFrame()
        
        # Export to CSV if requested
        if csv_export:
            filename = f"master_node_grouped_{site_att}_radius_{radius}km.csv"
            df.to_csv(filename, index=False)
            print(f"Data exported to: {filename}")
        
        print(f"Found {len(df)} unique region/province/municipality combinations for site {site_att} within {radius}km")
        return df.reset_index(drop=True)
        
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error in get_master_node_with_neighbors_grouped: {e}")
        return pd.DataFrame()

def get_master_node_with_neighbors_detailed(site_att, radius=5):
    """
    Get target site and neighbors with detailed information for mapping and plotting.
    
    Args:
        site_att (str): Target site name
        radius (float): Search radius in kilometers (default: 5)
        
    Returns:
        pandas.DataFrame: DataFrame with all sites (target + neighbors) with detailed info
    """
    if not site_att or site_att.strip() == "":
        return pd.DataFrame()
    
    engine = get_engine()
    radius_meters = radius * 1000
    
    try:
        query = """
        WITH target_site AS (
            SELECT 
                att_name, 
                region,
                province,
                municipality,
                vendor,
                latitude,
                longitude,
                ST_Transform(ST_SetSRID(ST_Point(longitude, latitude), 4326), 3857) as geom
            FROM master_node_total 
            WHERE att_name = %(site_att)s
            AND latitude IS NOT NULL 
            AND longitude IS NOT NULL
        )
        -- Target site with distance 0
        SELECT 
            t.att_name,
            t.region,
            t.province,
            t.municipality,
            t.vendor,
            t.latitude,
            t.longitude,
            0.0 as distance_km,
            'TARGET' as site_type
        FROM target_site t
        
        UNION ALL
        
        -- Neighbor sites
        SELECT DISTINCT
            n.att_name,
            n.region,
            n.province,
            n.municipality,
            n.vendor,
            n.latitude,
            n.longitude,
            ROUND(
                CAST(ST_Distance(
                    t.geom,
                    ST_Transform(ST_SetSRID(ST_Point(n.longitude, n.latitude), 4326), 3857)
                ) / 1000.0 AS numeric), 3
            ) as distance_km,
            'NEIGHBOR' as site_type
        FROM target_site t
        CROSS JOIN master_node_total n
        WHERE n.att_name != %(site_att)s
        AND n.latitude IS NOT NULL 
        AND n.longitude IS NOT NULL
        AND ST_DWithin(
            t.geom,
            ST_Transform(ST_SetSRID(ST_Point(n.longitude, n.latitude), 4326), 3857),
            %(radius_meters)s
        )
        ORDER BY distance_km, att_name
        """
        
        df = pd.read_sql(query, engine, params={
            "site_att": site_att,
            "radius_meters": radius_meters
        })
        
        if df.empty:
            print(f"No data found for site: {site_att}")
            return pd.DataFrame()
        
        print(f"Found {len(df)} total sites (1 target + {len(df)-1} neighbors) for site {site_att} within {radius}km")
        return df.reset_index(drop=True)
        
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error in get_master_node_with_neighbors_detailed: {e}")
        return pd.DataFrame()
