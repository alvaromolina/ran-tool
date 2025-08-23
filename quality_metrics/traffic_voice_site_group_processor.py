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
        if not var_value:
            raise ValueError(f"Environment variable {var_name} is not set")
    
    try:
        port = int(POSTGRES_PORT)
    except (ValueError, TypeError) as e:
        raise ValueError(f"POSTGRES_PORT must be a valid integer: {e}")
    
    connection_string = f"postgresql+psycopg2://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    return create_engine(connection_string)


def get_neighbor_sites_within_radius(site_att, radius_km=5.0, engine=None):
    """
    Get all sites within specified radius of the target site using master_node_total table.
    
    Args:
        site_att (str): Target site name
        radius_km (float): Radius in kilometers (default 5.0)
        engine: SQLAlchemy engine (if None, will create new one)
        
    Returns:
        list: List of site names within the radius (including the target site)
    """
    if engine is None:
        engine = get_engine()
    
    query = """
        SELECT DISTINCT neighbor.att_name
        FROM master_node_total target
        JOIN master_node_total neighbor ON ST_DWithin(
            target.geom::geography, 
            neighbor.geom::geography, 
            %(radius_m)s
        )
        WHERE target.att_name = %(site_att)s
        ORDER BY neighbor.att_name
    """
    
    params = {
        "site_att": site_att,
        "radius_m": radius_km * 1000  # Convert km to meters
    }
    
    try:
        result_df = pd.read_sql_query(query, engine, params=params)
        site_list = result_df['att_name'].tolist()
        print(f"Found {len(site_list)} sites within {radius_km}km of {site_att}")
        return site_list
    except Exception as e:
        print(f"Error getting neighbor sites: {e}")
        return [site_att]  # Return at least the target site


def get_daily_traffic_voice_for_site(site_att, min_date, max_date, engine):
    """
    Get daily traffic voice data for a single site from both UMTS and VoLTE tables.
    
    Args:
        site_att (str): Site name to retrieve data for
        min_date (str): Start date in 'YYYY-MM-DD' format
        max_date (str): End date in 'YYYY-MM-DD' format
        engine: SQLAlchemy engine
        
    Returns:
        pandas.DataFrame: Daily traffic voice data for the site
    """
    # Query UMTS traffic voice data
    umts_query = """
        SELECT 
            date,
            site_att,
            COALESCE(h3g_traffic_v_user_cs, 0) AS h3g_traffic_v_user_cs,
            COALESCE(e3g_traffic_v_user_cs, 0) AS e3g_traffic_v_user_cs,
            COALESCE(n3g_traffic_v_user_cs, 0) AS n3g_traffic_v_user_cs
        FROM public.umts_cqi_daily
        WHERE site_att = %(site_att)s
        AND date >= %(min_date)s 
        AND date <= %(max_date)s
        ORDER BY date
    """
    
    # Query VoLTE traffic voice data
    volte_query = """
        SELECT 
            date,
            site_att,
            COALESCE(user_traffic_volte_h, 0) AS user_traffic_volte_h,
            COALESCE(user_traffic_volte_e, 0) AS user_traffic_volte_e,
            COALESCE(user_traffic_volte_n, 0) AS user_traffic_volte_n,
            COALESCE(user_traffic_volte_s, 0) AS user_traffic_volte_s
        FROM public.volte_cqi_vendor_daily
        WHERE site_att = %(site_att)s
        AND date >= %(min_date)s 
        AND date <= %(max_date)s
        ORDER BY date
    """
    
    params = {
        "site_att": site_att,
        "min_date": min_date,
        "max_date": max_date
    }
    
    # Execute queries
    umts_df = pd.read_sql_query(umts_query, engine, params=params)
    volte_df = pd.read_sql_query(volte_query, engine, params=params)
    
    # Merge the data on date and site_att
    if not umts_df.empty and not volte_df.empty:
        combined_df = pd.merge(umts_df, volte_df, on=['date', 'site_att'], how='outer')
    elif not umts_df.empty:
        combined_df = umts_df.copy()
        # Add VoLTE columns with zeros
        for col in ['user_traffic_volte_h', 'user_traffic_volte_e', 'user_traffic_volte_n', 'user_traffic_volte_s']:
            combined_df[col] = 0.0
    elif not volte_df.empty:
        combined_df = volte_df.copy()
        # Add UMTS columns with zeros
        for col in ['h3g_traffic_v_user_cs', 'e3g_traffic_v_user_cs', 'n3g_traffic_v_user_cs']:
            combined_df[col] = 0.0
    else:
        return pd.DataFrame()
    
    # Fill NaN values with 0
    traffic_columns = ['h3g_traffic_v_user_cs', 'e3g_traffic_v_user_cs', 'n3g_traffic_v_user_cs',
                      'user_traffic_volte_h', 'user_traffic_volte_e', 'user_traffic_volte_n', 'user_traffic_volte_s']
    
    for col in traffic_columns:
        if col in combined_df.columns:
            combined_df[col] = combined_df[col].fillna(0)
        else:
            combined_df[col] = 0.0
    
    # Calculate total traffic voice
    combined_df['traffic_voice'] = (
        combined_df['h3g_traffic_v_user_cs'] + 
        combined_df['e3g_traffic_v_user_cs'] + 
        combined_df['n3g_traffic_v_user_cs'] +
        combined_df['user_traffic_volte_h'] + 
        combined_df['user_traffic_volte_e'] + 
        combined_df['user_traffic_volte_n'] + 
        combined_df['user_traffic_volte_s']
    )
    
    # Round to 4 decimal places
    for col in traffic_columns + ['traffic_voice']:
        combined_df[col] = combined_df[col].round(4)
    
    return combined_df.sort_values('date').reset_index(drop=True)


def get_daily_traffic_voice_for_group(site_list, min_date, max_date, engine):
    """
    Get daily aggregated traffic voice data for a group of sites.
    Returns one row per date with aggregated traffic for all sites in the group.
    
    Args:
        site_list (list): List of site_att values to include in the group
        min_date (str): Start date in 'YYYY-MM-DD' format
        max_date (str): End date in 'YYYY-MM-DD' format
        engine: SQLAlchemy engine
        
    Returns:
        pandas.DataFrame: Daily aggregated traffic voice data for the site group
    """
    if not site_list:
        print("Error: site_list cannot be empty")
        return pd.DataFrame()
    
    # Create placeholders for the site list
    site_placeholders = ', '.join([f"%(site_{i})s" for i in range(len(site_list))])
    
    # Query UMTS aggregated traffic voice data
    umts_query = f"""
        SELECT 
            date,
            SUM(COALESCE(h3g_traffic_v_user_cs, 0)) AS h3g_traffic_v_user_cs,
            SUM(COALESCE(e3g_traffic_v_user_cs, 0)) AS e3g_traffic_v_user_cs,
            SUM(COALESCE(n3g_traffic_v_user_cs, 0)) AS n3g_traffic_v_user_cs
        FROM public.umts_cqi_daily
        WHERE site_att IN ({site_placeholders})
        AND date >= %(min_date)s 
        AND date <= %(max_date)s
        GROUP BY date
        ORDER BY date
    """
    
    # Query VoLTE aggregated traffic voice data
    volte_query = f"""
        SELECT 
            date,
            SUM(COALESCE(user_traffic_volte_h, 0)) AS user_traffic_volte_h,
            SUM(COALESCE(user_traffic_volte_e, 0)) AS user_traffic_volte_e,
            SUM(COALESCE(user_traffic_volte_n, 0)) AS user_traffic_volte_n,
            SUM(COALESCE(user_traffic_volte_s, 0)) AS user_traffic_volte_s
        FROM public.volte_cqi_vendor_daily
        WHERE site_att IN ({site_placeholders})
        AND date >= %(min_date)s 
        AND date <= %(max_date)s
        GROUP BY date
        ORDER BY date
    """
    
    # Build parameters dictionary
    params = {"min_date": min_date, "max_date": max_date}
    for i, site in enumerate(site_list):
        params[f"site_{i}"] = site
    
    # Execute queries
    umts_df = pd.read_sql_query(umts_query, engine, params=params)
    volte_df = pd.read_sql_query(volte_query, engine, params=params)
    
    # Merge the data on date
    if not umts_df.empty and not volte_df.empty:
        combined_df = pd.merge(umts_df, volte_df, on='date', how='outer')
    elif not umts_df.empty:
        combined_df = umts_df.copy()
        # Add VoLTE columns with zeros
        for col in ['user_traffic_volte_h', 'user_traffic_volte_e', 'user_traffic_volte_n', 'user_traffic_volte_s']:
            combined_df[col] = 0.0
    elif not volte_df.empty:
        combined_df = volte_df.copy()
        # Add UMTS columns with zeros
        for col in ['h3g_traffic_v_user_cs', 'e3g_traffic_v_user_cs', 'n3g_traffic_v_user_cs']:
            combined_df[col] = 0.0
    else:
        return pd.DataFrame()
    
    # Fill NaN values with 0
    traffic_columns = ['h3g_traffic_v_user_cs', 'e3g_traffic_v_user_cs', 'n3g_traffic_v_user_cs',
                      'user_traffic_volte_h', 'user_traffic_volte_e', 'user_traffic_volte_n', 'user_traffic_volte_s']
    
    for col in traffic_columns:
        if col in combined_df.columns:
            combined_df[col] = combined_df[col].fillna(0)
        else:
            combined_df[col] = 0.0
    
    # Calculate total traffic voice
    combined_df['traffic_voice'] = (
        combined_df['h3g_traffic_v_user_cs'] + 
        combined_df['e3g_traffic_v_user_cs'] + 
        combined_df['n3g_traffic_v_user_cs'] +
        combined_df['user_traffic_volte_h'] + 
        combined_df['user_traffic_volte_e'] + 
        combined_df['user_traffic_volte_n'] + 
        combined_df['user_traffic_volte_s']
    )
    
    # Round to 4 decimal places
    for col in traffic_columns + ['traffic_voice']:
        combined_df[col] = combined_df[col].round(4)
    
    return combined_df.sort_values('date').reset_index(drop=True)


def get_traffic_voice_for_site_and_neighbors(site_att, date_range, radius_km=5.0, csv_export=False):
    """
    Get traffic voice data for a site and its neighbors within specified radius.
    
    Args:
        site_att (str): Target site name
        date_range (tuple): Tuple of (min_date, max_date) in 'YYYY-MM-DD' format
        radius_km (float): Radius in kilometers to find neighbors (default 5.0)
        csv_export (bool): Whether to export results to CSV
        
    Returns:
        dict: Dictionary containing 'site_data' and 'group_data' DataFrames
    """
    print(f"Getting traffic voice data for site {site_att} and neighbors within {radius_km}km")
    print(f"Date range: {date_range[0]} to {date_range[1]}")
    
    if not site_att:
        print("Error: site_att cannot be empty")
        return {"site_data": pd.DataFrame(), "group_data": pd.DataFrame()}
    
    engine = get_engine()
    
    try:
        # Get neighbor sites within radius
        neighbor_sites = get_neighbor_sites_within_radius(site_att, radius_km, engine)
        
        if not neighbor_sites:
            print(f"No sites found within {radius_km}km of {site_att}")
            return {"site_data": pd.DataFrame(), "group_data": pd.DataFrame()}
        
        # Get data for single site
        print(f"Getting traffic voice data for target site: {site_att}")
        site_data = get_daily_traffic_voice_for_site(site_att, date_range[0], date_range[1], engine)
        
        # Get aggregated data for neighbor group
        print(f"Getting aggregated traffic voice data for {len(neighbor_sites)} neighbor sites")
        group_data = get_daily_traffic_voice_for_group(neighbor_sites, date_range[0], date_range[1], engine)
        
        # Export to CSV if requested
        if csv_export:
            site_filename = f"traffic_voice_site_{site_att}_{date_range[0]}_to_{date_range[1]}.csv"
            group_filename = f"traffic_voice_group_{site_att}_{radius_km}km_{date_range[0]}_to_{date_range[1]}.csv"
            
            if not site_data.empty:
                site_data.to_csv(site_filename, index=False)
                print(f"Site data exported to {site_filename}")
            
            if not group_data.empty:
                group_data.to_csv(group_filename, index=False)
                print(f"Group data exported to {group_filename}")
        
        print(f"Site data: {len(site_data)} records")
        print(f"Group data: {len(group_data)} records")
        
        return {
            "site_data": site_data,
            "group_data": group_data,
            "neighbor_sites": neighbor_sites
        }
        
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        return {"site_data": pd.DataFrame(), "group_data": pd.DataFrame()}
    except Exception as e:
        print(f"Error in get_traffic_voice_for_site_and_neighbors: {e}")
        return {"site_data": pd.DataFrame(), "group_data": pd.DataFrame()}


def analyze_traffic_voice_patterns(site_data, group_data, site_att):
    """
    Analyze traffic voice patterns and provide insights.
    
    Args:
        site_data (pandas.DataFrame): Site traffic voice data
        group_data (pandas.DataFrame): Group traffic voice data
        site_att (str): Site name
        
    Returns:
        dict: Analysis results and insights
    """
    if site_data.empty or group_data.empty:
        return {"error": "Insufficient data for analysis"}
    
    # Calculate statistics for site
    site_stats = {
        "total_traffic": site_data['traffic_voice'].sum(),
        "avg_daily_traffic": site_data['traffic_voice'].mean(),
        "max_daily_traffic": site_data['traffic_voice'].max(),
        "min_daily_traffic": site_data['traffic_voice'].min(),
        "umts_share": (site_data['h3g_traffic_v_user_cs'] + 
                      site_data['e3g_traffic_v_user_cs'] + 
                      site_data['n3g_traffic_v_user_cs']).sum() / site_data['traffic_voice'].sum() * 100 if site_data['traffic_voice'].sum() > 0 else 0,
        "volte_share": (site_data['user_traffic_volte_h'] + 
                       site_data['user_traffic_volte_e'] + 
                       site_data['user_traffic_volte_n'] + 
                       site_data['user_traffic_volte_s']).sum() / site_data['traffic_voice'].sum() * 100 if site_data['traffic_voice'].sum() > 0 else 0
    }
    
    # Calculate statistics for group
    group_stats = {
        "total_traffic": group_data['traffic_voice'].sum(),
        "avg_daily_traffic": group_data['traffic_voice'].mean(),
        "max_daily_traffic": group_data['traffic_voice'].max(),
        "min_daily_traffic": group_data['traffic_voice'].min(),
        "umts_share": (group_data['h3g_traffic_v_user_cs'] + 
                      group_data['e3g_traffic_v_user_cs'] + 
                      group_data['n3g_traffic_v_user_cs']).sum() / group_data['traffic_voice'].sum() * 100 if group_data['traffic_voice'].sum() > 0 else 0,
        "volte_share": (group_data['user_traffic_volte_h'] + 
                       group_data['user_traffic_volte_e'] + 
                       group_data['user_traffic_volte_n'] + 
                       group_data['user_traffic_volte_s']).sum() / group_data['traffic_voice'].sum() * 100 if group_data['traffic_voice'].sum() > 0 else 0
    }
    
    # Calculate site contribution to group
    site_contribution = (site_stats["total_traffic"] / group_stats["total_traffic"] * 100) if group_stats["total_traffic"] > 0 else 0
    
    return {
        "site_stats": site_stats,
        "group_stats": group_stats,
        "site_contribution_pct": site_contribution,
        "analysis_period": f"{site_data['date'].min()} to {site_data['date'].max()}",
        "data_points": len(site_data)
    }


if __name__ == "__main__":
    # Test the traffic voice processor
    test_site = "MEXMET0396"
    test_date_range = ("2024-09-01", "2024-09-15")
    test_radius = 5.0
    
    print("Testing traffic_voice_site_group_processor...")
    print("=" * 60)
    
    # Test 1: Get traffic voice data for site and neighbors
    print(f"\nTest 1: Getting traffic voice data for {test_site} and neighbors within {test_radius}km")
    results = get_traffic_voice_for_site_and_neighbors(test_site, test_date_range, test_radius, csv_export=True)
    
    if not results["site_data"].empty:
        print(f"\nSite data preview for {test_site}:")
        print(results["site_data"].head())
        print(f"\nSite traffic voice summary:")
        print(f"- Total records: {len(results['site_data'])}")
        print(f"- Date range: {results['site_data']['date'].min()} to {results['site_data']['date'].max()}")
        print(f"- Total traffic voice: {results['site_data']['traffic_voice'].sum():.4f}")
        print(f"- Average daily traffic: {results['site_data']['traffic_voice'].mean():.4f}")
    
    if not results["group_data"].empty:
        print(f"\nGroup data preview:")
        print(results["group_data"].head())
        print(f"\nGroup traffic voice summary:")
        print(f"- Total records: {len(results['group_data'])}")
        print(f"- Total traffic voice: {results['group_data']['traffic_voice'].sum():.4f}")
        print(f"- Average daily traffic: {results['group_data']['traffic_voice'].mean():.4f}")
    
    # Test 2: Analyze traffic patterns
    if not results["site_data"].empty and not results["group_data"].empty:
        print(f"\nTest 2: Analyzing traffic voice patterns")
        analysis = analyze_traffic_voice_patterns(results["site_data"], results["group_data"], test_site)
        
        if "error" not in analysis:
            print(f"\nTraffic Voice Analysis for {test_site}:")
            print(f"Analysis period: {analysis['analysis_period']}")
            print(f"Data points: {analysis['data_points']}")
            
            print(f"\nSite Statistics:")
            print(f"- Total traffic: {analysis['site_stats']['total_traffic']:.4f}")
            print(f"- Average daily: {analysis['site_stats']['avg_daily_traffic']:.4f}")
            print(f"- UMTS share: {analysis['site_stats']['umts_share']:.2f}%")
            print(f"- VoLTE share: {analysis['site_stats']['volte_share']:.2f}%")
            
            print(f"\nGroup Statistics:")
            print(f"- Total traffic: {analysis['group_stats']['total_traffic']:.4f}")
            print(f"- Average daily: {analysis['group_stats']['avg_daily_traffic']:.4f}")
            print(f"- UMTS share: {analysis['group_stats']['umts_share']:.2f}%")
            print(f"- VoLTE share: {analysis['group_stats']['volte_share']:.2f}%")
            
            print(f"\nSite contribution to group: {analysis['site_contribution_pct']:.2f}%")
    
    print("\nTesting completed!")
