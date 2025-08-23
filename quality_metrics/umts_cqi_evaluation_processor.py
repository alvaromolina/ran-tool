import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timedelta
import os
import dotenv
import sys
import contextlib
from contextlib import redirect_stdout
from io import StringIO

# Import functions from existing modules
from master_node_neighbor_processor import get_master_node_neighbor
from umts_cqi_site_group_processor import get_umts_cqi_for_site_group

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

def calculate_period_average(site_att, start_date, end_date):
    """
    Calculate average UMTS CQI for a single site in a given period.
    """
    engine = get_engine()
    
    try:
        query = """
        SELECT AVG(umts_cqi) as avg_umts_cqi, COUNT(*) as record_count
        FROM umts_cqi_metrics_daily
        WHERE site_att = %(site_att)s
        AND date >= %(start_date)s 
        AND date <= %(end_date)s
        AND group_level = 'site'
        AND vendors = 'All'
        AND umts_cqi IS NOT NULL
        """
        
        params = {
            "site_att": site_att,
            "start_date": start_date,
            "end_date": end_date
        }
        
        df = pd.read_sql(query, engine, params=params)
        
        if df.empty or df.iloc[0]['avg_umts_cqi'] is None:
            return None, 0
        
        return float(df.iloc[0]['avg_umts_cqi']), int(df.iloc[0]['record_count'])
        
    except SQLAlchemyError as e:
        print(f"Database error calculating average: {e}")
        return None, 0

def calculate_neighbors_group_average(neighbors, start_date, end_date):
    """
    Calculate average UMTS CQI for neighbors using the group processor.
    """
    if not neighbors:
        return None, 0
    
    try:
        # Use the group processor to calculate neighbor metrics (suppress all output)
        import os
        import sys
        
        # Suppress both stdout and stderr
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        
        try:
            sys.stdout = StringIO()
            sys.stderr = StringIO()
            
            df_group = get_umts_cqi_for_site_group(
                site_list=neighbors,
                date_range=(start_date, end_date),
                csv_export=False
            )
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr
        
        if df_group.empty:
            return None, 0
        
        # Get the UMTS CQI value from the group calculation
        avg_umts_cqi = df_group['umts_cqi'].iloc[0]
        record_count = df_group['site_count'].iloc[0]
        
        return float(avg_umts_cqi), record_count
        
    except Exception as e:
        print(f"Error calculating neighbors group average: {e}")
        return None, 0

def get_max_date_available():
    """
    Get the maximum date available in umts_cqi_metrics_daily table.
    """
    engine = get_engine()
    
    try:
        query = "SELECT MAX(date) as max_date FROM umts_cqi_metrics_daily"
        df = pd.read_sql(query, engine)
        
        if df.empty or df.iloc[0]['max_date'] is None:
            return None
        
        return df.iloc[0]['max_date']
        
    except SQLAlchemyError as e:
        print(f"Database error getting max date: {e}")
        return None

def classify_change(before_ref, after_value, thd_low_pct, thd_high_pct):
    """Classify change as Degraded (D), Improved (I), or Neutral (N)"""
    if before_ref is None or after_value is None:
        return 'N'
    
    change_pct = ((after_value - before_ref) / before_ref) * 100
    
    if change_pct < -thd_low_pct:
        return 'D'  # Degraded
    elif change_pct > thd_high_pct:
        return 'I'  # Improved
    else:
        return 'N'  # Neutral

def get_final_evaluation(pattern):
    """Map pattern to final evaluation result"""
    evaluation_map = {
        'D-D': 'FAIL',      # Degraded after, Degraded last
        'D-N': 'RESTORE',   # Degraded after, Neutral last
        'D-I': 'RESTORE',   # Degraded after, Improved last
        'I-D': 'FAIL',      # Improved after, Degraded last
        'I-I': 'PASS',      # Improved after, Improved last
        'I-N': 'PASS',      # Improved after, Neutral last
        'N-I': 'PASS',      # Neutral after, Improved last
        'N-N': 'PASS',      # Neutral after, Neutral last
        'N-D': 'FAIL'       # Neutral after, Degraded last
    }
    return evaluation_map.get(pattern, 'UNKNOWN')

def calculate_change_pct(before, after):
    """Calculate percentage change between before and after values"""
    if before is None or after is None or before == 0:
        return None
    return round(((after - before) / before) * 100, 2)

def umts_cqi_site_evaluation(input_date, site_att, guard=7, period=7, thd_low=5, thd_high=5):
    """
    Evaluate UMTS CQI patterns for a specific site before and after a given date.
    
    Args:
        input_date (str): Reference date in 'YYYY-MM-DD' format
        site_att (str): Site name to evaluate
        guard (int): Guard period in days (default: 7)
        period (int): Analysis period in days (default: 7)
        thd_low (float): Low threshold percentage (default: 5)
        thd_high (float): High threshold percentage (default: 5)
    
    Returns:
        dict: Site evaluation results including averages, patterns, and final evaluation
    """
    
    # Convert input_date to datetime
    input_dt = datetime.strptime(input_date, '%Y-%m-%d').date()
    
    # Calculate period dates
    before_start = input_dt - timedelta(days=guard + period)
    before_end = input_dt - timedelta(days=guard)
    after_start = input_dt + timedelta(days=guard)
    after_end = input_dt + timedelta(days=guard + period)
    
    # Get max date available
    max_date = get_max_date_available()
    if max_date is None:
        return {"error": "No data available in umts_cqi_metrics_daily table"}
    
    last_start = max_date - timedelta(days=period)
    last_end = max_date
    
    # Calculate averages for the site only
    site_before_avg, site_before_count = calculate_period_average(site_att, before_start, before_end)
    site_after_avg, site_after_count = calculate_period_average(site_att, after_start, after_end)
    site_last_avg, site_last_count = calculate_period_average(site_att, last_start, last_end)
    
    # Site evaluation
    site_after_pattern = classify_change(site_before_avg, site_after_avg, thd_low, thd_high)
    site_last_pattern = classify_change(site_before_avg, site_last_avg, thd_low, thd_high)
    site_pattern = f"{site_after_pattern}-{site_last_pattern}"
    
    site_evaluation = get_final_evaluation(site_pattern)
    
    # Calculate percentage changes
    site_after_change_pct = calculate_change_pct(site_before_avg, site_after_avg)
    site_last_change_pct = calculate_change_pct(site_before_avg, site_last_avg)
    
    # Build result dictionary
    result = {
        'input_date': input_date,
        'site_att': site_att,
        'evaluation_type': 'site',
        'guard_days': guard,
        'period_days': period,
        'thd_low_pct': thd_low,
        'thd_high_pct': thd_high,
        
        # Period definitions
        'before_period': f"{before_start} to {before_end}",
        'after_period': f"{after_start} to {after_end}",
        'last_period': f"{last_start} to {last_end}",
        
        # Site results
        'before_avg': round(site_before_avg, 4) if site_before_avg else None,
        'after_avg': round(site_after_avg, 4) if site_after_avg else None,
        'last_avg': round(site_last_avg, 4) if site_last_avg else None,
        'before_count': site_before_count,
        'after_count': site_after_count,
        'last_count': site_last_count,
        'after_pattern': site_after_pattern,
        'last_pattern': site_last_pattern,
        'pattern': site_pattern,
        'evaluation': site_evaluation,
        'after_change_pct': site_after_change_pct,
        'last_change_pct': site_last_change_pct
    }
    
    return result

def umts_cqi_neighbor_evaluation(input_date, site_att, radius=5, guard=7, period=7, thd_low=5, thd_high=5):
    """
    Evaluate UMTS CQI patterns for neighbors of a specific site before and after a given date.
    Uses the umts_cqi_group_processor for efficient neighbor calculations.
    
    Args:
        input_date (str): Reference date in 'YYYY-MM-DD' format
        site_att (str): Site name to find neighbors for
        radius (float): Radius in km to find neighbors (default: 5)
        guard (int): Guard period in days (default: 7)
        period (int): Analysis period in days (default: 7)
        thd_low (float): Low threshold percentage (default: 5)
        thd_high (float): High threshold percentage (default: 5)
    
    Returns:
        dict: Neighbor evaluation results including averages, patterns, and final evaluation
    """
    
    # Convert input_date to datetime
    input_dt = datetime.strptime(input_date, '%Y-%m-%d').date()
    
    # Calculate period dates
    before_start = input_dt - timedelta(days=guard + period)
    before_end = input_dt - timedelta(days=guard)
    after_start = input_dt + timedelta(days=guard)
    after_end = input_dt + timedelta(days=guard + period)
    
    # Get max date available
    max_date = get_max_date_available()
    if max_date is None:
        return {"error": "No data available in umts_cqi_metrics_daily table"}
    
    last_start = max_date - timedelta(days=period)
    last_end = max_date
    
    # Get neighbors within radius using imported function - suppress output
    with open(os.devnull, 'w') as devnull:
        with contextlib.redirect_stdout(devnull), contextlib.redirect_stderr(devnull):
            neighbors_df = get_master_node_neighbor(site_att, radius_km=radius)
    neighbors = neighbors_df['att_name'].tolist() if not neighbors_df.empty else []
    
    if not neighbors:
        return {"error": f"No neighbors found within {radius}km of site {site_att}"}
    
    # Calculate averages for neighbors using group processor (silent mode)
    neighbor_before_avg, neighbor_before_count = calculate_neighbors_group_average(neighbors, before_start, before_end)
    neighbor_after_avg, neighbor_after_count = calculate_neighbors_group_average(neighbors, after_start, after_end)
    neighbor_last_avg, neighbor_last_count = calculate_neighbors_group_average(neighbors, last_start, last_end)
    
    # Neighbor evaluation (using neighbor's before as reference)
    neighbor_after_pattern = classify_change(neighbor_before_avg, neighbor_after_avg, thd_low, thd_high)
    neighbor_last_pattern = classify_change(neighbor_before_avg, neighbor_last_avg, thd_low, thd_high)
    neighbor_pattern = f"{neighbor_after_pattern}-{neighbor_last_pattern}"
    
    neighbor_evaluation = get_final_evaluation(neighbor_pattern)
    
    # Calculate percentage changes
    neighbor_after_change_pct = calculate_change_pct(neighbor_before_avg, neighbor_after_avg)
    neighbor_last_change_pct = calculate_change_pct(neighbor_before_avg, neighbor_last_avg)
    
    # Build result dictionary
    result = {
        'input_date': input_date,
        'site_att': site_att,
        'evaluation_type': 'neighbor',
        'radius_km': radius,
        'guard_days': guard,
        'period_days': period,
        'thd_low_pct': thd_low,
        'thd_high_pct': thd_high,
        'neighbors_count': len(neighbors),
        'neighbors_list': neighbors,
        
        # Period definitions
        'before_period': f"{before_start} to {before_end}",
        'after_period': f"{after_start} to {after_end}",
        'last_period': f"{last_start} to {last_end}",
        
        # Neighbor results
        'before_avg': round(neighbor_before_avg, 4) if neighbor_before_avg else None,
        'after_avg': round(neighbor_after_avg, 4) if neighbor_after_avg else None,
        'last_avg': round(neighbor_last_avg, 4) if neighbor_last_avg else None,
        'before_count': neighbor_before_count,
        'after_count': neighbor_after_count,
        'last_count': neighbor_last_count,
        'after_pattern': neighbor_after_pattern,
        'last_pattern': neighbor_last_pattern,
        'pattern': neighbor_pattern,
        'evaluation': neighbor_evaluation,
        'after_change_pct': neighbor_after_change_pct,
        'last_change_pct': neighbor_last_change_pct
    }
    
    return result

def print_evaluation_summary(result):
    """
    Print a formatted summary of the evaluation results.
    """
    if 'error' in result:
        print(f"Error: {result['error']}")
        return
    
    eval_type = result.get('evaluation_type', 'unknown')
    title = f"UMTS CQI {eval_type.upper()} EVALUATION SUMMARY"
    
    print("\n" + "="*80)
    print(title)
    print("="*80)
    print(f"Site: {result['site_att']}")
    print(f"Evaluation Type: {eval_type.title()}")
    print(f"Input Date: {result['input_date']}")
    print(f"Parameters: Guard={result['guard_days']}d, Period={result['period_days']}d, Radius={result['radius_km']}km")
    print(f"Thresholds: Low={result['thd_low_pct']}%, High={result['thd_high_pct']}%")
    
    if eval_type == 'neighbor':
        print(f"Neighbors: {result['neighbors_count']} sites")
        if result['neighbors_list']:
            print(f"Neighbor List: {', '.join(result['neighbors_list'])}")
    
    print(f"\nPERIOD DEFINITIONS:")
    print(f"Before: {result['before_period']}")
    print(f"After:  {result['after_period']}")
    print(f"Last:   {result['last_period']}")
    
    entity = "Site" if eval_type == 'site' else "Neighbors"
    print(f"\n{entity.upper()} ANALYSIS:")
    print(f"Before Avg: {result['before_avg']} ({result['before_count']} records)")
    print(f"After Avg:  {result['after_avg']} ({result['after_count']} records) - Change: {result['after_change_pct']}%")
    print(f"Last Avg:   {result['last_avg']} ({result['last_count']} records) - Change: {result['last_change_pct']}%")
    print(f"Pattern: {result['pattern']} → {result['evaluation']}")
    
    print(f"\nFINAL EVALUATION: {result['evaluation']}")
    print("="*80)

# Example usage
if __name__ == "__main__":
    # Test both evaluation functions
    test_site = "MEXMET0396"
    test_date = "2024-09-21"
    
    site_result = umts_cqi_site_evaluation(
        input_date=test_date,
        site_att=test_site,
        guard=7,
        period=7,
        thd_low=5,
        thd_high=5
    )
    print_evaluation_summary(site_result)
    
    neighbor_result = umts_cqi_neighbor_evaluation(
        input_date=test_date,
        site_att=test_site,
        radius=5,
        guard=7,
        period=7,
        thd_low=5,
        thd_high=5
    )
    print_evaluation_summary(neighbor_result)