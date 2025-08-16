import pandas as pd
import os
import dotenv
from sqlalchemy import create_engine, text

# Load environment variables
dotenv.load_dotenv()
ROOT_DIRECTORY = os.getenv('ROOT_DIRECTORY')
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

def get_cell_change_data_grouped(
    group_by='network',
    site_list=None,
    region_list=None,
    province_list=None,
    municipality_list=None,
    technology_list=None,
    vendor_list=None  # <-- Add vendor filter
):
    """
    Retrieve grouped data from both lte_cell_change_event and umts_cell_change_event tables.

    Args:
        group_by (str): Grouping level - 'network', 'region', 'province', or 'municipality'
        site_list (list): Optional list of sites to filter by. If None, no filtering applied.
        region_list (list): Optional list of regions to filter by. If None, no filtering applied.
        province_list (list): Optional list of provinces to filter by. If None, no filtering applied.
        municipality_list (list): Optional list of municipalities to filter by. If None, no filtering applied.
        technology_list (list): Optional list of technologies to filter by ('3G', '4G'). If None, no filtering applied.
        vendor_list (list): Optional list of vendors to filter by. If None, no filtering applied.

    Returns:
        pd.DataFrame: Grouped data with aggregated values
    """
    engine = create_connection()
    if engine is None:
        return None

    # Define grouping columns based on group_by parameter
    if group_by == 'network':
        group_columns = "'All Network' as network_level, date"
        group_by_clause = "GROUP BY date"
        order_by_clause = "network_level, date"
        partition_clause = ""  # No partition for network level - single running total
    elif group_by == 'region':
        group_columns = "region, date"
        group_by_clause = "GROUP BY region, date"
        order_by_clause = "region, date"
        partition_clause = "PARTITION BY region"
    elif group_by == 'province':
        group_columns = "region, province, date"
        group_by_clause = "GROUP BY region, province, date"
        order_by_clause = "region, province, date"
        partition_clause = "PARTITION BY region, province"
    elif group_by == 'municipality':
        group_columns = "region, province, municipality, date"
        group_by_clause = "GROUP BY region, province, municipality, date"
        order_by_clause = "region, province, municipality, date"
        partition_clause = "PARTITION BY region, province, municipality"
    else:
        raise ValueError("group_by must be one of: 'network', 'region', 'province', 'municipality'")

    # Build WHERE clause for filtering
    where_clauses = []

    # Add region filter if provided
    if region_list and len(region_list) > 0:
        region_list_str = "', '".join(str(region) for region in region_list)
        where_clauses.append(f"region IN ('{region_list_str}')")

    # Add province filter if provided
    if province_list and len(province_list) > 0:
        province_list_str = "', '".join(str(province) for province in province_list)
        where_clauses.append(f"province IN ('{province_list_str}')")

    # Add municipality filter if provided
    if municipality_list and len(municipality_list) > 0:
        municipality_list_str = "', '".join(str(municipality) for municipality in municipality_list)
        where_clauses.append(f"municipality IN ('{municipality_list_str}')")

    # Add site filter if provided
    if site_list and len(site_list) > 0:
        site_list_str = "', '".join(str(site) for site in site_list)
        where_clauses.append(f"att_name IN ('{site_list_str}')")

    # DISABLE the database vendor filter (since there's no vendor column)
    # if vendor_list and len(vendor_list) > 0:
    #     vendor_list_str = "', '".join(str(vendor) for vendor in vendor_list)
    #     where_clauses.append(f"vendor IN ('{vendor_list_str}')")

    site_filter = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    # Determine which tables to query based on technology filter
    query_lte = True
    query_umts = True

    if technology_list and len(technology_list) > 0:
        technology_set = set(str(tech).upper() for tech in technology_list)
        query_lte = '4G' in technology_set
        query_umts = '3G' in technology_set

    # Determine which vendor columns to include based on vendor filter
    vendor_patterns = ['h', 'e', 'n', 's']  # All vendors by default
    if vendor_list and len(vendor_list) > 0:
        vendor_mapping = {
            'huawei': 'h',
            'ericsson': 'e',
            'nokia': 'n', 
            'samsung': 's'
        }
        vendor_patterns = []
        for vendor in vendor_list:
            pattern = vendor_mapping.get(vendor.lower())
            if pattern:
                vendor_patterns.append(pattern)
        
        if not vendor_patterns:
            vendor_patterns = ['h', 'e', 'n', 's']  # Fallback to all vendors

    print(f"Vendor patterns to include: {vendor_patterns}")

    # Build vendor-specific column selections
    def build_vendor_columns(vendor_patterns, technology='4g'):
        """Build column selections based on vendor patterns"""
        columns = []
        
        # Band columns for each vendor pattern
        bands = ['b2', 'b4', 'b5', 'b7', 'b26', 'b42', 'x']
        
        for band in bands:
            for vendor in ['h', 'e', 'n', 's']:
                if vendor in vendor_patterns:
                    columns.append(f"SUM(COALESCE({band}_{vendor}{technology}, 0)) as {band}_{vendor}{technology}")
                else:
                    columns.append(f"0 as {band}_{vendor}{technology}")
        
        return columns

    # Build the query components
    lte_query = ""
    umts_query = ""
    union_clause = ""

    if query_lte:
        lte_vendor_columns = build_vendor_columns(vendor_patterns, '4g')
        
        lte_query = f"""
        -- LTE changes by date
        SELECT 
            {group_columns},
            SUM(COALESCE(add_cell, 0)) as add_cell_lte,
            SUM(COALESCE(delete_cell, 0)) as delete_cell_lte,
            0 as add_cell_umts,
            0 as delete_cell_umts,
            {', '.join(lte_vendor_columns)},
            0 as b2_h3g, 0 as b2_e3g, 0 as b2_n3g,
            0 as b4_h3g, 0 as b4_e3g, 0 as b4_n3g,
            0 as b5_h3g, 0 as b5_e3g, 0 as b5_n3g,
            0 as x_h3g, 0 as x_e3g, 0 as x_n3g
        FROM lte_cell_change_event
        {site_filter}
        {group_by_clause}
        """

    if query_umts:
        # Build UMTS vendor columns properly - only 3G vendors (no Samsung for 3G)
        umts_vendor_columns = []
        bands = ['b2', 'b4', 'b5', 'x']
        
        for band in bands:
            for vendor in ['h', 'e', 'n']:  # Only these vendors for 3G
                if vendor in vendor_patterns:
                    umts_vendor_columns.append(f"SUM(COALESCE({band}_{vendor}3g, 0)) as {band}_{vendor}3g")
                else:
                    umts_vendor_columns.append(f"0 as {band}_{vendor}3g")
        
        umts_query = f"""
        -- UMTS changes by date
        SELECT 
            {group_columns},
            0 as add_cell_lte,
            0 as delete_cell_lte,
            SUM(COALESCE(add_cell, 0)) as add_cell_umts,
            SUM(COALESCE(delete_cell, 0)) as delete_cell_umts,
            0 as b2_h4g, 0 as b2_e4g, 0 as b2_n4g, 0 as b2_s4g,
            0 as b4_h4g, 0 as b4_e4g, 0 as b4_n4g, 0 as b4_s4g,
            0 as b5_h4g, 0 as b5_e4g, 0 as b5_n4g, 0 as b5_s4g,
            0 as b7_h4g, 0 as b7_e4g, 0 as b7_n4g, 0 as b7_s4g,
            0 as b26_h4g, 0 as b26_e4g, 0 as b26_n4g, 0 as b26_s4g,
            0 as b42_h4g, 0 as b42_e4g, 0 as b42_n4g, 0 as b42_s4g,
            0 as x_h4g, 0 as x_e4g, 0 as x_n4g, 0 as x_s4g,
            {', '.join(umts_vendor_columns)}
        FROM umts_cell_change_event
        {site_filter}
        {group_by_clause}
        """

    # Combine queries with UNION ALL if both are needed
    if query_lte and query_umts:
        union_clause = "UNION ALL"
        daily_changes_query = lte_query + "\n" + union_clause + "\n" + umts_query
    elif query_lte:
        daily_changes_query = lte_query
    elif query_umts:
        daily_changes_query = umts_query
    else:
        # No technology selected, return empty DataFrame
        print("No technology selected or invalid technology filter.")
        return pd.DataFrame()

    # Combined query with UNION ALL - Calculate running totals from changes
    combined_query = f"""
    WITH daily_changes AS (
        {daily_changes_query}
    ),
    aggregated_changes AS (
        SELECT 
            {group_columns},
            SUM(add_cell_lte) as add_cell_lte,
            SUM(delete_cell_lte) as delete_cell_lte,
            SUM(add_cell_umts) as add_cell_umts,
            SUM(delete_cell_umts) as delete_cell_umts,
            SUM(b2_h4g) as b2_h4g, SUM(b2_e4g) as b2_e4g, SUM(b2_n4g) as b2_n4g, SUM(b2_s4g) as b2_s4g,
            SUM(b4_h4g) as b4_h4g, SUM(b4_e4g) as b4_e4g, SUM(b4_n4g) as b4_n4g, SUM(b4_s4g) as b4_s4g,
            SUM(b5_h4g) as b5_h4g, SUM(b5_e4g) as b5_e4g, SUM(b5_n4g) as b5_n4g, SUM(b5_s4g) as b5_s4g,
            SUM(b7_h4g) as b7_h4g, SUM(b7_e4g) as b7_e4g, SUM(b7_n4g) as b7_n4g, SUM(b7_s4g) as b7_s4g,
            SUM(b26_h4g) as b26_h4g, SUM(b26_e4g) as b26_e4g, SUM(b26_n4g) as b26_n4g, SUM(b26_s4g) as b26_s4g,
            SUM(b42_h4g) as b42_h4g, SUM(b42_e4g) as b42_e4g, SUM(b42_n4g) as b42_n4g, SUM(b42_s4g) as b42_s4g,
            SUM(x_h4g) as x_h4g, SUM(x_e4g) as x_e4g, SUM(x_n4g) as x_n4g, SUM(x_s4g) as x_s4g,
            SUM(b2_h3g) as b2_h3g, SUM(b2_e3g) as b2_e3g, SUM(b2_n3g) as b2_n3g,
            SUM(b4_h3g) as b4_h3g, SUM(b4_e3g) as b4_e3g, SUM(b4_n3g) as b4_n3g,
            SUM(b5_h3g) as b5_h3g, SUM(b5_e3g) as b5_e3g, SUM(b5_n3g) as b5_n3g,
            SUM(x_h3g) as x_h3g, SUM(x_e3g) as x_e3g, SUM(x_n3g) as x_n3g
        FROM daily_changes
        {group_by_clause}
    )
    SELECT 
        {group_columns},
        add_cell_lte,
        delete_cell_lte,
        -- Calculate running total for LTE: sum of all previous (add_cell - delete_cell)
        SUM(add_cell_lte - delete_cell_lte) OVER (
            {partition_clause} 
            ORDER BY date 
            ROWS UNBOUNDED PRECEDING
        ) as total_cell_lte,
        add_cell_umts,
        delete_cell_umts,
        -- Calculate UMTS total as sum of all 3G band components (running total)
        SUM(b2_h3g + b2_e3g + b2_n3g + b4_h3g + b4_e3g + b4_n3g + 
            b5_h3g + b5_e3g + b5_n3g + x_h3g + x_e3g + x_n3g) OVER (
            {partition_clause} 
            ORDER BY date 
            ROWS UNBOUNDED PRECEDING
        ) as total_cell_umts,
        -- Calculate running totals for all band columns
        SUM(b2_h4g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b2_h4g,
        SUM(b2_e4g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b2_e4g,
        SUM(b2_n4g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b2_n4g,
        SUM(b2_s4g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b2_s4g,
        SUM(b4_h4g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b4_h4g,
        SUM(b4_e4g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b4_e4g,
        SUM(b4_n4g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b4_n4g,
        SUM(b4_s4g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b4_s4g,
        SUM(b5_h4g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b5_h4g,
        SUM(b5_e4g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b5_e4g,
        SUM(b5_n4g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b5_n4g,
        SUM(b5_s4g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b5_s4g,
        SUM(b7_h4g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b7_h4g,
        SUM(b7_e4g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b7_e4g,
        SUM(b7_n4g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b7_n4g,
        SUM(b7_s4g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b7_s4g,
        SUM(b26_h4g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b26_h4g,
        SUM(b26_e4g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b26_e4g,
        SUM(b26_n4g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b26_n4g,
        SUM(b26_s4g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b26_s4g,
        SUM(b42_h4g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b42_h4g,
        SUM(b42_e4g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b42_e4g,
        SUM(b42_n4g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b42_n4g,
        SUM(b42_s4g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b42_s4g,
        SUM(x_h4g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as x_h4g,
        SUM(x_e4g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as x_e4g,
        SUM(x_n4g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as x_n4g,
        SUM(x_s4g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as x_s4g,
        SUM(b2_h3g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b2_h3g,
        SUM(b2_e3g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b2_e3g,
        SUM(b2_n3g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b2_n3g,
        SUM(b4_h3g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b4_h3g,
        SUM(b4_e3g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b4_e3g,
        SUM(b4_n3g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b4_n3g,
        SUM(b5_h3g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b5_h3g,
        SUM(b5_e3g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b5_e3g,
        SUM(b5_n3g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as b5_n3g,
        SUM(x_h3g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as x_h3g,
        SUM(x_e3g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as x_e3g,
        SUM(x_n3g) OVER ({partition_clause} ORDER BY date ROWS UNBOUNDED PRECEDING) as x_n3g
    FROM aggregated_changes
    ORDER BY {order_by_clause};
    """

    try:
        print("Connected to PostgreSQL database successfully.")
        
        # Build filter summary message
        filter_messages = []
        if region_list:
            filter_messages.append(f"{len(region_list)} regions")
        if province_list:
            filter_messages.append(f"{len(province_list)} provinces")
        if municipality_list:
            filter_messages.append(f"{len(municipality_list)} municipalities")
        if site_list:
            filter_messages.append(f"{len(site_list)} sites")
        if technology_list:
            filter_messages.append(f"technologies: {', '.join(technology_list)}")
        if vendor_list:
            filter_messages.append(f"vendors: {', '.join(vendor_list)}")
        
        filter_msg = f" (filtered by: {', '.join(filter_messages)})" if filter_messages else " (no filters applied)"
        print(f"Fetching grouped data by {group_by}{filter_msg}...")
        
        # Execute the combined query
        grouped_df = pd.read_sql_query(combined_query, engine)
        print(f"Retrieved {len(grouped_df)} grouped records.")

        return grouped_df

    except Exception as e:
        print(f"Error executing query: {e}")
        return None
    finally:
        engine.dispose()

def create_zero_filled_result(group_by, region_list=None, province_list=None, municipality_list=None, site_list=None):
    """
    Create a zero-filled DataFrame when no data is found but filters were applied.
    This provides a meaningful response for client requests with no matching data.
    
    Args:
        group_by (str): Grouping level ('network', 'region', 'province', 'municipality')
        region_list (list): List of regions for the result
        province_list (list): List of provinces for the result  
        municipality_list (list): List of municipalities for the result
        site_list (list): List of sites for the result
    
    Returns:
        pd.DataFrame: Zero-filled DataFrame with appropriate structure
    """
    import pandas as pd
    from datetime import datetime, timedelta
    
    print("No data found for the specified filters. Creating zero-filled result...")
    
    # Create a date range for the last 30 days as default
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # Determine geographic structure based on filters and group_by level
    if group_by == 'network':
        geographic_combinations = [('NETWORK_LEVEL',)]
        columns = ['network_level', 'date']
    elif group_by == 'region':
        regions = region_list if region_list else ['NO_DATA_REGION']
        geographic_combinations = [(region,) for region in regions]
        columns = ['region', 'date']
    elif group_by == 'province':
        if region_list and province_list:
            geographic_combinations = [(region, province) 
                                     for region in region_list 
                                     for province in province_list]
        else:
            geographic_combinations = [('NO_DATA_REGION', 'NO_DATA_PROVINCE')]
        columns = ['region', 'province', 'date']
    elif group_by == 'municipality':
        if region_list and province_list and municipality_list:
            geographic_combinations = [(region, province, municipality)
                                     for region in region_list
                                     for province in province_list  
                                     for municipality in municipality_list]
        elif municipality_list:
            geographic_combinations = [('NO_DATA_REGION', 'NO_DATA_PROVINCE', municipality)
                                     for municipality in municipality_list]
        else:
            geographic_combinations = [('NO_DATA_REGION', 'NO_DATA_PROVINCE', 'NO_DATA_MUNICIPALITY')]
        columns = ['region', 'province', 'municipality', 'date']
    
    # Create all combinations of geographic levels and dates
    rows = []
    for geo_combo in geographic_combinations:
        for date in date_range:
            row = list(geo_combo) + [date]
            rows.append(row)
    
    # Create DataFrame with basic structure
    df = pd.DataFrame(rows, columns=columns)
    
    # Add all the expected data columns with zero values
    zero_columns = {
        'add_cell_lte': 0,
        'delete_cell_lte': 0, 
        'total_cell_lte': 0,
        'add_cell_umts': 0,
        'delete_cell_umts': 0,
        'total_cell_umts': 0,
        # LTE band columns
        'b2_h4g': 0, 'b2_e4g': 0, 'b2_n4g': 0, 'b2_s4g': 0,
        'b4_h4g': 0, 'b4_e4g': 0, 'b4_n4g': 0, 'b4_s4g': 0,
        'b5_h4g': 0, 'b5_e4g': 0, 'b5_n4g': 0, 'b5_s4g': 0,
        'b7_h4g': 0, 'b7_e4g': 0, 'b7_n4g': 0, 'b7_s4g': 0,
        'b26_h4g': 0, 'b26_e4g': 0, 'b26_n4g': 0, 'b26_s4g': 0,
        'b42_h4g': 0, 'b42_e4g': 0, 'b42_n4g': 0, 'b42_s4g': 0,
        'x_h4g': 0, 'x_e4g': 0, 'x_n4g': 0, 'x_s4g': 0,
        # UMTS band columns  
        'b2_h3g': 0, 'b2_e3g': 0, 'b2_n3g': 0,
        'b4_h3g': 0, 'b4_e3g': 0, 'b4_n3g': 0,
        'b5_h3g': 0, 'b5_e3g': 0, 'b5_n3g': 0,
        'x_h3g': 0, 'x_e3g': 0, 'x_n3g': 0
    }
    
    for col, value in zero_columns.items():
        df[col] = value
    
    print(f"Created zero-filled result with {len(df)} rows covering {len(date_range)} dates.")
    return df

def expand_dates(df, group_by):
    """
    Expand the DataFrame to include all dates between min and max dates,
    filling missing dates with previous values (carry forward).
    
    Args:
        df (pd.DataFrame): Input DataFrame with potential date gaps
        group_by (str): Grouping level used to determine grouping columns
    
    Returns:
        pd.DataFrame: Expanded DataFrame with all dates filled
    """
    if df is None or df.empty:
        return df
    
    print("Expanding dates to fill gaps...")
    
    # Convert date column to datetime
    df['date'] = pd.to_datetime(df['date'])
    df.sort_values('date', inplace=True)
    
    # Prepare empty list for results
    all_rows = []
    
    # Define grouping columns based on group_by parameter
    if group_by == 'network':
        grouping_cols = ['network_level']
    elif group_by == 'region':
        grouping_cols = ['region']
    elif group_by == 'province':
        grouping_cols = ['region', 'province']
    elif group_by == 'municipality':
        grouping_cols = ['region', 'province', 'municipality']
    else:
        print(f"Unknown group_by value: {group_by}")
        return df
    
    # Process each unique group
    for group_values, group_data in df.groupby(grouping_cols):
        # Create date range from min to max date for this group
        date_range = pd.date_range(
            start=group_data['date'].min(), 
            end=group_data['date'].max(), 
            freq='D'
        )
        
        # Initialize previous row
        previous_row = None
        
        # Iterate over each date in the range
        for date in date_range:
            if date in group_data['date'].values:
                # Date exists in data - use actual row
                current_row = group_data[group_data['date'] == date].iloc[0].to_dict()
                previous_row = current_row
            else:
                # Date missing - carry forward previous values
                if previous_row:
                    current_row = previous_row.copy()
                    current_row['date'] = date
                    # Reset daily change columns to 0 for filled dates
                    current_row['add_cell_lte'] = 0
                    current_row['delete_cell_lte'] = 0
                    current_row['add_cell_umts'] = 0
                    current_row['delete_cell_umts'] = 0
                    # Band columns keep their previous values (cumulative totals maintained)
            
            if 'current_row' in locals():
                all_rows.append(current_row)
    
    # Create expanded DataFrame
    expanded_df = pd.DataFrame(all_rows)
    
    # Sort by grouping columns and date
    sort_cols = grouping_cols + ['date']
    expanded_df.sort_values(sort_cols, inplace=True)
    
    print(f"Date expansion completed. Original rows: {len(df)}, Expanded rows: {len(expanded_df)}")
    
    return expanded_df

if __name__ == "__main__":
    # Import functions from other modules
    from plot_processor import plot_cell_change_data, save_plot_html, show_plot
    from report_processor import save_to_csv, save_summary_report
    
    # Configuration
    group_level = 'network'
    region_list = []
    province_list = []
    municipality_list = []
    site_list = []
    technology_list = []  # Add technology filter
    vendor_list = ['huawei']  # Add vendor filter
    
    print(f"Starting cell change data retrieval with grouping by {group_level}...")
    
    # Get the grouped data
    df = get_cell_change_data_grouped(
        group_by=group_level, 
        site_list=site_list,
        region_list=region_list,
        province_list=province_list,
        municipality_list=municipality_list,
        technology_list=technology_list,  # Add technology parameter
        vendor_list=vendor_list  # Add vendor parameter
    )
    
    # If no data found and filters were applied, create zero-filled result
    if (df is None or df.empty) and (site_list or region_list or province_list or municipality_list or technology_list or vendor_list):
        print("No data found for the specified filters. Creating zero-filled result for client validation...")
        df = create_zero_filled_result(
            group_by=group_level,
            region_list=region_list,
            province_list=province_list, 
            municipality_list=municipality_list,
            site_list=site_list
        )
    
    if df is not None and not df.empty:
        print(f"\nRetrieved data shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print(f"Filters applied: technology={technology_list}, vendor={vendor_list}, region={region_list}, province={province_list}, municipality={municipality_list}, site={site_list}")
        
        # Expand dates to fill gaps
        df_expanded = expand_dates(df, group_by=group_level)
        
        # Save to CSV using report_processor
        csv_path = save_to_csv(df_expanded)
        
        # Generate summary report
        summary_path = save_summary_report(df_expanded, group_level)
        
        if csv_path:
            print(f"\nData retrieval and save completed successfully!")
            print(f"CSV file saved: {csv_path}")
            if summary_path:
                print(f"Summary report saved: {summary_path}")
            
            # Plot the data using the same DataFrame
            print("\nGenerating plot...")
            fig = plot_cell_change_data(
                df=df_expanded,
                group_by=group_level,
                category='technology',  
                dates=None
            )
            
            if fig:
                # Save the plot as HTML
                output_dir = os.path.join(ROOT_DIRECTORY, "output") if ROOT_DIRECTORY else "output"
                filename = f"cell_change_plot_{group_level}.html"
                
                save_plot_html(fig, filename, output_dir, suppress_output=True)
                show_plot(fig, suppress_output=True)
        else:
            print("\nData retrieval completed but save failed.")
    else:
        print("Failed to retrieve data.")