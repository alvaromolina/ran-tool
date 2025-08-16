import os
import dotenv
import pandas as pd
import logging
from datetime import datetime
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
dotenv.load_dotenv()
POSTGRES_USERNAME = os.getenv('POSTGRES_USERNAME')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_DB = os.getenv('POSTGRES_DB')
ROOT_DIRECTORY = os.getenv('ROOT_DIRECTORY')

def create_connection():
    """Create database connection using SQLAlchemy"""
    try:
        connection_string = f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        engine = create_engine(connection_string)
        logger.info("Database connection established successfully")
        return engine
    except Exception as e:
        logger.error(f"Error creating database connection: {e}")
        return None

def truncate_umts_cell_change_event():
    """Truncate the umts_cell_change_event table using SQLAlchemy"""
    try:
        engine = create_connection()
        if engine is None:
            return False
        
        print(f"Truncating umts_cell_change_event table...")
        
        with engine.connect() as connection:
            connection.execute(text("TRUNCATE TABLE public.umts_cell_change_event;"))
            connection.commit()
        
        print(f"Table truncated successfully")
        print()
        
        return True
        
    except Exception as e:
        print(f"Error in truncate_umts_cell_change_event: {e}")
        return False

def execute_umts_cell_query(engine):
    """Execute the UMTS cell traffic period query with SQLAlchemy"""
    query = """
    SELECT 
        mc.region,
        mc.province,
        mc.municipality,
        utp.vendor,
        mc.att_name,
        mc.att_tech,
        mc.band_indicator,
        utp.init_date,
        utp.end_date
    FROM 
        public.umts_cell_traffic_period AS utp
    LEFT JOIN 
        public.master_cell_total AS mc
    ON 
        utp.cell = mc.cell_name
    WHERE 
        utp.cell IS NOT NULL;
    """
    
    try:
        logger.info("Executing UMTS cell traffic period query...")
        df = pd.read_sql_query(query, engine)
        logger.info(f"Query executed successfully. Retrieved {len(df)} records")
        
        if len(df) > 0:
            logger.info(f"Date range: {df['init_date'].min()} to {df['end_date'].max()}")
        
        return df
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        return None

def insert_incremental_summary_to_db(df, engine):
    """Insert incremental summary data using SQLAlchemy to_sql method"""
    if df is None or df.empty:
        logger.warning("No incremental summary data to insert")
        return False
    
    try:
        # Use pandas to_sql for efficient bulk insert
        df.to_sql(
            'umts_cell_change_event',
            engine,
            schema='public',
            if_exists='append',
            index=False,
            method='multi'
        )
        
        logger.info(f"Successfully inserted {len(df)} records into umts_cell_change_event table")
        return True
        
    except Exception as e:
        logger.error(f"Error inserting data into database: {e}")
        return False

def process_cell_report(df):
    """
    Process the UMTS cell data to create a comprehensive report with cell additions/deletions
    and counts by band, vendor, and technology.
    
    Args:
        df (pandas.DataFrame): Raw query results
        
    Returns:
        pandas.DataFrame: Processed report with specified columns
    """
    if df is None or df.empty:
        logger.warning("No data to process")
        return None
    
    logger.info("Starting report processing...")
    
    # Convert dates once at the beginning
    df = df.copy()
    df['init_date'] = pd.to_datetime(df['init_date']).dt.date
    df['end_date'] = pd.to_datetime(df['end_date']).dt.date
    
    # Get unique dates only where there are actual changes
    init_dates = set(df['init_date'].unique())
    end_dates = set(df['end_date'].unique())
    change_dates = sorted(init_dates.union(end_dates))
    
    logger.info(f"Processing {len(change_dates)} unique change dates...")
    
    # Pre-process band and vendor mappings for UMTS
    def map_band(band):
        mapping = {
            'band_2_pcs': 'b2',     # UMTS Band II (1900 MHz PCS)
            'band_4_aws': 'b4',     # UMTS Band IV (AWS 1700/2100 MHz)
            'band_5_850': 'b5',     # UMTS Band V (850 MHz)
        }
        return mapping.get(band, 'x')
    
    def map_vendor(vendor):
        mapping = {
            'huawei': 'h3g',
            'ericsson': 'e3g',
            'nokia': 'n3g'
        }
        return mapping.get(vendor, 'x3g')
    
    df['band_mapped'] = df['band_indicator'].apply(map_band)
    df['vendor_mapped'] = df['vendor'].apply(map_vendor)
    df['band_vendor'] = df['band_mapped'] + '_' + df['vendor_mapped']
    
    report_data = []
    
    # Group by location and vendor for more efficient processing
    location_groups = df.groupby(['region', 'province', 'municipality', 'vendor', 'att_name'])
    
    total_groups = len(location_groups)
    logger.info(f"Processing {total_groups} location/vendor combinations...")
    
    for group_idx, ((region, province, municipality, vendor, att_name), group_data) in enumerate(location_groups):
        if group_idx % 50 == 0:  # Progress logging
            logger.info(f"Processing group {group_idx + 1}/{total_groups}")
        
        # Only process dates where this group has changes
        group_init_dates = set(group_data['init_date'].unique())
        group_end_dates = set(group_data['end_date'].unique())
        group_change_dates = group_init_dates.union(group_end_dates)
        
        # Initialize cumulative band counts for this group
        cumulative_band_counts = {
            'b2_h3g': 0, 'b2_e3g': 0, 'b2_n3g': 0,
            'b4_h3g': 0, 'b4_e3g': 0, 'b4_n3g': 0,
            'b5_h3g': 0, 'b5_e3g': 0, 'b5_n3g': 0,
            'x_h3g': 0, 'x_e3g': 0, 'x_n3g': 0
        }
        
        # Process dates in chronological order
        for date in sorted(group_change_dates):
            # Count additions and deletions for this date
            add_cell = len(group_data[group_data['init_date'] == date])
            delete_cell = len(group_data[group_data['end_date'] == date])
            
            if add_cell > 0 or delete_cell > 0:
                # Calculate changes for this specific date
                
                # Get cells added on this date
                added_cells = group_data[group_data['init_date'] == date]
                added_band_counts = {}
                if not added_cells.empty:
                    added_band_distribution = added_cells['band_vendor'].value_counts()
                    for band_vendor, count in added_band_distribution.items():
                        if band_vendor in cumulative_band_counts:
                            added_band_counts[band_vendor] = count
                            cumulative_band_counts[band_vendor] += count
                
                # Get cells deleted on this date
                deleted_cells = group_data[group_data['end_date'] == date]
                deleted_band_counts = {}
                if not deleted_cells.empty:
                    deleted_band_distribution = deleted_cells['band_vendor'].value_counts()
                    for band_vendor, count in deleted_band_distribution.items():
                        if band_vendor in cumulative_band_counts:
                            deleted_band_counts[band_vendor] = count
                            cumulative_band_counts[band_vendor] -= count
                            # Ensure we don't go below zero
                            if cumulative_band_counts[band_vendor] < 0:
                                cumulative_band_counts[band_vendor] = 0
                
                # Create the record with cumulative counts
                record = {
                    'region': region,
                    'province': province,
                    'municipality': municipality,
                    'vendor': vendor,
                    'att_name': att_name,
                    'date': date,
                    'add_cell': add_cell,
                    'delete_cell': delete_cell,
                    **cumulative_band_counts.copy()  # Use cumulative counts
                }
                
                report_data.append(record)
    
    if not report_data:
        logger.warning("No report data generated")
        return None
    
    logger.info("Creating final DataFrame...")
    report_df = pd.DataFrame(report_data)
    
    # Sort by date, region, province, municipality, vendor, att_name
    report_df = report_df.sort_values(['date', 'region', 'province', 'municipality', 'vendor', 'att_name'])
    
    logger.info(f"Generated report with {len(report_df)} rows")
    return report_df

def print_report_summary(df):
    """Print summary statistics of the report"""
    if df is None or df.empty:
        logger.warning("No report data to summarize")
        return
        
    print("\n" + "="*60)
    print("UMTS CELL REPORT SUMMARY")
    print("="*60)
    print(f"Total report records: {len(df)}")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"Total cells added: {df['add_cell'].sum()}")
    print(f"Total cells deleted: {df['delete_cell'].sum()}")
    
    print("\nCells added by date:")
    date_adds = df.groupby('date')['add_cell'].sum().sort_index()
    print(date_adds[date_adds > 0])
    
    print("\nCells deleted by date:")
    date_deletes = df.groupby('date')['delete_cell'].sum().sort_index()
    print(date_deletes[date_deletes > 0])
    
    print("\nTop 10 records by total band activity:")
    # Calculate total band activity (sum of all band columns)
    band_columns = [col for col in df.columns if col.startswith(('b2_', 'b4_', 'b5_', 'x_'))]
    df['total_band_activity'] = df[band_columns].sum(axis=1)
    top_activity = df.nlargest(10, 'total_band_activity')[['region', 'province', 'municipality', 'vendor', 'date', 'total_band_activity']]
    print(top_activity)
    
    print("="*60)

def create_incremental_summary(df, raw_df=None):
    """
    Create an incremental summary with cumulative cell counts and remarks
    
    Args:
        df (pandas.DataFrame): Processed report DataFrame
        raw_df (pandas.DataFrame): Raw query results with init_date and end_date columns
        
    Returns:
        pandas.DataFrame: Incremental summary with total_cell and remark columns
    """
    if df is None or df.empty:
        logger.warning("No data to create incremental summary")
        return None
    
    logger.info("Creating incremental summary...")
    
    # Remove vendor and total_band_activity columns if they exist, but keep original date columns for tracking
    summary_df = df.copy()
    columns_to_remove = ['vendor', 'total_band_activity']
    for col in columns_to_remove:
        if col in summary_df.columns:
            summary_df = summary_df.drop(columns=[col])
    
    # Use the raw DataFrame if provided, otherwise fall back to processed df
    if raw_df is not None:
        original_df = raw_df.copy()
        # Ensure raw data has proper date conversion and mappings
        original_df['init_date'] = pd.to_datetime(original_df['init_date']).dt.date
        original_df['end_date'] = pd.to_datetime(original_df['end_date']).dt.date
        
        # Apply band and vendor mappings to raw data
        def map_band(band):
            mapping = {
                'band_2_pcs': 'b2',     # UMTS Band II (1900 MHz PCS)
                'band_4_aws': 'b4',     # UMTS Band IV (AWS 1700/2100 MHz)
                'band_5_850': 'b5',     # UMTS Band V (850 MHz)
            }
            return mapping.get(band, 'x')
        
        def map_vendor(vendor):
            mapping = {
                'huawei': 'h3g',
                'ericsson': 'e3g',
                'nokia': 'n3g'
            }
            return mapping.get(vendor, 'x3g')
        
        original_df['band_mapped'] = original_df['band_indicator'].apply(map_band)
        original_df['vendor_mapped'] = original_df['vendor'].apply(map_vendor)
        original_df['band_vendor'] = original_df['band_mapped'] + '_' + original_df['vendor_mapped']
        
        # Initialize all possible band counts for raw data
        band_counts = {
            'b2_h3g': 0, 'b2_e3g': 0, 'b2_n3g': 0,
            'b4_h3g': 0, 'b4_e3g': 0, 'b4_n3g': 0,
            'b5_h3g': 0, 'b5_e3g': 0, 'b5_n3g': 0,
            'x_h3g': 0, 'x_e3g': 0, 'x_n3g': 0
        }
        
        # Add band columns to raw data (each row represents 1 cell with its band_vendor combination)
        for col in band_counts.keys():
            original_df[col] = 0
        
        # Set the appropriate band column to 1 for each row
        for idx, row in original_df.iterrows():
            band_vendor = row['band_vendor']
            if band_vendor in band_counts:
                original_df.at[idx, band_vendor] = 1
                
    else:
        logger.warning("No raw data provided, using processed data (may not have init_date/end_date)")
        original_df = df.copy()
    
    # Group by location and cell to track incremental changes, then by date to consolidate same-date entries
    location_groups = summary_df.groupby(['region', 'province', 'municipality', 'att_name'])
    original_location_groups = original_df.groupby(['region', 'province', 'municipality', 'att_name'])
    
    incremental_data = []
    
    for (region, province, municipality, att_name), group_data in location_groups:
        # Get the corresponding original group data with init_date and end_date
        original_group_data = original_location_groups.get_group((region, province, municipality, att_name))
        
        # First, consolidate records by date (sum up changes that happen on the same date)
        date_consolidated = group_data.groupby('date').agg({
            'add_cell': 'sum',
            'delete_cell': 'sum',
            # For band columns, we need to calculate what was actually added/deleted on this date
            # NOT the total active cells on this date
            **{col: 'sum' for col in group_data.columns if col.startswith(('b2_', 'b4_', 'b5_', 'x_'))}
        }).reset_index()
        
        # For proper tracking, we need to separate additions from deletions by date
        # Create separate dataframes for additions and deletions
        additions_by_date = {}
        deletions_by_date = {}
        
        for date in date_consolidated['date']:
            # Get cells that were added on this date
            added_cells = original_group_data[original_group_data['init_date'] == date]
            if not added_cells.empty:
                added_band_counts = added_cells.groupby('band_vendor').size()
                additions_by_date[date] = added_band_counts
            else:
                additions_by_date[date] = pd.Series(dtype=int)
            
            # Get cells that were deleted on this date
            deleted_cells = original_group_data[original_group_data['end_date'] == date]
            if not deleted_cells.empty:
                deleted_band_counts = deleted_cells.groupby('band_vendor').size()
                deletions_by_date[date] = deleted_band_counts
            else:
                deletions_by_date[date] = pd.Series(dtype=int)
        
        # Sort by date to ensure chronological order
        date_consolidated = date_consolidated.sort_values('date')
        
        cumulative_total = 0
        previous_band_totals = {}
        
        for _, row in date_consolidated.iterrows():
            current_date = row['date']
            
            # Calculate incremental total
            net_change = row['add_cell'] - row['delete_cell']
            cumulative_total += net_change
            
            # Generate remark based on actual additions and deletions on this specific date
            remark_parts = []
            
            # Get current band totals for this date (cumulative state after all changes)
            current_band_totals = {}
            band_columns = [col for col in row.index if col.startswith(('b2_', 'b4_', 'b5_', 'x_'))]
            for col in band_columns:
                current_band_totals[col] = row[col]
            
            # SPECIAL CASE: If this is a pure deletion that results in zero total cells, 
            # all band counts should be zero (override the consolidated values)
            if row['delete_cell'] > 0 and row['add_cell'] == 0 and cumulative_total == 0:
                for col in band_columns:
                    current_band_totals[col] = 0
            
            # For additions, use the additions_by_date dictionary
            if row['add_cell'] > 0:
                add_details = []
                if current_date in additions_by_date:
                    for band_vendor, count in additions_by_date[current_date].items():
                        if count > 0:
                            add_details.append(f"{count:02d} {band_vendor}")
                if add_details:
                    remark_parts.append(f"add {', '.join(add_details)}")
            
            # For deletions, use the deletions_by_date dictionary
            if row['delete_cell'] > 0:
                del_details = []
                if current_date in deletions_by_date:
                    for band_vendor, count in deletions_by_date[current_date].items():
                        if count > 0:
                            del_details.append(f"{count:02d} {band_vendor}")
                if del_details:
                    remark_parts.append(f"del {', '.join(del_details)}")
            
            remark = ", ".join(remark_parts) if remark_parts else ""
            
            # Update previous totals for next iteration
            previous_band_totals = current_band_totals.copy()
            
            # Create new row for incremental summary
            new_row = {
                'region': region,
                'province': province,
                'municipality': municipality,
                'att_name': att_name,
                'date': current_date,
                'add_cell': row['add_cell'],
                'delete_cell': row['delete_cell'],
                'total_cell': cumulative_total,
                'remark': remark
            }
            
            # Add all band columns
            band_columns = [col for col in row.index if col.startswith(('b2_', 'b4_', 'b5_', 'x_'))]
            for col in band_columns:
                new_row[col] = current_band_totals[col]
            
            incremental_data.append(new_row)
    
    if not incremental_data:
        logger.warning("No incremental summary data generated")
        return None
    
    incremental_df = pd.DataFrame(incremental_data)
    
    # Reorder columns to match the required format
    base_columns = ['region', 'province', 'municipality', 'att_name', 'date', 'add_cell', 'delete_cell', 'total_cell', 'remark']
    band_columns = [col for col in incremental_df.columns if col.startswith(('b2_', 'b4_', 'b5_', 'x_'))]
    band_columns.sort()  # Sort band columns alphabetically
    
    final_columns = base_columns + band_columns
    incremental_df = incremental_df[final_columns]
    
    logger.info(f"Created incremental summary with {len(incremental_df)} rows")
    return incremental_df

def save_report_summary(df, filename=None):
    """Save summary statistics of the report to a text file"""
    if df is None or df.empty:
        logger.warning("No report data to save summary for")
        return None
        
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"umts_cell_report_summary_{timestamp}.txt"
    
    try:
        output_path = os.path.join(ROOT_DIRECTORY, "output", filename)
        
        with open(output_path, 'w') as f:
            f.write("="*60 + "\n")
            f.write("UMTS CELL REPORT SUMMARY\n")
            f.write("="*60 + "\n")
            f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total report records: {len(df)}\n")
            f.write(f"Date range: {df['date'].min()} to {df['date'].max()}\n")
            f.write(f"Total cells added: {df['add_cell'].sum()}\n")
            f.write(f"Total cells deleted: {df['delete_cell'].sum()}\n\n")
            
            f.write("Cells added by date:\n")
            date_adds = df.groupby('date')['add_cell'].sum().sort_index()
            adds_with_values = date_adds[date_adds > 0]
            if not adds_with_values.empty:
                f.write(adds_with_values.to_string() + "\n\n")
            else:
                f.write("No additions found\n\n")
            
            f.write("Cells deleted by date:\n")
            date_deletes = df.groupby('date')['delete_cell'].sum().sort_index()
            deletes_with_values = date_deletes[date_deletes > 0]
            if not deletes_with_values.empty:
                f.write(deletes_with_values.to_string() + "\n\n")
            else:
                f.write("No deletions found\n\n")
            
            f.write("Top 10 records by total band activity:\n")
            # Calculate total band activity (sum of all band columns)
            band_columns = [col for col in df.columns if col.startswith(('b2_', 'b4_', 'b5_', 'x_'))]
            df_temp = df.copy()
            df_temp['total_band_activity'] = df_temp[band_columns].sum(axis=1)
            top_activity = df_temp.nlargest(10, 'total_band_activity')[['region', 'province', 'municipality', 'att_name', 'date', 'total_band_activity']]
            f.write(top_activity.to_string(index=False) + "\n")
            f.write("="*60 + "\n")
        
        logger.info(f"Summary saved to {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Error saving summary to file: {e}")
        return None

def umts_cell_change_process():
    """Run the unified UMTS cell change event processing workflow with SQLAlchemy"""
    print("=" * 60)
    print("UMTS CELL CHANGE EVENT PROCESSING")
    print("=" * 60)
    
    # Step 1: Truncate table
    truncate_success = truncate_umts_cell_change_event()
    if not truncate_success:
        return False
    
    # Step 2: Process data
    engine = create_connection()
    if engine is None:
        return False
    
    try:
        results_df = execute_umts_cell_query(engine)
        
        if results_df is not None:
            report_df = process_cell_report(results_df)
            
            if report_df is not None:
                print_report_summary(report_df)
                incremental_df = create_incremental_summary(report_df, results_df)
                
                if incremental_df is not None:
                    if insert_incremental_summary_to_db(incremental_df, engine):
                        summary_file = save_report_summary(incremental_df)
                        logger.info("Processing completed successfully")
                        return True                    
        return False
        
    finally:
        engine.dispose()

if __name__ == "__main__":
    # Run UMTS cell change event processing workflow
    success = umts_cell_change_process()
    
    if success:
        print("\nAll processing completed successfully!")
    else:
        print("\nProcessing failed. Check logs above for details.")
