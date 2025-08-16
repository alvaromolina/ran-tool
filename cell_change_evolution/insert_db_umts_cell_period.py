import os
import dotenv
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text

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
        return engine
    except Exception as e:
        print(f"Error creating database connection: {e}")
        return None

def truncate_umts_cell_traffic_period():
    """Truncate the umts_cell_traffic_period table using SQLAlchemy"""
    try:
        engine = create_connection()
        if engine is None:
            return False
        
        print(f"Truncating umts_cell_traffic_period table...")
        
        with engine.connect() as connection:
            connection.execute(text("TRUNCATE TABLE public.umts_cell_traffic_period;"))
            connection.commit()
        
        print(f"Table truncated successfully")
        print()
        
        return True
        
    except Exception as e:
        print(f"Error in truncate_umts_cell_traffic_period: {e}")
        return False

def umts_cell_init_date_p3():
    """Find the earliest date for each cell with 3 consecutive days using SQLAlchemy"""
    try:
        engine = create_connection()
        if engine is None:
            return False
        
        print(f"Processing init_date detection (p3 = 3 consecutive days)")
        print(f"Processing all data in umts_cell_traffic_daily table")
        print(f"Algorithm: Window function with ROW_NUMBER()")
        print()
        
        query = text("""
        WITH traffic_data AS (
            SELECT 
                cell,
                vendor,
                date,
                traffic_d_user_ps_gb,
                ROW_NUMBER() OVER (PARTITION BY cell, vendor ORDER BY date) AS rn,
                date - (ROW_NUMBER() OVER (PARTITION BY cell, vendor ORDER BY date) * INTERVAL '1 day') AS date_group
            FROM public.umts_cell_traffic_daily
            WHERE traffic_d_user_ps_gb > 0
        ),
        consecutive_groups AS (
            SELECT 
                cell,
                vendor,
                date_group,
                MIN(date) AS group_start_date,
                MAX(date) AS group_end_date,
                COUNT(*) AS consecutive_days
            FROM traffic_data
            GROUP BY cell, vendor, date_group
        ),
        valid_init_dates AS (
            SELECT 
                cell,
                vendor,
                group_start_date AS init_date
            FROM consecutive_groups
            WHERE consecutive_days >= 3
        ),
        earliest_init_dates AS (
            SELECT 
                cell,
                vendor,
                MIN(init_date) AS init_date
            FROM valid_init_dates
            GROUP BY cell, vendor
        )
        INSERT INTO public.umts_cell_traffic_period (cell, vendor, init_date, end_date, period)
        SELECT 
            cell,
            vendor,
            init_date,
            NULL as end_date,
            3 as period
        FROM earliest_init_dates;
        """)
        
        with engine.connect() as connection:
            result = connection.execute(query)
            rows_affected = result.rowcount
            connection.commit()
        
        print(f"Init_date processing completed successfully")
        print(f"Rows affected: {rows_affected:,}")
        print(f"Approach: INSERT new calculations")
        print()
        
        return True
        
    except Exception as e:
        print(f"Error in umts_cell_init_date_p3: {e}")
        return False

def umts_cell_end_date_p3():
    """Find the latest date for each cell using reverse window functions with SQLAlchemy"""
    try:
        engine = create_connection()
        if engine is None:
            return False
        
        print(f"Processing end_date detection (p3 = 3 consecutive days)")
        print(f"Processing all data in umts_cell_traffic_daily table")
        print(f"Algorithm: Reverse window function with ROW_NUMBER()")
        print()
        
        query = text("""
        WITH traffic_data AS (
            SELECT 
                cell,
                vendor,
                date,
                traffic_d_user_ps_gb,
                ROW_NUMBER() OVER (PARTITION BY cell, vendor ORDER BY date DESC) AS rn,
                date + (ROW_NUMBER() OVER (PARTITION BY cell, vendor ORDER BY date DESC) * INTERVAL '1 day') AS date_group
            FROM public.umts_cell_traffic_daily
            WHERE traffic_d_user_ps_gb > 0
        ),
        consecutive_groups AS (
            SELECT 
                cell,
                vendor,
                date_group,
                MAX(date) AS group_end_date,
                MIN(date) AS group_start_date,
                COUNT(*) AS consecutive_days
            FROM traffic_data
            GROUP BY cell, vendor, date_group
        ),
        valid_end_dates AS (
            SELECT 
                cell,
                vendor,
                group_end_date AS end_date
            FROM consecutive_groups
            WHERE consecutive_days >= 3
        ),
        latest_end_dates AS (
            SELECT 
                cell,
                vendor,
                MAX(end_date) AS end_date
            FROM valid_end_dates
            GROUP BY cell, vendor
        ),
        traffic_metrics AS (
            SELECT 
                p.cell,
                p.vendor,
                p.init_date,
                e.end_date
            FROM public.umts_cell_traffic_period p
            JOIN latest_end_dates e ON p.cell = e.cell AND p.vendor = e.vendor
            WHERE p.init_date IS NOT NULL
        )
        UPDATE public.umts_cell_traffic_period
        SET 
            end_date = tm.end_date,
            created_at = CURRENT_TIMESTAMP
        FROM traffic_metrics tm
        WHERE umts_cell_traffic_period.cell = tm.cell 
          AND umts_cell_traffic_period.vendor = tm.vendor
          AND umts_cell_traffic_period.init_date = tm.init_date;
        """)
        
        with engine.connect() as connection:
            result = connection.execute(query)
            rows_affected = result.rowcount
            connection.commit()
        
        print(f"End_date processing completed successfully")
        print(f"Rows affected: {rows_affected:,}")
        print(f"Approach: UPDATE existing records")
        print(f"Updated: end_date field")
        print()
        
        return True
        
    except Exception as e:
        print(f"Error in umts_cell_end_date_p3: {e}")
        return False

def umts_cell_period_process():
    """Run the unified UMTS cell traffic period processing workflow with SQLAlchemy"""
    print("=" * 60)
    print("UMTS CELL TRAFFIC PERIOD UNIFIED PROCESSING")
    print("=" * 60)
    print(f"Processing all data in umts_cell_traffic_daily table")
    print(f"Period: p3 (3 consecutive days)")
    print(f"Database: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    print(f"Username: {POSTGRES_USERNAME}")
    print(f"Root Directory: {ROOT_DIRECTORY}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print()
    
    # Step 1: Truncate table
    print("STEP 1: Truncating table...")
    print("-" * 40)
    truncate_success = truncate_umts_cell_traffic_period()
    
    if not truncate_success:
        print("Table truncation failed. Stopping workflow.")
        return False
    
    # Step 2: Process init_dates
    print("STEP 2: Processing init_dates...")
    print("-" * 40)
    init_success = umts_cell_init_date_p3()
    
    if not init_success:
        print("Init_date processing failed. Stopping workflow.")
        return False
    
    # Step 3: Process end_dates
    print("STEP 3: Processing end_dates...")
    print("-" * 40)
    end_success = umts_cell_end_date_p3()
    
    if not end_success:
        print("End_date processing failed.")
        return False
    
    # Summary
    print("=" * 60)
    print("UNIFIED PROCESSING COMPLETED SUCCESSFULLY")
    print("=" * 60)
    print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Summary:")
    print("   - Table:      Truncated successfully")
    print("   - Init_dates: Processed (earliest dates with 3+ consecutive days)")
    print("   - End_dates:  Processed (latest dates with 3+ consecutive days)")
    print("   - Period:     Set to 3 (p3 = 3 consecutive days)")
    print("   - Table:      umts_cell_traffic_period")
    print("=" * 60)
    
    return True

if __name__ == "__main__":
    success = umts_cell_period_process()
    
    if success:
        print("\nAll processing completed successfully!")
    else:
        print("\nProcessing failed. Check logs above for details.")

