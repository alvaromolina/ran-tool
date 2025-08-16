import os
import dotenv
import pandas as pd
from datetime import datetime
import logging
from sqlalchemy import create_engine, text
from cell_change_processor import process_cell_report, create_incremental_summary

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

def truncate_lte_cell_change_event():
    """Truncate the lte_cell_change_event table using SQLAlchemy"""
    try:
        engine = create_connection()
        if engine is None:
            return False
        
        print(f"Truncating lte_cell_change_event table...")
        
        with engine.connect() as connection:
            connection.execute(text("TRUNCATE TABLE public.lte_cell_change_event;"))
            connection.commit()
        
        print(f"Table truncated successfully")
        return True
        
    except Exception as e:
        print(f"Error in truncate_lte_cell_change_event: {e}")
        return False

def execute_lte_cell_query(engine):
    """Execute the LTE cell traffic period query with SQLAlchemy"""
    query = """
    SELECT 
        mc.region,
        mc.province,
        mc.municipality,
        ltp.vendor,
        mc.att_name,
        mc.att_tech,
        mc.band_indicator,
        ltp.init_date,
        ltp.end_date
    FROM 
        public.lte_cell_traffic_period AS ltp
    LEFT JOIN 
        public.master_cell_total AS mc
    ON 
        ltp.cell = mc.cell_name
    WHERE 
        ltp.cell IS NOT NULL;
    """
    
    try:
        logger.info("Executing LTE cell traffic period query...")
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
            'lte_cell_change_event',
            engine,
            schema='public',
            if_exists='append',
            index=False,
            method='multi'
        )
        
        logger.info(f"Successfully inserted {len(df)} records into lte_cell_change_event table")
        return True
        
    except Exception as e:
        logger.error(f"Error inserting data into database: {e}")
        return False

def lte_cell_change_process():
    """Run the unified LTE cell change event processing workflow with SQLAlchemy"""
    print("=" * 60)
    print("LTE CELL CHANGE EVENT PROCESSING")
    print("=" * 60)
    
    # Step 1: Truncate table
    truncate_success = truncate_lte_cell_change_event()
    if not truncate_success:
        return False
    
    # Step 2: Process data
    engine = create_connection()
    if engine is None:
        return False
    
    try:
        results_df = execute_lte_cell_query(engine)
        
        if results_df is not None:
            report_df = process_cell_report(results_df)
            
            if report_df is not None:
                incremental_df = create_incremental_summary(report_df, results_df)
                
                if incremental_df is not None:
                    if insert_incremental_summary_to_db(incremental_df, engine):
                        logger.info("Processing completed successfully")
                        return True                    
        return False
        
    finally:
        engine.dispose()

if __name__ == "__main__":
    # Run LTE cell change event processing workflow
    success = lte_cell_change_process()
    
    if success:
        print("\nAll processing completed successfully!")
    else:
        print("\nProcessing failed. Check logs above for details.")