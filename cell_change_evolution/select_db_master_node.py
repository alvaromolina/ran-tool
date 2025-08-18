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

def get_provinces():
    """
    Get all unique provinces from master_node_total table
    
    Returns:
        list: List of unique province names
    """
    engine = create_connection()
    if engine is None:
        return []
    
    try:
        query = """
        SELECT DISTINCT province 
        FROM public.master_node_total 
        WHERE province IS NOT NULL 
        ORDER BY province;
        """
        
        df = pd.read_sql_query(query, engine)
        provinces = df['province'].tolist()
        
        print(f"Retrieved {len(provinces)} unique provinces")
        return provinces
        
    except Exception as e:
        print(f"Error fetching provinces: {e}")
        return []
    finally:
        engine.dispose()

def get_municipalities():
    """
    Get all unique municipalities from master_node_total table
    
    Returns:
        list: List of unique municipality names
    """
    engine = create_connection()
    if engine is None:
        return []
    
    try:
        query = """
        SELECT DISTINCT municipality 
        FROM public.master_node_total 
        WHERE municipality IS NOT NULL 
        ORDER BY municipality;
        """
        
        df = pd.read_sql_query(query, engine)
        municipalities = df['municipality'].tolist()
        
        print(f"Retrieved {len(municipalities)} unique municipalities")
        return municipalities
        
    except Exception as e:
        print(f"Error fetching municipalities: {e}")
        return []
    finally:
        engine.dispose()

def get_att_names():
    """
    Get all unique att_name from master_node_total table
    
    Returns:
        list: List of unique att_name values
    """
    engine = create_connection()
    if engine is None:
        return []
    
    try:
        query = """
        SELECT DISTINCT att_name 
        FROM public.master_node_total 
        WHERE att_name IS NOT NULL 
        ORDER BY att_name;
        """
        
        df = pd.read_sql_query(query, engine)
        att_names = df['att_name'].tolist()
        
        print(f"Retrieved {len(att_names)} unique att_names")
        return att_names
        
    except Exception as e:
        print(f"Error fetching att_names: {e}")
        return []
    finally:
        engine.dispose()

def get_max_date():
    """
    Get the minimum of the maximum end_dates from lte and umts tables
    
    Returns:
        date: Min of (max lte end_date, max umts end_date), or None if no data
    """
    engine = create_connection()
    if engine is None:
        return None
    
    try:
        # Check table existence using to_regclass and get MAX(end_date) if present
        with engine.connect() as connection:
            lte_exists = connection.execute(text("SELECT to_regclass('public.lte_cell_traffic_period') IS NOT NULL")).scalar()
            umts_exists = connection.execute(text("SELECT to_regclass('public.umts_cell_traffic_period') IS NOT NULL")).scalar()

            lte_max = None
            umts_max = None

            if lte_exists:
                try:
                    lte_max = connection.execute(
                        text("SELECT MAX(end_date) FROM public.lte_cell_traffic_period WHERE end_date IS NOT NULL")
                    ).scalar()
                except Exception as e:
                    print(f"Warning: failed to get LTE max end_date: {e}")

            if umts_exists:
                try:
                    umts_max = connection.execute(
                        text("SELECT MAX(end_date) FROM public.umts_cell_traffic_period WHERE end_date IS NOT NULL")
                    ).scalar()
                except Exception as e:
                    print(f"Warning: failed to get UMTS max end_date: {e}")

            # If both traffic-period maxima are missing, fall back to CQI daily tables (MAX(time))
            if not lte_max and not umts_max:
                lte_cqi_exists = connection.execute(text("SELECT to_regclass('public.lte_cqi_daily') IS NOT NULL")).scalar()
                umts_cqi_exists = connection.execute(text("SELECT to_regclass('public.umts_cqi_daily') IS NOT NULL")).scalar()
                lte_cqi_max = None
                umts_cqi_max = None
                if lte_cqi_exists:
                    try:
                        lte_cqi_max = connection.execute(
                            text("SELECT MAX(date) FROM public.lte_cqi_daily WHERE date IS NOT NULL")
                        ).scalar()
                    except Exception as e:
                        print(f"Warning: failed to get LTE CQI max date: {e}")
                if umts_cqi_exists:
                    try:
                        umts_cqi_max = connection.execute(
                            text("SELECT MAX(date) FROM public.umts_cqi_daily WHERE date IS NOT NULL")
                        ).scalar()
                    except Exception as e:
                        print(f"Warning: failed to get UMTS CQI max date: {e}")
                # Normalize to dates if datetimes
                def _to_date(x):
                    try:
                        return x.date() if hasattr(x, 'date') else x
                    except Exception:
                        return x
                lte_cqi_max = _to_date(lte_cqi_max)
                umts_cqi_max = _to_date(umts_cqi_max)
                if lte_cqi_max and umts_cqi_max:
                    final_max = min(lte_cqi_max, umts_cqi_max)
                else:
                    final_max = lte_cqi_max or umts_cqi_max or None
            else:
                # Use traffic-period maxima when available
                if lte_max and umts_max:
                    final_max = min(lte_max, umts_max)
                else:
                    final_max = lte_max or umts_max or None

        print(f"Retrieved max date: {final_max}")
        return final_max

    except Exception as e:
        print(f"Error fetching max date: {e}")
        return None
    
    finally:
        engine.dispose()

if __name__ == "__main__":
    provinces = get_provinces()
    print("Available provinces:")
    for province in provinces:
        print(f"  - {province}")
    
    print("\n")
    
    municipalities = get_municipalities()
    print("Available municipalities:")
    for municipality in municipalities:
        print(f"  - {municipality}")
        
    print("\n")
    
    att_names = get_att_names()
    print("Available att_names:")
    for att_name in att_names:
        print(f"  - {att_name}")
        
    print("\n")
    
    date = get_max_date()
    print("Available dates:")
    print(f"  - {date if date else 'No data available'}")