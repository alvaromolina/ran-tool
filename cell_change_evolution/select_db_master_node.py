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
        query = """
        SELECT MIN(max_end_date) as final_max_date
        FROM (
            SELECT MAX(end_date) as max_end_date FROM public.lte_cell_traffic_period WHERE end_date IS NOT NULL
            UNION ALL
            SELECT MAX(end_date) as max_end_date FROM public.umts_cell_traffic_period WHERE end_date IS NOT NULL
        ) max_dates;
        """
        
        with engine.connect() as connection:
            result = connection.execute(text(query))
            row = result.fetchone()
            max_date = row[0] if row and row[0] else None
        
        print(f"Retrieved max date: {max_date}")
        return max_date
        
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