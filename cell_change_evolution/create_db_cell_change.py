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

def create_table_lte_cell_traffic_period():
    create_table_query = """
        DROP TABLE IF EXISTS lte_cell_traffic_period;
        CREATE TABLE IF NOT EXISTS public.lte_cell_traffic_period (
            cell VARCHAR(255) NOT NULL,
            vendor VARCHAR(255) NOT NULL,
            init_date DATE NOT NULL,
            end_date DATE NULL,
            period INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (cell, vendor, init_date)
        );
        """

    try:
        engine = create_connection()
        if engine is None:
            return False
            
        with engine.connect() as connection:
            connection.execute(text(create_table_query))
            connection.commit()
            
        print("Table 'lte_cell_traffic_period' created successfully.")
        return True

    except Exception as e:
        print(f"Error creating table: {e}")
        return False
    finally:
        if engine:
            engine.dispose()

def create_table_umts_cell_change_event():
    create_table_query = """
        DROP TABLE IF EXISTS umts_cell_change_event;
        CREATE TABLE IF NOT EXISTS public.umts_cell_change_event (
            region VARCHAR(255) NOT NULL,
            province VARCHAR(255) NOT NULL,
            municipality VARCHAR(255) NOT NULL,
            att_name VARCHAR(255) NOT NULL,
            date DATE NOT NULL,
            add_cell INTEGER,
            delete_cell INTEGER,
            total_cell INTEGER,
            remark VARCHAR(255),
            b2_h3g INTEGER,
            b2_e3g INTEGER,
            b2_n3g INTEGER,
            b4_h3g INTEGER,
            b4_e3g INTEGER,
            b4_n3g INTEGER,
            b5_h3g INTEGER,
            b5_e3g INTEGER,
            b5_n3g INTEGER,
            x_h3g INTEGER,
            x_e3g INTEGER,
            x_n3g INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (region, province, municipality, att_name, date)
        );
        """

    try:
        engine = create_connection()
        if engine is None:
            return False
            
        with engine.connect() as connection:
            connection.execute(text(create_table_query))
            connection.commit()
            
        print("Table 'umts_cell_change_event' created successfully.")
        return True

    except Exception as e:
        print(f"Error creating table: {e}")
        return False
    finally:
        if engine:
            engine.dispose()

def create_table_umts_cell_traffic_period():
    create_table_query = """
        DROP TABLE IF EXISTS umts_cell_traffic_period;
        CREATE TABLE IF NOT EXISTS public.umts_cell_traffic_period (
            cell VARCHAR(255) NOT NULL,
            vendor VARCHAR(255) NOT NULL,
            init_date DATE NOT NULL,
            end_date DATE NULL,
            period INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (cell, vendor, init_date)
        );
        """

    try:
        engine = create_connection()
        if engine is None:
            return False
            
        with engine.connect() as connection:
            connection.execute(text(create_table_query))
            connection.commit()
            
        print("Table 'umts_cell_traffic_period' created successfully.")
        return True

    except Exception as e:
        print(f"Error creating table: {e}")
        return False
    finally:
        if engine:
            engine.dispose()

def create_table_lte_cell_change_event():
    create_table_query = """
        DROP TABLE IF EXISTS lte_cell_change_event;
        CREATE TABLE IF NOT EXISTS public.lte_cell_change_event (
            region VARCHAR(255) NOT NULL,
            province VARCHAR(255) NOT NULL,
            municipality VARCHAR(255) NOT NULL,
            att_name VARCHAR(255) NOT NULL,
            date DATE NOT NULL,
            add_cell INTEGER,
            delete_cell INTEGER,
            total_cell INTEGER,
            remark VARCHAR(255),
            b2_h4g INTEGER,
            b2_e4g INTEGER,
            b2_n4g INTEGER,
            b2_s4g INTEGER,
            b4_h4g INTEGER,
            b4_e4g INTEGER,
            b4_n4g INTEGER,
            b4_s4g INTEGER,
            b5_h4g INTEGER,
            b5_e4g INTEGER,
            b5_n4g INTEGER,
            b5_s4g INTEGER,
            b7_h4g INTEGER,
            b7_e4g INTEGER,
            b7_n4g INTEGER,
            b7_s4g INTEGER,
            b26_h4g INTEGER,
            b26_e4g INTEGER,
            b26_n4g INTEGER,
            b26_s4g INTEGER,
            b42_h4g INTEGER,
            b42_e4g INTEGER,
            b42_n4g INTEGER,
            b42_s4g INTEGER,
            x_h4g INTEGER,
            x_e4g INTEGER,
            x_n4g INTEGER,
            x_s4g INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (region, province, municipality, att_name, date)
        );
        """

    try:
        engine = create_connection()
        if engine is None:
            return False
            
        with engine.connect() as connection:
            connection.execute(text(create_table_query))
            connection.commit()
            
        print("Table 'lte_cell_change_event' created successfully.")
        return True

    except Exception as e:
        print(f"Error creating table: {e}")
        return False
    finally:
        if engine:
            engine.dispose()

def get_last_date(table):
    # SQL command to get the maximum date from the lte_cqi_daily table
    last_date_query = f"SELECT MAX(date) FROM {table};"

    try:
        engine = create_connection()
        if engine is None:
            return None
            
        with engine.connect() as connection:
            result = connection.execute(text(last_date_query))
            last_date = result.fetchone()[0]  # Fetch the result

        return last_date

    except Exception as e:
        print(f"Error fetching last date: {e}")
        return None

def delete_newer_than(table, date):
    # SQL command to delete records newer than a specified date from the table
    delete_query = f"DELETE FROM {table} WHERE date > %s;"

    try:
        engine = create_connection()
        if engine is None:
            return False
            
        with engine.connect() as connection:
            connection.execute(text(delete_query), (date,))
            connection.commit()

        print(f"Records newer than {date} have been deleted from {table}.")
        return True

    except Exception as e:
        print(f"Error deleting records: {e}")
        return False

def delete_all(table):
    # SQL command to delete records newer than a specified date from the table
    delete_query = f"DELETE FROM {table};"

    try:
        engine = create_connection()
        if engine is None:
            return False
            
        with engine.connect() as connection:
            connection.execute(text(delete_query))
            connection.commit()

        print(f"Records have been deleted from {table}.")
        return True

    except Exception as e:
        print(f"Error deleting records: {e}")
        return False

def truncate_table(table):
    # SQL command to remove all rows from the specified table
    truncate_query = f"TRUNCATE TABLE {table};"

    try:
        engine = create_connection()
        if engine is None:
            return False
            
        with engine.connect() as connection:
            connection.execute(text(truncate_query))
            connection.commit()

        print(f"Table '{table}' has been successfully truncated.")
        return True

    except Exception as e:
        print(f"Error truncating table '{table}': {e}")
        return False
    finally:
        if engine:
            engine.dispose()