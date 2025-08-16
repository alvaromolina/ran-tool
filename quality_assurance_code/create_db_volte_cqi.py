import psycopg2
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

def create_db_volte_cqi_analytics():
    # Replace these variables with your PostgreSQL credentials
    username = POSTGRES_USERNAME
    password = POSTGRES_PASSWORD
    host = POSTGRES_HOST
    port = POSTGRES_PORT
    database_name = POSTGRES_DB

    try:
        # Connect to the default database 'postgres' to create a new database
        conn = psycopg2.connect(
            user=username,
            password=password,
            host=host,
            port=port,
            database='postgres'
        )

        # Set autocommit to True to create the new database
        conn.autocommit = True

        # Create a cursor to execute SQL commands
        cursor = conn.cursor()

        # SQL command to create the new database
        drop_db_query = f"DROP DATABASE IF EXISTS {database_name} WITH (FORCE);"
        create_db_query = f"CREATE DATABASE {database_name};"

        # Execute the SQL command
        cursor.execute(drop_db_query)
        print(f"Database '{database_name}' deleted successfully.")

        cursor.execute(create_db_query)
        print(f"Database '{database_name}' created successfully.")

        # Close the cursor and connection
        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"Error creating database: {e}")

def create_table_volte_cqi_vendor_daily():
    username = POSTGRES_USERNAME
    password = POSTGRES_PASSWORD
    host = POSTGRES_HOST
    port = POSTGRES_PORT
    database_name = POSTGRES_DB

    # SQL command to drop and create the volte_cqi_vendor_daily table
    create_table_query = """
        DROP TABLE IF EXISTS volte_cqi_vendor_daily;
        CREATE TABLE volte_cqi_vendor_daily (
            id SERIAL PRIMARY KEY,
            date DATE,
            region TEXT,
            province TEXT,
            municipality TEXT,
            site_att TEXT,
            volte_cqi_e FLOAT,
            acc_volte_e FLOAT,
            erab_drop_qci1_e FLOAT,
            erab_drop_qci5_e FLOAT,
            srvcc_rate_e FLOAT,
            user_traffic_volte_e FLOAT, 
            volte_cqi_h FLOAT,
            acc_volte_h FLOAT,
            erab_drop_qci1_h FLOAT,
            erab_drop_qci5_h FLOAT,
            srvcc_rate_h FLOAT,
            user_traffic_volte_h FLOAT,
            volte_cqi_n FLOAT,
            acc_volte_n FLOAT,
            erab_drop_qci1_n FLOAT,
            erab_drop_qci5_n FLOAT,
            srvcc_rate_n FLOAT,
            user_traffic_volte_n FLOAT, 
            volte_cqi_s FLOAT,
            acc_volte_s FLOAT,
            erab_drop_qci1_s FLOAT,
            erab_drop_qci5_s FLOAT,
            srvcc_rate_s FLOAT,
            user_traffic_volte_s FLOAT
        );
    """

    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            user=username,
            password=password,
            host=host,
            port=port,
            database=database_name
        )

        # Create a cursor to execute the SQL commands
        cursor = conn.cursor()

        # Execute the SQL command to drop and create the table
        cursor.execute(create_table_query)
        conn.commit()
        print("Table 'volte_cqi_vendor_daily' created successfully.")

        # Close the cursor
        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"Error creating table: {e}")

def create_table_volte_cqi_metrics_weekly():
    username = POSTGRES_USERNAME
    password = POSTGRES_PASSWORD
    host = POSTGRES_HOST
    port = POSTGRES_PORT
    database_name = POSTGRES_DB

    # SQL command to drop and create the volte_cqi_vendor_daily table
    create_table_query = """
        DROP TABLE IF EXISTS volte_cqi_metrics_weekly;
        CREATE TABLE volte_cqi_metrics_weekly (
            id SERIAL PRIMARY KEY,
            week TEXT,
            region TEXT,
            site_att TEXT,
            date DATE,
            volte_cqi FLOAT,
            acc_volte FLOAT,
            erab_drop_qci1 FLOAT,
            erab_drop_qci5 FLOAT,
            srvcc_rate FLOAT
        );
    """

    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            user=username,
            password=password,
            host=host,
            port=port,
            database=database_name
        )

        # Create a cursor to execute the SQL commands
        cursor = conn.cursor()

        # Execute the SQL command to drop and create the table
        cursor.execute(create_table_query)
        conn.commit()
        print("Table 'volte_cqi_metrics_weekly' created successfully.")

        # Close the cursor
        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"Error creating table: {e}")