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

def create_db_quality_analytics():
    try:
        # Connect to the default database 'postgres' to create a new database
        conn = psycopg2.connect(
            user=POSTGRES_USERNAME,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database="postgres"
        )

        # Set autocommit to True to allow database creation
        conn.autocommit = True

        # Create a cursor to execute SQL commands
        cursor = conn.cursor()

        # SQL commands to drop and create the new database
        drop_db_query = f"DROP DATABASE IF EXISTS {POSTGRES_DB};"
        create_db_query = f"CREATE DATABASE {POSTGRES_DB};"

        # Execute commands to drop and create the database
        cursor.execute(drop_db_query)
        print(f"Database '{POSTGRES_DB}' deleted successfully.")

        cursor.execute(create_db_query)
        print(f"Database '{POSTGRES_DB}' created successfully.")

        # Close the cursor and connection for 'postgres'
        cursor.close()
        conn.close()

        # Connect to the newly created database to enable extensions
        conn = psycopg2.connect(
            user=POSTGRES_USERNAME,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB
        )

        # Create a new cursor for the new database
        cursor = conn.cursor()

        # Enable the required extensions
        extensions = [
            "plpgsql",
            "postgis",
            "postgis_raster",
            "pgrouting",
            "postgis_topology",
            "address_standardizer",
            "address_standardizer_data_us",
            "postgis_sfcgal",
            "pointcloud",
            "pointcloud_postgis",
            "ogr_fdw",
            "fuzzystrmatch",
            "postgis_tiger_geocoder",
            "h3",
            "h3_postgis"
        ]

        for extension in extensions:
            try:
                cursor.execute(f"CREATE EXTENSION IF NOT EXISTS {extension};")
                print(f"Extension '{extension}' enabled successfully.")
            except psycopg2.Error as e:
                print(f"Error enabling extension '{extension}': {e}")

        # Commit changes and close the connection
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Database '{POSTGRES_DB}' is ready with all extensions enabled.")

    except psycopg2.Error as e:
        print(f"Error creating or configuring database: {e}")


def create_table_master_cell_total():
    # Replace these variables with your PostgreSQL credentials
    username = POSTGRES_USERNAME
    password = POSTGRES_PASSWORD
    host = POSTGRES_HOST
    port = POSTGRES_PORT
    database_name = POSTGRES_DB

    # SQL command to drop and create the master_cell_total table
    create_table_query = """
        DROP TABLE IF EXISTS master_cell_total;
        CREATE TABLE master_cell_total (
            id SERIAL PRIMARY KEY,
            region TEXT,
            province TEXT,
            municipality TEXT,
            att_name TEXT,
            att_tech TEXT,
            node_id TEXT,
            cell_name TEXT UNIQUE,
            physical_sector TEXT,
            rnc_name TEXT,
            rnc_id BIGINT,
            cell_id BIGINT,
            vendor TEXT,
            dl_arfcn BIGINT,
            ul_arfcn BIGINT,
            earfcn_dl BIGINT,
            earfcn_ul BIGINT,
            lac BIGINT,
            rac BIGINT,
            tac BIGINT,
            freq_band TEXT,
            band_indicator TEXT,
            band_width TEXT,
            latitude FLOAT,
            longitude FLOAT,
            azimuth FLOAT,
            beam FLOAT,
            period TEXT
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
        print("Table 'master_cell_total' created successfully.")

        # Close the cursor and connection
        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"Error creating table: {e}")


def create_table_master_node_total():
    # Replace these variables with your PostgreSQL credentials
    username = POSTGRES_USERNAME
    password = POSTGRES_PASSWORD
    host = POSTGRES_HOST
    port = POSTGRES_PORT
    database_name = POSTGRES_DB

    # SQL command to drop and create the master_node_total table
    create_table_query = """
        DROP TABLE IF EXISTS master_node_total;
        CREATE TABLE master_node_total (
            id SERIAL PRIMARY KEY,
            node TEXT UNIQUE,
            region TEXT,
            province TEXT,
            municipality TEXT,
            latitude FLOAT,
            longitude FLOAT,
            att_name TEXT,
            vendor TEXT,
            period TEXT
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
        print("Table 'master_node' created successfully.")

        # Close the cursor and connection
        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"Error creating table: {e}")


def create_table_ept_cell():
    # Replace these variables with your PostgreSQL credentials
    username = POSTGRES_USERNAME
    password = POSTGRES_PASSWORD
    host = POSTGRES_HOST
    port = POSTGRES_PORT
    database_name = POSTGRES_DB

    create_table_query = """
        DROP TABLE IF EXISTS ept_cell;

        CREATE TABLE ept_cell (
            id SERIAL PRIMARY KEY,
            cell_name TEXT,
            att_site TEXT,
            att_name TEXT,
            att_tech TEXT,
            latitude FLOAT,
            longitude FLOAT,
            state TEXT,
            province TEXT,
            region TEXT,
            coverage TEXT,
            status TEXT,
            band_indicator TEXT,
            band_width TEXT,
            ul_arfcn BIGINT,
            dl_arfcn BIGINT,
            node_id TEXT,
            physical_sector TEXT,
            cell_id BIGINT,
            local_cell_id BIGINT,
            psc BIGINT,
            rnc_name TEXT,
            rnc_id BIGINT,
            lac BIGINT,
            tac BIGINT,
            rac BIGINT,
            ura TEXT,
            sac TEXT,
            vendor TEXT,
            tracker TEXT,
            azimuth FLOAT,
            beam FLOAT,
            radio FLOAT,
            geom_cell GEOMETRY(Point, 4326),
            geom_sector GEOMETRY(Polygon, 4326),
            UNIQUE (cell_name)
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
        print("Table 'ept_cell' created successfully.")

        # Close the cursor and connection
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"An error occurred: {e}")


def create_table_master_cell():
    username = POSTGRES_USERNAME
    password = POSTGRES_PASSWORD
    host = POSTGRES_HOST
    port = POSTGRES_PORT
    database_name = POSTGRES_DB

    create_table_query = """
        DROP TABLE IF EXISTS master_cell;
                
        CREATE TABLE master_cell (
            id SERIAL PRIMARY KEY,
            region TEXT,
            province TEXT,
            municipality TEXT,
            att_name TEXT,
            att_tech TEXT,
            node_id TEXT,
            cell_name TEXT UNIQUE,
            physical_sector TEXT,
            rnc_name TEXT,
            rnc_id BIGINT,
            cell_id BIGINT,
            vendor TEXT,
            dl_arfcn BIGINT,
            ul_arfcn BIGINT,
            earfcn_dl BIGINT,
            earfcn_ul BIGINT,
            lac BIGINT,
            rac BIGINT,
            tac BIGINT,
            freq_band TEXT,
            band_indicator TEXT,
            band_width TEXT,
            latitude FLOAT,
            longitude FLOAT,
            azimuth FLOAT,
            beam FLOAT
        );
    """
    try:
        conn = psycopg2.connect(
            user=username,
            password=password,
            host=host,
            port=port,
            database=database_name
        )
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        print("Table 'master_cell' created successfully.")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error creating master_cell: {e}")


def create_table_master_node():
    create_table_query = """
        DROP TABLE IF EXISTS master_node;

        CREATE TABLE master_node (
            id SERIAL PRIMARY KEY,
            node TEXT,
            region TEXT,
            province TEXT,
            municipality TEXT,
            latitude FLOAT,
            longitude FLOAT,
            att_name TEXT,
            vendor TEXT,
            UNIQUE (node)
        );
    """
    try:
        conn = psycopg2.connect(
            user=POSTGRES_USERNAME,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB
        )
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        print("Table 'master_node' created successfully.")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"An error occurred while creating master_node: {e}")

def get_last_date(table):
    # Replace these variables with your PostgreSQL credentials
    username = POSTGRES_USERNAME
    password = POSTGRES_PASSWORD
    host = POSTGRES_HOST
    port = POSTGRES_PORT
    database_name = POSTGRES_DB

    # SQL command to get the maximum date from the lte_cqi_daily table
    last_date_query = f"SELECT MAX(date) FROM {table};"

    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            user=username,
            password=password,
            host=host,
            port=port,
            database=database_name
        )

        # Create a cursor to execute the SQL command
        cursor = conn.cursor()

        # Execute the SQL command to get the last date
        cursor.execute(last_date_query)
        last_date = cursor.fetchone()[0]  # Fetch the result

        # Close the cursor and connection
        cursor.close()
        conn.close()

        return last_date

    except psycopg2.Error as e:
        print(f"Error fetching last date: {e}")
        return None


def delete_newer_than(table, date):
    # Replace these variables with your PostgreSQL credentials
    username = POSTGRES_USERNAME
    password = POSTGRES_PASSWORD
    host = POSTGRES_HOST
    port = POSTGRES_PORT
    database_name = POSTGRES_DB

    # SQL command to delete records newer than a specified date from the table
    delete_query = f"DELETE FROM {table} WHERE date > %s;"

    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            user=username,
            password=password,
            host=host,
            port=port,
            database=database_name
        )
        conn.autocommit = False  # Enable transaction control

        # Create a cursor to execute the SQL command
        cursor = conn.cursor()

        # Execute the SQL command to delete records
        cursor.execute(delete_query, (date,))

        # Commit changes
        conn.commit()

        print(f"Records newer than {date} have been deleted from {table}.")

        # Close the cursor and connection
        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        # Rollback in case of error
        if conn:
            conn.rollback()
        print(f"Error deleting records: {e}")

def delete_all(table):
    # Replace these variables with your PostgreSQL credentials
    username = POSTGRES_USERNAME
    password = POSTGRES_PASSWORD
    host = POSTGRES_HOST
    port = POSTGRES_PORT
    database_name = POSTGRES_DB

    # SQL command to delete records newer than a specified date from the table
    delete_query = f"DELETE FROM {table};"

    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            user=username,
            password=password,
            host=host,
            port=port,
            database=database_name
        )
        conn.autocommit = False  # Enable transaction control

        # Create a cursor to execute the SQL command
        cursor = conn.cursor()

        # Execute the SQL command to delete records
        cursor.execute(delete_query)

        # Commit changes
        conn.commit()

        print(f"Records have been deleted from {table}.")

        # Close the cursor and connection
        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        # Rollback in case of error
        if conn:
            conn.rollback()
        print(f"Error deleting records: {e}")

def truncate_table(table):
    # PostgreSQL connection parameters (assumed to be loaded from environment variables)
    username = POSTGRES_USERNAME
    password = POSTGRES_PASSWORD
    host = POSTGRES_HOST
    port = POSTGRES_PORT
    database_name = POSTGRES_DB

    # SQL command to remove all rows from the specified table
    truncate_query = f"TRUNCATE TABLE {table};"

    try:
        # Establish database connection
        conn = psycopg2.connect(
            user=username,
            password=password,
            host=host,
            port=port,
            database=database_name
        )
        conn.autocommit = False  # Disable autocommit for transaction safety

        # Create cursor to execute SQL
        cursor = conn.cursor()

        # Execute TRUNCATE statement
        cursor.execute(truncate_query)

        # Commit the transaction
        conn.commit()

        print(f"Table '{table}' has been successfully truncated.")

        # Clean up resources
        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        # Roll back the transaction on error
        if conn:
            conn.rollback()
        print(f"Error truncating table '{table}': {e}")

