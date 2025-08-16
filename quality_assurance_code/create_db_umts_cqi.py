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

def create_table_3g_cqi_daily():
    username = POSTGRES_USERNAME
    password = POSTGRES_PASSWORD
    host = POSTGRES_HOST
    port = POSTGRES_PORT
    database_name = POSTGRES_DB

    # SQL command to drop and create the 5g_cqi_daily table, consider umts = 3g
    create_table_query = """
        DROP TABLE IF EXISTS umts_cqi_daily;
        CREATE TABLE umts_cqi_daily (
            id SERIAL PRIMARY KEY,
            date DATE,
            region TEXT,
            province TEXT,
            municipality TEXT,
            city TEXT,
            site_att TEXT,
            vendors TEXT,
            umts_composite_quality FLOAT,
            h3g_rrc_success_cs FLOAT,
            h3g_rrc_attempts_cs FLOAT,
            h3g_nas_success_cs FLOAT,
            h3g_nas_attempts_cs FLOAT,
            h3g_rab_success_cs FLOAT,
            h3g_rab_attempts_cs FLOAT,
            h3g_drop_num_cs FLOAT,
            h3g_drop_denom_cs FLOAT,
            h3g_rrc_success_ps FLOAT,
            h3g_rrc_attempts_ps FLOAT,
            h3g_nas_success_ps FLOAT,
            h3g_nas_attempts_ps FLOAT,
            h3g_rab_success_ps FLOAT,
            h3g_rab_attempts_ps FLOAT,
            h3g_ps_retainability_num FLOAT,
            h3g_ps_retainability_denom FLOAT,
            h3g_thpt_user_dl_kbps_num FLOAT,
            h3g_thpt_user_dl_kbps_denom FLOAT,
            e3g_rrc_success_cs FLOAT,
            e3g_rrc_attempts_cs FLOAT,
            e3g_nas_success_cs FLOAT,
            e3g_nas_attempts_cs FLOAT,
            e3g_rab_success_cs FLOAT,
            e3g_rab_attempts_cs FLOAT,
            e3g_drop_num_cs FLOAT,
            e3g_drop_denom_cs FLOAT,
            e3g_rrc_success_ps FLOAT,
            e3g_rrc_attempts_ps FLOAT,
            e3g_nas_success_ps FLOAT,
            e3g_nas_attempts_ps FLOAT,
            e3g_rab_success_ps FLOAT,
            e3g_rab_attempts_ps FLOAT,
            e3g_ps_retainability_num FLOAT,
            e3g_ps_retainability_denom FLOAT,
            e3g_thpt_user_dl_kbps_num FLOAT,
            e3g_thpt_user_dl_kbps_denom FLOAT,
            n3g_rrc_success_cs FLOAT,
            n3g_rrc_attempts_cs FLOAT,
            n3g_nas_success_cs FLOAT,
            n3g_nas_attempts_cs FLOAT,
            n3g_rab_success_cs FLOAT,
            n3g_rab_attempts_cs FLOAT,
            n3g_drop_num_cs FLOAT,
            n3g_drop_denom_cs FLOAT,
            n3g_rrc_success_ps FLOAT,
            n3g_rrc_attempts_ps FLOAT,
            n3g_nas_success_ps FLOAT,
            n3g_nas_attempts_ps FLOAT,
            n3g_rab_success_ps FLOAT,
            n3g_rab_attempts_ps FLOAT,
            n3g_ps_retainability_num FLOAT,
            n3g_ps_retainability_denom FLOAT,
            n3g_thpt_user_dl_kbps_num FLOAT,
            n3g_thpt_user_dl_kbps_denom FLOAT,
            accessibility_cs FLOAT,
            acc_cs_failures FLOAT,
            retainability_cs FLOAT,
            ret_cs_failures FLOAT,
            accessibility_ps FLOAT,
            acc_ps_failures FLOAT,
            retainability_ps FLOAT,
            ret_ps_failures FLOAT,
            traffic_voice FLOAT,
            h3g_traffic_v_user_cs FLOAT,
            e3g_traffic_v_user_cs FLOAT,
            n3g_traffic_v_user_cs FLOAT,
            throughput_dl FLOAT,
            thpt_failures FLOAT,
            ps_gb_uldl FLOAT,
            h3g_traffic_d_user_ps_gb FLOAT,
            e3g_traffic_d_user_ps_gb FLOAT,
            n3g_traffic_d_user_ps_gb FLOAT
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
        print("Table '3g_cqi_vendor_daily' created successfully.")

        # Close the cursor
        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"Error creating table: {e}")

def create_table_umts_cell_traffic_daily():
    # Replace these variables with your PostgreSQL credentials
    username = POSTGRES_USERNAME
    password = POSTGRES_PASSWORD
    host = POSTGRES_HOST
    port = POSTGRES_PORT
    database_name = POSTGRES_DB

    # SQL command to drop and create the umts_cell_traffic_daily table
    create_table_query = """
        DROP TABLE IF EXISTS umts_cell_traffic_daily;
        CREATE TABLE umts_cell_traffic_daily (
            id SERIAL PRIMARY KEY,
            date DATE,
            vendor TEXT,
            rnc TEXT,
            cell TEXT,
            traffic_v_user_cs FLOAT,
            traffic_d_user_ps_gb FLOAT,
            CONSTRAINT unique_umts_cell_traffic_daily UNIQUE (date, cell, vendor)
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
        print("Table 'umts_cell_traffic_daily' created successfully.")

        # Close the cursor and connection
        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"Error creating table: {e}")