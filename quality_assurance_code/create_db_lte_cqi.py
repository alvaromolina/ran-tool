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

def create_table_lte_cqi_daily():
    # Replace these variables with your PostgreSQL credentials
    username = POSTGRES_USERNAME
    password = POSTGRES_PASSWORD
    host = POSTGRES_HOST
    port = POSTGRES_PORT
    database_name = POSTGRES_DB

    # SQL command to drop and create the lte_cqi_daily table
    create_table_query = """
        DROP TABLE IF EXISTS lte_cqi_daily;
        CREATE TABLE lte_cqi_daily (
            id SERIAL PRIMARY KEY,
            date DATE,
            region TEXT,
            province TEXT,
            municipality TEXT,
            city TEXT,
            site_att TEXT,
            vendors TEXT,
            f4g_composite_quality FLOAT,
            h4g_rrc_success_all FLOAT,
            h4g_rrc_attemps_all FLOAT,
            h4g_s1_success FLOAT,
            h4g_s1_attemps FLOAT,
            h4g_erab_success FLOAT,
            h4g_erabs_attemps FLOAT,
            h4g_retainability_num FLOAT,
            h4g_retainability_denom FLOAT,
            h4g_irat_4g_to_3g_events FLOAT,
            h4g_erab_succ_established FLOAT,
            h4g_thpt_user_dl_kbps_num FLOAT,
            h4g_thpt_user_dl_kbps_denom FLOAT,
            h4g_time3g FLOAT,
            h4g_time4g FLOAT,
            h4g_sumavg_latency FLOAT,
            h4g_sumavg_dl_kbps FLOAT,
            h4g_summuestras FLOAT,
            s4g_rrc_success_all FLOAT,
            s4g_rrc_attemps_all FLOAT,
            s4g_s1_success FLOAT,
            s4g_s1_attemps FLOAT,
            s4g_erab_success FLOAT,
            s4g_erabs_attemps FLOAT,
            s4g_retainability_num FLOAT,
            s4g_retainability_denom FLOAT,
            s4g_irat_4g_to_3g_events FLOAT,
            s4g_erab_succ_established FLOAT,
            s4g_thpt_user_dl_kbps_num FLOAT,
            s4g_thpt_user_dl_kbps_denom FLOAT,
            s4g_time3g FLOAT,
            s4g_time4g FLOAT,
            s4g_sumavg_latency FLOAT,
            s4g_sumavg_dl_kbps FLOAT,
            s4g_summuestras FLOAT,
            e4g_rrc_success_all FLOAT,
            e4g_rrc_attemps_all FLOAT,
            e4g_s1_success FLOAT,
            e4g_s1_attemps FLOAT,
            e4g_erab_success FLOAT,
            e4g_erabs_attemps FLOAT,
            e4g_retainability_num FLOAT,
            e4g_retainability_denom FLOAT,
            e4g_irat_4g_to_3g_events FLOAT,
            e4g_erab_succ_established FLOAT,
            e4g_thpt_user_dl_kbps_num FLOAT,
            e4g_thpt_user_dl_kbps_denom FLOAT,
            e4g_time3g FLOAT,
            e4g_time4g FLOAT,
            e4g_sumavg_latency FLOAT,
            e4g_sumavg_dl_kbps FLOAT,
            e4g_summuestras FLOAT,
            n4g_rrc_success_all FLOAT,
            n4g_rrc_attemps_all FLOAT,
            n4g_s1_success FLOAT,
            n4g_s1_attemps FLOAT,
            n4g_erab_success FLOAT,
            n4g_erabs_attemps FLOAT,
            n4g_retainability_num FLOAT,
            n4g_retainability_denom FLOAT,
            n4g_irat_4g_to_3g_events FLOAT,
            n4g_erab_succ_established FLOAT,
            n4g_thpt_user_dl_kbps_num FLOAT,
            n4g_thpt_user_dl_kbps_denom FLOAT,
            n4g_time3g FLOAT,
            n4g_time4g FLOAT,
            n4g_sumavg_latency FLOAT,
            n4g_sumavg_dl_kbps FLOAT,
            n4g_summuestras FLOAT,
            accessibility_ps FLOAT,
            acc_failures FLOAT,
            retainability_ps FLOAT,
            ret_failures FLOAT,
            irat_ps FLOAT,
            irat_failures FLOAT,
            thpt_dl_kbps_ran_drb FLOAT,
            thpt_failures FLOAT,
            ookla_latency FLOAT,
            latency_failures FLOAT,
            ookla_thp FLOAT,
            thpt_ookla_failures FLOAT,
            f4gon3g FLOAT,
            f4gon3g_failures FLOAT,
            traffic_dlul_tb FLOAT,
            h4g_traffic_d_user_ps_gb FLOAT,
            s4g_traffic_d_user_ps_gb FLOAT,
            e4g_traffic_d_user_ps_gb FLOAT,
            n4g_traffic_d_user_ps_gb FLOAT,
            CONSTRAINT unique_lte_cqi_daily UNIQUE (date, site_att, vendors)
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
        print("Table 'lte_cqi_daily' created successfully.")

        # Close the cursor
        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"Error creating table: {e}")

def create_table_lte_cell_traffic_daily():
    # Replace these variables with your PostgreSQL credentials
    username = POSTGRES_USERNAME
    password = POSTGRES_PASSWORD
    host = POSTGRES_HOST
    port = POSTGRES_PORT
    database_name = POSTGRES_DB

    # SQL command to drop and create the lte_cell_traffic_daily table
    create_table_query = """
        DROP TABLE IF EXISTS lte_cell_traffic_daily;
        CREATE TABLE lte_cell_traffic_daily (
            id SERIAL PRIMARY KEY,
            date DATE,
            vendor TEXT,
            enb_agg TEXT,
            cell TEXT,
            traffic_d_user_ps_gb FLOAT,
            CONSTRAINT unique_lte_cell_traffic_daily UNIQUE (date, cell, vendor)
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
        print("Table 'lte_cell_traffic_daily' created successfully.")

        # Close the cursor and connection
        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"Error creating table: {e}")


def create_table_lte_cqi_metrics():
    # Replace these variables with your PostgreSQL credentials
    username = POSTGRES_USERNAME
    password = POSTGRES_PASSWORD
    host = POSTGRES_HOST
    port = POSTGRES_PORT
    database_name = POSTGRES_DB

    # SQL command to drop and create the lte_cqi_metrics table
    create_table_query = """
        DROP TABLE IF EXISTS lte_cqi_metrics;
        CREATE TABLE lte_cqi_metrics (
            id SERIAL PRIMARY KEY,
            site_att TEXT,
            init_date DATE,
            lte_cqi_before FLOAT,
            lte_cqi_after FLOAT,
            lte_cqi_latest FLOAT,
            lte_cqi_variation FLOAT,
            lte_cqi_variation_latest FLOAT,
            CONSTRAINT unique_lte_cqi_metrics UNIQUE (site_att, init_date)
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
        print("Table 'lte_cqi_metrics' created successfully.")

        # Close the cursor and connection
        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"Error creating table: {e}")

def create_table_new_cell_expansion():
    # Replace these variables with your PostgreSQL credentials
    username = POSTGRES_USERNAME
    password = POSTGRES_PASSWORD
    host = POSTGRES_HOST
    port = POSTGRES_PORT
    database_name = POSTGRES_DB

    # SQL command to drop and create the new_cell_expansion table
    create_table_query = """
        DROP TABLE IF EXISTS new_cell_expansion;
        CREATE TABLE new_cell_expansion (
            id SERIAL PRIMARY KEY,
            att_site_name TEXT,
            init_date DATE,
            band_2_pcs NUMERIC,
            band_4_aws NUMERIC,
            band_5_850 NUMERIC,
            band_7_2600 NUMERIC,
            band_26_800 NUMERIC,
            band_42_3500 NUMERIC,
            band_default NUMERIC,
            new_cells NUMERIC,
            total_cells NUMERIC,
            expansion TEXT
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
        print("Table 'new_cell_expansion' created successfully.")

        # Close the cursor and connection
        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"Error creating table: {e}")

def create_table_att_site_init_date_count():
    # Replace these variables with your PostgreSQL credentials
    username = POSTGRES_USERNAME
    password = POSTGRES_PASSWORD
    host = POSTGRES_HOST
    port = POSTGRES_PORT
    database_name = POSTGRES_DB

    # SQL command to drop and create the new_cell_expansion table
    create_table_query = """
        DROP TABLE IF EXISTS att_site_init_date_count;
        CREATE TABLE att_site_init_date_count (
            id SERIAL PRIMARY KEY,
            att_site_name TEXT,
            init_date DATE,
            band_2_pcs NUMERIC,
            band_4_aws NUMERIC,
            band_5_850 NUMERIC,
            band_7_2600 NUMERIC,
            band_26_800 NUMERIC,
            band_42_3500 NUMERIC,
            band_default NUMERIC,
            new_cells NUMERIC,
            total_cells NUMERIC,
            expansion TEXT
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
        print("Table 'new_cell_expansion' created successfully.")

        # Close the cursor and connection
        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
        
def create_table_lte_cqi_vendor_daily():
    username = POSTGRES_USERNAME
    password = POSTGRES_PASSWORD
    host = POSTGRES_HOST
    port = POSTGRES_PORT
    database_name = POSTGRES_DB

    # SQL command to drop and create the lte_cqi_vendor_daily table
    create_table_query = """
        DROP TABLE IF EXISTS lte_cqi_vendor_daily;
        CREATE TABLE lte_cqi_vendor_daily (
            id SERIAL PRIMARY KEY,
            date DATE,
            region TEXT,
            province TEXT,
            municipality TEXT,
            site_att TEXT,
            vendors TEXT,
            lte_cqi FLOAT,
            lte_acc FLOAT,
            lte_ret FLOAT,
            lte_irat FLOAT,
            lte_thp_user_dl FLOAT,
            lte_4g_on_3g FLOAT, 
            lte_ookla_lat FLOAT, 
            lte_ookla_thp FLOAT,
            lte_traff FLOAT,
            lte_cqi_h FLOAT,
            lte_acc_h FLOAT,
            lte_ret_h FLOAT,
            lte_irat_h FLOAT,
            lte_thp_user_dl_h FLOAT,
            lte_4g_on_3g_h FLOAT, 
            lte_ookla_lat_h FLOAT, 
            lte_ookla_thp_h FLOAT,
            lte_traff_h FLOAT,
            lte_cqi_e FLOAT,
            lte_acc_e FLOAT,
            lte_ret_e FLOAT,
            lte_irat_e FLOAT,
            lte_thp_user_dl_e FLOAT,
            lte_4g_on_3g_e FLOAT, 
            lte_ookla_lat_e FLOAT, 
            lte_ookla_thp_e FLOAT,
            lte_traff_e FLOAT,
            lte_cqi_n FLOAT,
            lte_acc_n FLOAT,
            lte_ret_n FLOAT,
            lte_irat_n FLOAT,
            lte_thp_user_dl_n FLOAT,
            lte_4g_on_3g_n FLOAT, 
            lte_ookla_lat_n FLOAT, 
            lte_ookla_thp_n FLOAT,
            lte_traff_n FLOAT,
            lte_cqi_s FLOAT,
            lte_acc_s FLOAT,
            lte_ret_s FLOAT,
            lte_irat_s FLOAT,
            lte_thp_user_dl_s FLOAT,
            lte_4g_on_3g_s FLOAT, 
            lte_ookla_lat_s FLOAT, 
            lte_ookla_thp_s FLOAT,
            lte_traff_s FLOAT,
        CONSTRAINT unique_lte_cqi_vendor_daily UNIQUE (date, site_att, vendors)
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
        print("Table 'lte_cqi_vendor_daily' created successfully.")

        # Close the cursor
        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"Error creating table: {e}")

