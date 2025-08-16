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

def create_table_5g_cqi_daily():
    username = POSTGRES_USERNAME
    password = POSTGRES_PASSWORD
    host = POSTGRES_HOST
    port = POSTGRES_PORT
    database_name = POSTGRES_DB

    # SQL command to drop and create the 5g_cqi_daily table, consider nr = 5g
    create_table_query = """
        DROP TABLE IF EXISTS nr_cqi_daily;
        CREATE TABLE nr_cqi_daily (
            id SERIAL PRIMARY KEY,
            date DATE,
            region TEXT,
            province TEXT,
            municipality TEXT,
            city TEXT,
            site_att TEXT,
            vendors TEXT,
            nr_composite_quality FLOAT,
            e5g_acc_rrc_num_n FLOAT,
            e5g_s1_sr_num_n FLOAT,
            e5g_nsa_acc_erab_sr_4gendc_num_n FLOAT,
            e5g_acc_rrc_den_n FLOAT,
            e5g_s1_sr_den_n FLOAT,
            e5g_nsa_acc_erab_sr_4gendc_den_n FLOAT,
            e5g_nsa_acc_erab_succ_5gendc_5gleg_n FLOAT,
            e5g_nsa_acc_erab_att_5gendc_5gleg_n FLOAT,
            e5g_nsa_ret_erab_drop_4gendc_n FLOAT,
            e5g_nsa_ret_erab_att_4gendc_n FLOAT,
            e5g_nsa_ret_erab_drop_5gendc_4g5gleg_num_n FLOAT,
            e5g_nsa_ret_erab_drop_5gendc_4g5gleg_den_n FLOAT,
            e5g_nsa_thp_mn_num FLOAT,
            e5g_nsa_thp_mn_den FLOAT,
            e5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_num_n FLOAT,
            e5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_denom_n FLOAT,
            n5g_acc_rrc_num_n FLOAT,
            n5g_s1_sr_num_n FLOAT,
            n5g_nsa_acc_erab_sr_4gendc_num_n FLOAT,
            n5g_acc_rrc_den_n FLOAT,
            n5g_s1_sr_den_n FLOAT,
            n5g_nsa_acc_erab_sr_4gendc_den_n FLOAT,
            n5g_nsa_acc_erab_succ_5gendc_5gleg_n FLOAT,
            n5g_nsa_acc_erab_att_5gendc_5gleg_n FLOAT,
            n5g_nsa_ret_erab_drop_4gendc_n FLOAT,
            n5g_nsa_ret_erab_att_4gendc_n FLOAT,
            n5g_nsa_ret_erab_drop_5gendc_4g5gleg_num_n FLOAT,
            n5g_nsa_ret_erab_drop_5gendc_4g5gleg_den_n FLOAT,
            n5g_nsa_thp_mn_num FLOAT,
            n5g_nsa_thp_mn_den FLOAT,
            n5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_num_n FLOAT,
            n5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_denom_n FLOAT,
            acc_mn FLOAT,
            acc_sn FLOAT,
            endc_ret_tot FLOAT,
            ret_mn FLOAT,
            thp_mn FLOAT,
            thp_sn FLOAT,
            traffic_4gleg_gb FLOAT,
            e5g_nsa_traffic_pdcp_gb_5gendc_4glegn FLOAT,
            n5g_nsa_traffic_pdcp_gb_5gendc_4glegn FLOAT,
            traffic_5gleg_gb FLOAT,
            e5g_nsa_traffic_pdcp_gb_5gendc_5gleg FLOAT,
            n5g_nsa_traffic_pdcp_gb_5gendc_5gleg FLOAT,
            traffic_mac_gb FLOAT,
            e5g_nsa_traffic_mac_gb_5gendc_5gleg_n FLOAT,
            n5g_nsa_traffic_mac_gb_5gendc_5gleg_n FLOAT
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
        print("Table '5g_cqi_vendor_daily' created successfully.")

        # Close the cursor
        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
