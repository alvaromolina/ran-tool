import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
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

# Create database engine
def get_engine():
    connection_string = f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    return create_engine(connection_string)

def create_table_umts_cqi_metrics_daily():
    engine = get_engine()
    
    create_table_query = """
        DROP TABLE IF EXISTS public.umts_cqi_metrics_daily;
        CREATE TABLE public.umts_cqi_metrics_daily
        (
            id SERIAL PRIMARY KEY,
            date date,
            region text COLLATE pg_catalog."default",
            province text COLLATE pg_catalog."default",
            municipality text COLLATE pg_catalog."default",
            site_att text COLLATE pg_catalog."default",
            vendors text COLLATE pg_catalog."default",
            group_level text COLLATE pg_catalog."default",
            umts_cqi double precision,
            umts_acc_cs double precision,
            umts_acc_ps double precision,
            umts_ret_cs double precision,
            umts_ret_ps double precision,
            umts_thp_dl double precision,
            umts_traff_voice double precision,
            umts_traff_data double precision,
            umts_cqi_h double precision,
            umts_acc_cs_h double precision,
            umts_acc_ps_h double precision,
            umts_ret_cs_h double precision,
            umts_ret_ps_h double precision,
            umts_thp_dl_h double precision,
            umts_traff_voice_h double precision,
            umts_traff_data_h double precision,
            umts_cqi_e double precision,
            umts_acc_cs_e double precision,
            umts_acc_ps_e double precision,
            umts_ret_cs_e double precision,
            umts_ret_ps_e double precision,
            umts_thp_dl_e double precision,
            umts_traff_voice_e double precision,
            umts_traff_data_e double precision,
            umts_cqi_n double precision,
            umts_acc_cs_n double precision,
            umts_acc_ps_n double precision,
            umts_ret_cs_n double precision,
            umts_ret_ps_n double precision,
            umts_thp_dl_n double precision,
            umts_traff_voice_n double precision,
            umts_traff_data_n double precision,
            CONSTRAINT unique_umts_cqi_metrics_daily UNIQUE (date, region, province, municipality, site_att)
        )
        TABLESPACE pg_default;
        
        ALTER TABLE IF EXISTS public.umts_cqi_metrics_daily
            OWNER to postgres;
    """

    try:
        with engine.connect() as conn:
            conn.execute(text(create_table_query))
            conn.commit()
        print("Table 'umts_cqi_metrics_daily' created successfully.")

    except SQLAlchemyError as e:
        print(f"Error creating table: {e}")

def create_table_lte_cqi_metrics_daily():
    engine = get_engine()
    
    create_table_query = """
        DROP TABLE IF EXISTS public.lte_cqi_metrics_daily;
        CREATE TABLE public.lte_cqi_metrics_daily
        (
            id SERIAL PRIMARY KEY,
            date date,
            region text COLLATE pg_catalog."default",
            province text COLLATE pg_catalog."default",
            municipality text COLLATE pg_catalog."default",
            site_att text COLLATE pg_catalog."default",
            vendors text COLLATE pg_catalog."default",
            group_level text COLLATE pg_catalog."default",
            lte_cqi double precision,
            lte_acc double precision,
            lte_ret double precision,
            lte_irat double precision,
            lte_thp_user_dl double precision,
            lte_4g_on_3g double precision, 
            lte_ookla_lat double precision, 
            lte_ookla_thp double precision,
            lte_traff double precision,
            lte_cqi_h double precision,
            lte_acc_h double precision,
            lte_ret_h double precision,
            lte_irat_h double precision,
            lte_thp_user_dl_h double precision,
            lte_4g_on_3g_h double precision, 
            lte_ookla_lat_h double precision, 
            lte_ookla_thp_h double precision,
            lte_traff_h double precision,
            lte_cqi_e double precision,
            lte_acc_e double precision,
            lte_ret_e double precision,
            lte_irat_e double precision,
            lte_thp_user_dl_e double precision,
            lte_4g_on_3g_e double precision, 
            lte_ookla_lat_e double precision, 
            lte_ookla_thp_e double precision,
            lte_traff_e double precision,
            lte_cqi_n double precision,
            lte_acc_n double precision,
            lte_ret_n double precision,
            lte_irat_n double precision,
            lte_thp_user_dl_n double precision,
            lte_4g_on_3g_n double precision, 
            lte_ookla_lat_n double precision, 
            lte_ookla_thp_n double precision,
            lte_traff_n double precision,
            lte_cqi_s double precision,
            lte_acc_s double precision,
            lte_ret_s double precision,
            lte_irat_s double precision,
            lte_thp_user_dl_s double precision,
            lte_4g_on_3g_s double precision, 
            lte_ookla_lat_s double precision, 
            lte_ookla_thp_s double precision,
            lte_traff_s double precision,
            CONSTRAINT unique_lte_cqi_metrics_daily UNIQUE (date, region, province, municipality, site_att)
        )
        TABLESPACE pg_default;
        
        ALTER TABLE IF EXISTS public.lte_cqi_metrics_daily
            OWNER to postgres;
    """

    try:
        with engine.connect() as conn:
            conn.execute(text(create_table_query))
            conn.commit()
        print("Table 'lte_cqi_metrics_daily' created successfully.")

    except SQLAlchemyError as e:
        print(f"Error creating table: {e}")

def create_table_nr_cqi_metrics_daily():
    engine = get_engine()
    
    create_table_query = """
        DROP TABLE IF EXISTS public.nr_cqi_metrics_daily;
        CREATE TABLE public.nr_cqi_metrics_daily
        (
            id SERIAL PRIMARY KEY,
            date date,
            region text COLLATE pg_catalog."default",
            province text COLLATE pg_catalog."default",
            municipality text COLLATE pg_catalog."default",
            city text COLLATE pg_catalog."default",
            site_att text COLLATE pg_catalog."default",
            vendors text COLLATE pg_catalog."default",
            group_level text COLLATE pg_catalog."default",
            
            -- Total NR metrics (all vendors combined)
            nr_cqi double precision,
            nr_acc_mn double precision,
            nr_acc_sn double precision,
            nr_ret_mn double precision,
            nr_endc_ret_tot double precision,
            nr_thp_mn double precision,
            nr_thp_sn double precision,
            nr_traffic_4gleg_gb double precision,
            nr_traffic_5gleg_gb double precision,
            nr_traffic_mac_gb double precision,
            
            -- Ericsson-specific NR metrics
            nr_cqi_e double precision,
            nr_acc_mn_e double precision,
            nr_acc_sn_e double precision,
            nr_ret_mn_e double precision,
            nr_endc_ret_tot_e double precision,
            nr_thp_mn_e double precision,
            nr_thp_sn_e double precision,
            nr_traffic_4gleg_gb_e double precision,
            nr_traffic_5gleg_gb_e double precision,
            nr_traffic_mac_gb_e double precision,
            
            -- Nokia-specific NR metrics
            nr_cqi_n double precision,
            nr_acc_mn_n double precision,
            nr_acc_sn_n double precision,
            nr_ret_mn_n double precision,
            nr_endc_ret_tot_n double precision,
            nr_thp_mn_n double precision,
            nr_thp_sn_n double precision,
            nr_traffic_4gleg_gb_n double precision,
            nr_traffic_5gleg_gb_n double precision,
            nr_traffic_mac_gb_n double precision,
            
            CONSTRAINT unique_nr_cqi_metrics_daily UNIQUE (date, region, province, municipality, city, site_att)
        )
        TABLESPACE pg_default;
        
        ALTER TABLE IF EXISTS public.nr_cqi_metrics_daily
            OWNER to postgres;
    """

    try:
        with engine.connect() as conn:
            conn.execute(text(create_table_query))
            conn.commit()
        print("Table 'nr_cqi_metrics_daily' created successfully.")

    except SQLAlchemyError as e:
        print(f"Error creating table: {e}")


def create_table_master_node_neighbor():
    """Create the master_node_neighbor table if it doesn't exist."""
    engine = get_engine()
    
    create_table_query = """
        DROP TABLE IF EXISTS public.master_node_neighbor;
        CREATE TABLE public.master_node_neighbor
        (
            region text COLLATE pg_catalog."default",
            province text COLLATE pg_catalog."default",
            municipality text COLLATE pg_catalog."default",
            att_name text COLLATE pg_catalog."default",
            neighbor_count integer,
            site_neighbor_list text COLLATE pg_catalog."default"
        )
        TABLESPACE pg_default;
        
        ALTER TABLE IF EXISTS public.master_node_neighbor
            OWNER to postgres;
    """

    try:
        with engine.connect() as conn:
            conn.execute(text(create_table_query))
            conn.commit()
        print("Table 'master_node_neighbor' created successfully.")

    except SQLAlchemyError as e:
        print(f"Error creating table: {e}")

def get_last_date(table):
    engine = get_engine()

    # SQL command to get the maximum date from the table
    last_date_query = f"SELECT MAX(date) FROM {table};"

    try:
        with engine.connect() as conn:
            result = conn.execute(text(last_date_query))
            last_date = result.fetchone()[0]  # Fetch the result
            
        return last_date

    except SQLAlchemyError as e:
        print(f"Error fetching last date: {e}")
        return None

def delete_newer_than(table, date):
    engine = get_engine()

    # SQL command to delete records newer than a specified date from the table
    delete_query = f"DELETE FROM {table} WHERE date > :date"

    try:
        with engine.begin() as conn:  # begin() automatically handles commit/rollback
            # Execute the SQL command to delete records
            result = conn.execute(text(delete_query), {"date": date})
            
            print(f"Records newer than {date} have been deleted from {table}.")

    except SQLAlchemyError as e:
        print(f"Error deleting records: {e}")

def truncate_table(table):
    engine = get_engine()

    # SQL command to remove all rows from the specified table
    truncate_query = f"TRUNCATE TABLE {table}"

    try:
        with engine.begin() as conn:  # begin() automatically handles commit/rollback
            # Execute TRUNCATE statement
            conn.execute(text(truncate_query))
            
            print(f"Table '{table}' has been successfully truncated.")

    except SQLAlchemyError as e:
        print(f"Error truncating table '{table}': {e}")
