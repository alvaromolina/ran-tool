import os
import zipfile
import pandas as pd
from sqlalchemy import create_engine, func
from sqlalchemy.sql import select
from sqlalchemy.exc import SQLAlchemyError
import dotenv
import glob

# Load environment variables
dotenv.load_dotenv()
ROOT_DIRECTORY = os.getenv('ROOT_DIRECTORY')
POSTGRES_USERNAME = os.getenv('POSTGRES_USERNAME')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_DB = os.getenv('POSTGRES_DB')

def insert_lte_cqi_zip_files(last_date):

    # Replace these variables with your PostgreSQL credentials
    username = POSTGRES_USERNAME
    password = POSTGRES_PASSWORD
    host = POSTGRES_HOST
    port = POSTGRES_PORT
    database_name = POSTGRES_DB

    # PostgreSQL connection string
    connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database_name}'

    # Column mapping dictionary
    csv_to_db_columns = {
        "DATE": "date", 
        "REGION": "region", 
        "PROVINCE": "province", 
        "MUNICIPALITY": "municipality", 
        "CITY": "city", 
        "SITE_ATT": "site_att", 
        "VENDORS": "vendors",
        "4G Composite Quality": "f4g_composite_quality",
        "H4G:RRC_SUCCESS_ALL": "h4g_rrc_success_all",
        "H4G:RRC_ATTEMPS_ALL": "h4g_rrc_attemps_all",
        "H4G:S1_SUCCESS": "h4g_s1_success",
        "H4G:S1_ATTEMPS": "h4g_s1_attemps",
        "H4G:ERAB_SUCCESS": "h4g_erab_success",
        "H4G:ERABS_ATTEMPS": "h4g_erabs_attemps",
        "H4G:RETAINABILITY_NUM": "h4g_retainability_num",
        "H4G:RETAINABILITY_DENOM": "h4g_retainability_denom",
        "H4G:IRAT_4G_TO_3G_EVENTS": "h4g_irat_4g_to_3g_events",
        "H4G:ERAB_SUCC_ESTABLISHED": "h4g_erab_succ_established",
        "H4G:THPT_USER_DL_KBPS_NUM": "h4g_thpt_user_dl_kbps_num",
        "H4G:THPT_USER_DL_KBPS_DENOM": "h4g_thpt_user_dl_kbps_denom",
        "H4G:TIME3G": "h4g_time3g",
        "H4G:TIME4G": "h4g_time4g",
        "H4G:SUMAVG_LATENCY": "h4g_sumavg_latency",
        "H4G:SUMAVG_DL_KBPS": "h4g_sumavg_dl_kbps",
        "H4G:SUMMUESTRAS": "h4g_summuestras",
        "S4G:RRC_SUCCESS_ALL": "s4g_rrc_success_all",
        "S4G:RRC_ATTEMPS_ALL": "s4g_rrc_attemps_all",
        "S4G:S1_SUCCESS": "s4g_s1_success",
        "S4G:S1_ATTEMPS": "s4g_s1_attemps",
        "S4G:ERAB_SUCCESS": "s4g_erab_success",
        "S4G:ERABS_ATTEMPS": "s4g_erabs_attemps",
        "S4G:RETAINABILITY_NUM": "s4g_retainability_num",
        "S4G:RETAINABILITY_DENOM": "s4g_retainability_denom",
        "S4G:IRAT_4G_TO_3G_EVENTS": "s4g_irat_4g_to_3g_events",
        "S4G:ERAB_SUCC_ESTABLISHED": "s4g_erab_succ_established",
        "S4G:THPT_USER_DL_KBPS_NUM": "s4g_thpt_user_dl_kbps_num",
        "S4G:THPT_USER_DL_KBPS_DENOM": "s4g_thpt_user_dl_kbps_denom",
        "S4G:TIME3G": "s4g_time3g",
        "S4G:TIME4G": "s4g_time4g",
        "S4G:SUMAVG_LATENCY": "s4g_sumavg_latency",
        "S4G:SUMAVG_DL_KBPS": "s4g_sumavg_dl_kbps",
        "S4G:SUMMUESTRAS": "s4g_summuestras",
        "E4G:RRC_SUCCESS_ALL": "e4g_rrc_success_all",
        "E4G:RRC_ATTEMPS_ALL": "e4g_rrc_attemps_all",
        "E4G:S1_SUCCESS": "e4g_s1_success",
        "E4G:S1_ATTEMPS": "e4g_s1_attemps",
        "E4G:ERAB_SUCCESS": "e4g_erab_success",
        "E4G:ERABS_ATTEMPS": "e4g_erabs_attemps",
        "E4G:RETAINABILITY_NUM": "e4g_retainability_num",
        "E4G:RETAINABILITY_DENOM": "e4g_retainability_denom",
        "E4G:IRAT_4G_TO_3G_EVENTS": "e4g_irat_4g_to_3g_events",
        "E4G:ERAB_SUCC_ESTABLISHED": "e4g_erab_succ_established",
        "E4G:THPT_USER_DL_KBPS_NUM": "e4g_thpt_user_dl_kbps_num",
        "E4G:THPT_USER_DL_KBPS_DENOM": "e4g_thpt_user_dl_kbps_denom",
        "E4G:TIME3G": "e4g_time3g",
        "E4G:TIME4G": "e4g_time4g",
        "E4G:SUMAVG_LATENCY": "e4g_sumavg_latency",
        "E4G:SUMAVG_DL_KBPS": "e4g_sumavg_dl_kbps",
        "E4G:SUMMUESTRAS": "e4g_summuestras",
        "N4G:RRC_SUCCESS_ALL": "n4g_rrc_success_all",
        "N4G:RRC_ATTEMPS_ALL": "n4g_rrc_attemps_all",
        "N4G:S1_SUCCESS": "n4g_s1_success",
        "N4G:S1_ATTEMPS": "n4g_s1_attemps",
        "N4G:ERAB_SUCCESS": "n4g_erab_success",
        "N4G:ERABS_ATTEMPS": "n4g_erabs_attemps",
        "N4G:RETAINABILITY_NUM": "n4g_retainability_num",
        "N4G:RETAINABILITY_DENOM": "n4g_retainability_denom",
        "N4G:IRAT_4G_TO_3G_EVENTS": "n4g_irat_4g_to_3g_events",
        "N4G:ERAB_SUCC_ESTABLISHED": "n4g_erab_succ_established",
        "N4G:THPT_USER_DL_KBPS_NUM": "n4g_thpt_user_dl_kbps_num",
        "N4G:THPT_USER_DL_KBPS_DENOM": "n4g_thpt_user_dl_kbps_denom",
        "N4G:TIME3G": "n4g_time3g",
        "N4G:TIME4G": "n4g_time4g",
        "N4G:SUMAVG_LATENCY": "n4g_sumavg_latency",
        "N4G:SUMAVG_DL_KBPS": "n4g_sumavg_dl_kbps",
        "N4G:SUMMUESTRAS": "n4g_summuestras",
        "Accessibility PS": "accessibility_ps",
        "Acc failures": "acc_failures",
        "Retainability PS": "retainability_ps",
        "Ret failures": "ret_failures",
        "IRAT PS": "irat_ps",
        "IRAT failures": "irat_failures",
        "ThpT DL kbps (RAN DRB)": "thpt_dl_kbps_ran_drb",
        "Thpt failures": "thpt_failures",
        "Ookla Latency": "ookla_latency",
        "Latency failures": "latency_failures",
        "Ookla thp": "ookla_thp",
        "Thpt Ookla failures": "thpt_ookla_failures",
        "4GON3G": "f4gon3g",
        "4Gon3G failures": "f4gon3g_failures",
        "Traffic DL+UL (TB)": "traffic_dlul_tb",
        "H4G:TRAFFIC_D_USER_PS_GB": "h4g_traffic_d_user_ps_gb",
        "S4G:TRAFFIC_D_USER_PS_GB": "s4g_traffic_d_user_ps_gb",
        "E4G:TRAFFIC_D_USER_PS_GB": "e4g_traffic_d_user_ps_gb",
        "N4G:TRAFFIC_D_USER_PS_GB": "n4g_traffic_d_user_ps_gb"
    }

    # Set up SQLAlchemy engine
    engine = create_engine(connection_string)

    # Directory path for input files
    input_path = os.path.join(os.getenv('ROOT_DIRECTORY'), 'input', 'daily_lte_cqi_site')

    # Iterate over each zip file in the input directory
    for zip_file in glob.glob(os.path.join(input_path, '*.zip')):
        try:
            # Open the zip file
            with zipfile.ZipFile(zip_file, 'r') as z:
                # Get the first file (assuming only one CSV per zip)
                csv_filename = z.namelist()[0]
                
                # Read CSV file inside zip to DataFrame
                with z.open(csv_filename) as csvfile:
                    df = pd.read_csv(csvfile, low_memory=False)
                
                # Select only the columns in the dictionary and rename them
                df = df[list(csv_to_db_columns.keys())]
                df.rename(columns=csv_to_db_columns, inplace=True)

                # Standardize date format and handle invalid dates
                df['date'] = pd.to_datetime(df['date'], errors='coerce')

                # Filter data to include only those dates greater than last_date
                df = df[df['date'] > pd.to_datetime(last_date)]

                if not df.empty:
                    # Remove duplicates based on 'date', 'site_att', and 'vendors' columns
                    df.drop_duplicates(subset=['date', 'site_att', 'vendors'], inplace=True)

                    # Insert the DataFrame to PostgreSQL
                    df.to_sql('lte_cqi_daily', engine, if_exists='append', index=False)
                    print(f"Successfully inserted new data from {zip_file} for dates after {last_date}")

        except SQLAlchemyError as e:
            print(f"Error inserting data into database: {e}")
        except KeyError as e:
            print(f"Missing column(s) in the CSV file: {e}")
        except Exception as e:
            print(f"Error processing file {zip_file}: {e}")

    print("All data has been processed insert_lte_cqi_zip_files.")

