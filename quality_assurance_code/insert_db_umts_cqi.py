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

def insert_umts_cqi_zip_files(last_date):

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
        "DATE" : "date",
        "REGION" : "region",
        "PROVINCE" : "province",
        "MUNICIPALITY" : "municipality",
        "CITY" : "city",
        "SITE_ATT" : "site_att",
        "VENDORS" : "vendors",
        "3G Composite Quality" : "umts_composite_quality",
        "H3G:RRC_SUCCESS_CS" : "h3g_rrc_success_cs",
        "H3G:RRC_ATTEMPTS_CS" : "h3g_rrc_attempts_cs",
        "H3G:NAS_SUCCESS_CS" : "h3g_nas_success_cs",
        "H3G:NAS_ATTEMPTS_CS" : "h3g_nas_attempts_cs",
        "H3G:RAB_SUCCESS_CS" : "h3g_rab_success_cs",
        "H3G:RAB_ATTEMPTS_CS" : "h3g_rab_attempts_cs",
        "H3G:DROP_NUM_CS" : "h3g_drop_num_cs",
        "H3G:DROP_DENOM_CS" : "h3g_drop_denom_cs",
        "H3G:RRC_SUCCESS_PS" : "h3g_rrc_success_ps",
        "H3G:RRC_ATTEMPTS_PS" : "h3g_rrc_attempts_ps",
        "H3G:NAS_SUCCESS_PS" : "h3g_nas_success_ps",
        "H3G:NAS_ATTEMPTS_PS" : "h3g_nas_attempts_ps",
        "H3G:RAB_SUCCESS_PS" : "h3g_rab_success_ps",
        "H3G:RAB_ATTEMPTS_PS" : "h3g_rab_attempts_ps",
        "H3G:PS_RETAINABILITY_NUM" : "h3g_ps_retainability_num",
        "H3G:PS_RETAINABILITY_DENOM" : "h3g_ps_retainability_denom",
        "H3G:THPT_USER_DL_KBPS_NUM" : "h3g_thpt_user_dl_kbps_num",
        "H3G:THPT_USER_DL_KBPS_DENOM" : "h3g_thpt_user_dl_kbps_denom",
        "E3G:RRC_SUCCESS_CS" : "e3g_rrc_success_cs",
        "E3G:RRC_ATTEMPTS_CS" : "e3g_rrc_attempts_cs",
        "E3G:NAS_SUCCESS_CS" : "e3g_nas_success_cs",
        "E3G:NAS_ATTEMPTS_CS" : "e3g_nas_attempts_cs",
        "E3G:RAB_SUCCESS_CS" : "e3g_rab_success_cs",
        "E3G:RAB_ATTEMPTS_CS" : "e3g_rab_attempts_cs",
        "E3G:DROP_NUM_CS" : "e3g_drop_num_cs",
        "E3G:DROP_DENOM_CS" : "e3g_drop_denom_cs",
        "E3G:RRC_SUCCESS_PS" : "e3g_rrc_success_ps",
        "E3G:RRC_ATTEMPTS_PS" : "e3g_rrc_attempts_ps",
        "E3G:NAS_SUCCESS_PS" : "e3g_nas_success_ps",
        "E3G:NAS_ATTEMPTS_PS" : "e3g_nas_attempts_ps",
        "E3G:RAB_SUCCESS_PS" : "e3g_rab_success_ps",
        "E3G:RAB_ATTEMPTS_PS" : "e3g_rab_attempts_ps",
        "E3G:PS_RETAINABILITY_NUM" : "e3g_ps_retainability_num",
        "E3G:PS_RETAINABILITY_DENOM" : "e3g_ps_retainability_denom",
        "E3G:THPT_USER_DL_KBPS_NUM" : "e3g_thpt_user_dl_kbps_num",
        "E3G:THPT_USER_DL_KBPS_DENOM" : "e3g_thpt_user_dl_kbps_denom",
        "N3G:RRC_SUCCESS_CS" : "n3g_rrc_success_cs",
        "N3G:RRC_ATTEMPTS_CS" : "n3g_rrc_attempts_cs",
        "N3G:NAS_SUCCESS_CS" : "n3g_nas_success_cs",
        "N3G:NAS_ATTEMPTS_CS" : "n3g_nas_attempts_cs",
        "N3G:RAB_SUCCESS_CS" : "n3g_rab_success_cs",
        "N3G:RAB_ATTEMPTS_CS" : "n3g_rab_attempts_cs",
        "N3G:DROP_NUM_CS" : "n3g_drop_num_cs",
        "N3G:DROP_DENOM_CS" : "n3g_drop_denom_cs",
        "N3G:RRC_SUCCESS_PS" : "n3g_rrc_success_ps",
        "N3G:RRC_ATTEMPTS_PS" : "n3g_rrc_attempts_ps",
        "N3G:NAS_SUCCESS_PS" : "n3g_nas_success_ps",
        "N3G:NAS_ATTEMPTS_PS" : "n3g_nas_attempts_ps",
        "N3G:RAB_SUCCESS_PS" : "n3g_rab_success_ps",
        "N3G:RAB_ATTEMPTS_PS" : "n3g_rab_attempts_ps",
        "N3G:PS_RETAINABILITY_NUM" : "n3g_ps_retainability_num",
        "N3G:PS_RETAINABILITY_DENOM" : "n3g_ps_retainability_denom",
        "N3G:THPT_USER_DL_KBPS_NUM" : "n3g_thpt_user_dl_kbps_num",
        "N3G:THPT_USER_DL_KBPS_DENOM" : "n3g_thpt_user_dl_kbps_denom",
        "Accessibility CS" : "accessibility_cs",
        "Acc CS failures" : "acc_cs_failures",
        "Retainability CS" : "retainability_cs",
        "Ret CS failures" : "ret_cs_failures",
        "Accessibility PS" : "accessibility_ps",
        "Acc PS failures" : "acc_ps_failures",
        "Retainability PS" : "retainability_ps",
        "Ret PS failures" : "ret_ps_failures",
        "Traffic Voice" : "traffic_voice",
        "H3G:TRAFFIC_V_USER_CS" : "h3g_traffic_v_user_cs",
        "E3G:TRAFFIC_V_USER_CS" : "e3g_traffic_v_user_cs",
        "N3G:TRAFFIC_V_USER_CS" : "n3g_traffic_v_user_cs",
        "Throughput DL" : "throughput_dl",
        "Thpt failures" : "thpt_failures",
        "PS GB (UL+DL)" : "ps_gb_uldl",
        "H3G:TRAFFIC_D_USER_PS_GB" : "h3g_traffic_d_user_ps_gb",
        "E3G:TRAFFIC_D_USER_PS_GB" : "e3g_traffic_d_user_ps_gb",
        "N3G:TRAFFIC_D_USER_PS_GB" : "n3g_traffic_d_user_ps_gb",
    }

    # Set up SQLAlchemy engine
    engine = create_engine(connection_string)

    # Directory path for input files
    input_path = os.path.join(os.getenv('ROOT_DIRECTORY'), 'input', 'daily_3g_cqi_site')

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
                    df.to_sql('umts_cqi_daily', engine, if_exists='append', index=False)
                    print(f"Successfully inserted new data from {zip_file} for dates after {last_date}")

        except SQLAlchemyError as e:
            print(f"Error inserting data into database: {e}")
        except KeyError as e:
            print(f"Missing column(s) in the CSV file: {e}")
        except Exception as e:
            print(f"Error processing file {zip_file}: {e}")

    print("All data has been processed insert_umts_cqi_zip_files.")