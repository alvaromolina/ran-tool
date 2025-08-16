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

def insert_nr_cqi_zip_files(last_date):

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
    "5G Composite Quality": "nr_composite_quality",
    "E5G:ACC_RRC_NUM_N": "e5g_acc_rrc_num_n",
    "E5G:S1_SR_NUM_N": "e5g_s1_sr_num_n",
    "E5G:NSA_ACC_ERAB_SR_4GENDC_NUM_N": "e5g_nsa_acc_erab_sr_4gendc_num_n",
    "E5G:ACC_RRC_DEN_N": "e5g_acc_rrc_den_n",
    "E5G:S1_SR_DEN_N": "e5g_s1_sr_den_n",
    "E5G:NSA_ACC_ERAB_SR_4GENDC_DEN_N": "e5g_nsa_acc_erab_sr_4gendc_den_n",
    "E5G:NSA_ACC_ERAB_SUCC_5GENDC_5GLEG_N": "e5g_nsa_acc_erab_succ_5gendc_5gleg_n",
    "E5G:NSA_ACC_ERAB_ATT_5GENDC_5GLEG_N": "e5g_nsa_acc_erab_att_5gendc_5gleg_n",
    "E5G:NSA_RET_ERAB_DROP_4GENDC_N": "e5g_nsa_ret_erab_drop_4gendc_n",
    "E5G:NSA_RET_ERAB_ATT_4GENDC_N": "e5g_nsa_ret_erab_att_4gendc_n",
    "E5G:NSA_RET_ERAB_DROP_5GENDC_4G5GLEG_NUM_N": "e5g_nsa_ret_erab_drop_5gendc_4g5gleg_num_n",
    "E5G:NSA_RET_ERAB_DROP_5GENDC_4G5GLEG_DEN_N": "e5g_nsa_ret_erab_drop_5gendc_4g5gleg_den_n",
    "E5G:NSA_THP_MN_NUM": "e5g_nsa_thp_mn_num",
    "E5G:NSA_THP_MN_DEN": "e5g_nsa_thp_mn_den",
    "E5G:NSA_THPT_MAC_DL_AVG_MBPS_5GENDC_5GLEG_NUM_N": "e5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_num_n",
    "E5G:NSA_THPT_MAC_DL_AVG_MBPS_5GENDC_5GLEG_DENOM_N": "e5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_denom_n",
    "N5G:ACC_RRC_NUM_N": "n5g_acc_rrc_num_n",
    "N5G:S1_SR_NUM_N": "n5g_s1_sr_num_n",
    "N5G:NSA_ACC_ERAB_SR_4GENDC_NUM_N": "n5g_nsa_acc_erab_sr_4gendc_num_n",
    "N5G:ACC_RRC_DEN_N": "n5g_acc_rrc_den_n",
    "N5G:S1_SR_DEN_N": "n5g_s1_sr_den_n",
    "N5G:NSA_ACC_ERAB_SR_4GENDC_DEN_N": "n5g_nsa_acc_erab_sr_4gendc_den_n",
    "N5G:NSA_ACC_ERAB_SUCC_5GENDC_5GLEG_N": "n5g_nsa_acc_erab_succ_5gendc_5gleg_n",
    "N5G:NSA_ACC_ERAB_ATT_5GENDC_5GLEG_N": "n5g_nsa_acc_erab_att_5gendc_5gleg_n",
    "N5G:NSA_RET_ERAB_DROP_4GENDC_N": "n5g_nsa_ret_erab_drop_4gendc_n",
    "N5G:NSA_RET_ERAB_ATT_4GENDC_N": "n5g_nsa_ret_erab_att_4gendc_n",
    "N5G:NSA_RET_ERAB_DROP_5GENDC_4G5GLEG_NUM_N": "n5g_nsa_ret_erab_drop_5gendc_4g5gleg_num_n",
    "N5G:NSA_RET_ERAB_DROP_5GENDC_4G5GLEG_DEN_N": "n5g_nsa_ret_erab_drop_5gendc_4g5gleg_den_n",
    "N5G:NSA_THP_MN_NUM": "n5g_nsa_thp_mn_num",
    "N5G:NSA_THP_MN_DEN": "n5g_nsa_thp_mn_den",
    "N5G:NSA_THPT_MAC_DL_AVG_MBPS_5GENDC_5GLEG_NUM_N": "n5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_num_n",
    "N5G:NSA_THPT_MAC_DL_AVG_MBPS_5GENDC_5GLEG_DENOM_N": "n5g_nsa_thpt_mac_dl_avg_mbps_5gendc_5gleg_denom_n",
    "Acc MN": "acc_mn",
    "Acc SN": "acc_sn",
    "ENDC Ret Tot": "endc_ret_tot",
    "Ret MN": "ret_mn",
    "Thp MN": "thp_mn",
    "Thp SN": "thp_sn",
    "Traffic 4GLeg GB": "traffic_4gleg_gb",
    "E5G:NSA_TRAFFIC_PDCP_GB_5GENDC_4GLEGN": "e5g_nsa_traffic_pdcp_gb_5gendc_4glegn",
    "N5G:NSA_TRAFFIC_PDCP_GB_5GENDC_4GLEGN": "n5g_nsa_traffic_pdcp_gb_5gendc_4glegn",
    "Traffic 5GLeg GB": "traffic_5gleg_gb",
    "E5G:NSA_TRAFFIC_PDCP_GB_5GENDC_5GLEG": "e5g_nsa_traffic_pdcp_gb_5gendc_5gleg",
    "N5G:NSA_TRAFFIC_PDCP_GB_5GENDC_5GLEG": "n5g_nsa_traffic_pdcp_gb_5gendc_5gleg",
    "Traffic MAC GB": "traffic_mac_gb",
    "E5G:NSA_TRAFFIC_MAC_GB_5GENDC_5GLEG_N": "e5g_nsa_traffic_mac_gb_5gendc_5gleg_n",
    "N5G:NSA_TRAFFIC_MAC_GB_5GENDC_5GLEG_N": "n5g_nsa_traffic_mac_gb_5gendc_5gleg_n"
    }

    # Set up SQLAlchemy engine
    engine = create_engine(connection_string)

    # Directory path for input files
    input_path = os.path.join(os.getenv('ROOT_DIRECTORY'), 'input', 'daily_5g_cqi_site')

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
                    df.to_sql('nr_cqi_daily', engine, if_exists='append', index=False)
                    print(f"Successfully inserted new data from {zip_file} for dates after {last_date}")

        except SQLAlchemyError as e:
            print(f"Error inserting data into database: {e}")
        except KeyError as e:
            print(f"Missing column(s) in the CSV file: {e}")
        except Exception as e:
            print(f"Error processing file {zip_file}: {e}")

    print("All data has been processed insert_nr_cqi_zip_files.")