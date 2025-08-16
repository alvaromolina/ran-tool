import os
import zipfile
import pandas as pd
import glob
import psycopg2
from sqlalchemy import create_engine
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

def process_volte_cqi_ericsson_daily(last_date):
    ericsson_path = os.path.join(ROOT_DIRECTORY, "input", 'daily_volte_cqi_site', "ericsson")

    columns_to_select = [
        "DATE",
        "REGION",
        "PROVINCE",
        "MUNICIPALITY",
        "SITE_ATT",
        "VOLTE_CQI",
        "ACC_VOLTE",
        "VOLTE_ERAB_CALL_DROP_RATE_QCI1",
        "VOLTE_ERAB_CALL_DROP_RATE_QCI5",
        "SRVCC_RATE",
        "USER_TRAFFIC_VOLTE"
    ]

    column_rename_map = {
        "DATE":"date",
        "REGION":"region",
        "PROVINCE":"province",
        "MUNICIPALITY":"municipality",
        "SITE_ATT":"site_att",
        "VOLTE_CQI": "volte_cqi_e",
        "ACC_VOLTE": "acc_volte_e",
        "VOLTE_ERAB_CALL_DROP_RATE_QCI1": "erab_drop_qci1_e",
        "VOLTE_ERAB_CALL_DROP_RATE_QCI5": "erab_drop_qci5_e",
        "SRVCC_RATE": "srvcc_rate_e",
        "USER_TRAFFIC_VOLTE": "user_traffic_volte_e"
    }

    all_data = []

    # Find all zip files in the Ericsson directory
    zip_files = glob.glob(os.path.join(ericsson_path, "*.zip"))

    for zip_file in zip_files:
        try:
            with zipfile.ZipFile(zip_file, 'r') as z:
                for filename in z.namelist():
                    if filename.endswith('.csv'):
                        try:
                            with z.open(filename) as f:
                                df = pd.read_csv(f)
                                df = df[columns_to_select]
                                df = df.rename(columns=column_rename_map)

                                # Standardize date format and handle invalid dates
                                df['date'] = pd.to_datetime(df['date'], errors='coerce')

                                # Filter data to include only those dates greater than last_date
                                df = df[df['date'] > pd.to_datetime(last_date)]

                                all_data.append(df)
                        except Exception as e:
                            print(f"Error processing file {filename} in {zip_file}: {e}")
        except Exception as e:
            print(f"Error reading zip file {zip_file}: {e}")

    if not all_data:
        print("No valid data to process.")
        return

    # Concatenate all the dataframes
    combined_df = pd.concat(all_data, ignore_index=True)

    return combined_df

def process_volte_cqi_huawei_daily(last_date):
    huawei_path = os.path.join(ROOT_DIRECTORY, "input", 'daily_volte_cqi_site', "huawei")

    columns_to_select = [
        "DATE",
        "REGION",
        "PROVINCE",
        "MUNICIPALITY",
        "SITE_ATT",
        "VOLTE_CQI",
        "VOLTE_ACC",
        "VOLTE_ERAB_CALL_DROP_RATE_QCI1",
        "VOLTE_ERAB_CALL_DROP_RATE_QCI5",
        "SRVCC_RATE",
        "USER_TRAFFIC_VOLTE"
    ]

    column_rename_map = {
        "DATE":"date",
        "REGION":"region",
        "PROVINCE":"province",
        "MUNICIPALITY":"municipality",
        "SITE_ATT":"site_att",
        "VOLTE_CQI": "volte_cqi_h",
        "VOLTE_ACC": "acc_volte_h",
        "VOLTE_ERAB_CALL_DROP_RATE_QCI1": "erab_drop_qci1_h",
        "VOLTE_ERAB_CALL_DROP_RATE_QCI5": "erab_drop_qci5_h",
        "SRVCC_RATE": "srvcc_rate_h",
        "USER_TRAFFIC_VOLTE": "user_traffic_volte_h"       
    }

    all_data = []

    # Find all zip files in the Ericsson directory
    zip_files = glob.glob(os.path.join(huawei_path, "*.zip"))

    for zip_file in zip_files:
        try:
            with zipfile.ZipFile(zip_file, 'r') as z:
                for filename in z.namelist():
                    if filename.endswith('.csv'):
                        try:
                            with z.open(filename) as f:
                                df = pd.read_csv(f)
                                df = df[columns_to_select]
                                df = df.rename(columns=column_rename_map)

                                # Standardize date format and handle invalid dates
                                df['date'] = pd.to_datetime(df['date'], errors='coerce')

                                # Filter data to include only those dates greater than last_date
                                df = df[df['date'] > pd.to_datetime(last_date)]

                                all_data.append(df)
                        except Exception as e:
                            print(f"Error processing file {filename} in {zip_file}: {e}")
        except Exception as e:
            print(f"Error reading zip file {zip_file}: {e}")

    if not all_data:
        print("No valid data to process.")
        return

    # Concatenate all the dataframes
    combined_df = pd.concat(all_data, ignore_index=True)

    return combined_df

def process_volte_cqi_nokia_daily(last_date):
    nokia_path = os.path.join(ROOT_DIRECTORY, "input", 'daily_volte_cqi_site', "nokia")

    columns_to_select = [
        "DATE",
        "REGION",
        "PROVINCE",
        "MUNICIPALITY",
        "SITE_ATT",
        "VOLTE_CQI",
        "ACC_VOLTE",
        "VOLTE_ERAB_CALL_DROP_RATE_QCI1",
        "VOLTE_ERAB_CALL_DROP_RATE_QCI5",
        "SRVCC_RATE",
        "USER_TRAFFIC_VOLTE"
    ]

    column_rename_map = {
        "DATE":"date",
        "REGION":"region",
        "PROVINCE":"province",
        "MUNICIPALITY":"municipality",
        "SITE_ATT":"site_att",
        "VOLTE_CQI": "volte_cqi_n",
        "ACC_VOLTE": "acc_volte_n",
        "VOLTE_ERAB_CALL_DROP_RATE_QCI1": "erab_drop_qci1_n",
        "VOLTE_ERAB_CALL_DROP_RATE_QCI5": "erab_drop_qci5_n",
        "SRVCC_RATE": "srvcc_rate_n",
        "USER_TRAFFIC_VOLTE": "user_traffic_volte_n"   
    }

    all_data = []

    # Find all zip files in the Ericsson directory
    zip_files = glob.glob(os.path.join(nokia_path, "*.zip"))

    for zip_file in zip_files:
        try:
            with zipfile.ZipFile(zip_file, 'r') as z:
                for filename in z.namelist():
                    if filename.endswith('.csv'):
                        try:
                            with z.open(filename) as f:
                                df = pd.read_csv(f)
                                df = df[columns_to_select]
                                df = df.rename(columns=column_rename_map)

                                # Standardize date format and handle invalid dates
                                df['date'] = pd.to_datetime(df['date'], errors='coerce')

                                # Filter data to include only those dates greater than last_date
                                df = df[df['date'] > pd.to_datetime(last_date)]

                                all_data.append(df)
                        except Exception as e:
                            print(f"Error processing file {filename} in {zip_file}: {e}")
        except Exception as e:
            print(f"Error reading zip file {zip_file}: {e}")

    if not all_data:
        print("No valid data to process.")
        return

    # Concatenate all the dataframes
    combined_df = pd.concat(all_data, ignore_index=True)

    return combined_df

def process_volte_cqi_samsung_daily(last_date):
    samsung_path = os.path.join(ROOT_DIRECTORY, "input", 'daily_volte_cqi_site', "samsung")

    columns_to_select = [
        "DATE",
        "REGION",
        "PROVINCE",
        "MUNICIPALITY",
        "SITE_ATT",
        "VOLTE_CQI",
        "ACC_VOLTE",
        "VOLTE_ERAB_CALL_DROP_RATE_QCI1",
        "VOLTE_ERAB_CALL_DROP_RATE_QCI5",
        "SRVCC_RATE",
        "USER_TRAFFIC_VOLTE"
    ]

    column_rename_map = {
        "DATE":"date",
        "REGION":"region",
        "PROVINCE":"province",
        "MUNICIPALITY":"municipality",
        "SITE_ATT":"site_att",
        "VOLTE_CQI": "volte_cqi_s",
        "ACC_VOLTE": "acc_volte_s",
        "VOLTE_ERAB_CALL_DROP_RATE_QCI1": "erab_drop_qci1_s",
        "VOLTE_ERAB_CALL_DROP_RATE_QCI5": "erab_drop_qci5_s",
        "SRVCC_RATE": "srvcc_rate_s",
        "USER_TRAFFIC_VOLTE": "user_traffic_volte_s"
    }

    all_data = []

    # Find all zip files in the Ericsson directory
    zip_files = glob.glob(os.path.join(samsung_path, "*.zip"))

    for zip_file in zip_files:
        try:
            with zipfile.ZipFile(zip_file, 'r') as z:
                for filename in z.namelist():
                    if filename.endswith('.csv'):
                        try:
                            with z.open(filename) as f:
                                df = pd.read_csv(f)
                                df = df[columns_to_select]
                                df = df.rename(columns=column_rename_map)

                                # Standardize date format and handle invalid dates
                                df['date'] = pd.to_datetime(df['date'], errors='coerce')

                                # Filter data to include only those dates greater than last_date
                                df = df[df['date'] > pd.to_datetime(last_date)]

                                all_data.append(df)
                        except Exception as e:
                            print(f"Error processing file {filename} in {zip_file}: {e}")
        except Exception as e:
            print(f"Error reading zip file {zip_file}: {e}")

    if not all_data:
        print("No valid data to process.")
        return

    # Concatenate all the dataframes
    combined_df = pd.concat(all_data, ignore_index=True)

    return combined_df

def insert_volte_cqi_vendor_daily(last_date):
    # Process each vendor's files
    ericsson_df = process_volte_cqi_ericsson_daily(last_date)
    huawei_df = process_volte_cqi_huawei_daily(last_date)
    nokia_df = process_volte_cqi_nokia_daily(last_date)
    samsung_df = process_volte_cqi_samsung_daily(last_date)

    # List of DataFrames to merge
    dfs = [ericsson_df, huawei_df, nokia_df, samsung_df]

    # Drop None results (if any vendor processing failed)
    dfs = [df for df in dfs if df is not None]

    if not dfs:
        print("No data to merge. All processing functions returned None.")
        return None

    # Merge DataFrames on the specified keys
    merged_df = dfs[0]
    for df in dfs[1:]:
        merged_df = pd.merge(
            merged_df,
            df,
            on=["date", "region", "province", "municipality", "site_att"],
            how="outer"
        )

    # Replace these variables with your PostgreSQL credentials
    username = POSTGRES_USERNAME
    password = POSTGRES_PASSWORD
    host = POSTGRES_HOST
    port = POSTGRES_PORT
    database_name = POSTGRES_DB

    # PostgreSQL connection string
    connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database_name}'

    # Set up SQLAlchemy engine
    engine = create_engine(connection_string)

    # Insert data in chunks of 100,000 rows
    chunk_size = 100000
    total_rows = len(merged_df)
    num_chunks = (total_rows // chunk_size) + (1 if total_rows % chunk_size != 0 else 0)

    try:
        for i, chunk in enumerate(range(0, total_rows, chunk_size), start=1):
            chunk_df = merged_df.iloc[chunk:chunk + chunk_size]
            chunk_df.to_sql(
                name='volte_cqi_vendor_daily',
                con=engine,
                schema='public',
                if_exists='append',  # Change to 'replace' to overwrite the table
                index=False,  # Don't write the DataFrame index as a column
                method='multi'  # Use multi-row insert for efficiency
            )
            print(f"Chunk {i}/{num_chunks} inserted ({len(chunk_df)} rows).")

        print("All data successfully inserted into PostgreSQL table `volte_cqi_vendor_daily`.")
    except Exception as e:
        print(f"Error inserting data into the database: {e}")
    finally:
        engine.dispose()
    return
