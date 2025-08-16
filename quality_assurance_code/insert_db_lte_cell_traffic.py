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


def insert_lte_traffic_cell_zip_file(last_date):

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

    # Define the vendors
    vendors = ['ericsson', 'nokia', 'huawei', 'samsung']

    # Iterate over vendors
    for vendor in vendors:
        # Path for the input directory for the specific vendor
        input_path = os.path.join(os.getenv('ROOT_DIRECTORY'), 'input', 'daily_lte_traffic_cell', vendor)

        # Iterate over each zip file in the vendor's directory
        for zip_file in glob.glob(os.path.join(input_path, '*.zip')):
            try:
                # Open the zip file
                with zipfile.ZipFile(zip_file, 'r') as z:
                    # Get the first file (assuming only one CSV per zip)
                    csv_filename = z.namelist()[0]
                    
                    # Read CSV file inside zip to DataFrame
                    with z.open(csv_filename) as csvfile:
                        df = pd.read_csv(csvfile, low_memory=False)
                    
                    # Add vendor column
                    df['VENDOR'] = vendor
                    
                    # Handle column structure for all LTE vendors (all have same structure)
                    if vendor in ['ericsson', 'huawei', 'nokia', 'samsung']:
                        # All LTE vendors: DATE, ENB_AGG, CELL, TRAFFIC_D_USER_PS_GB
                        df = df[['DATE', 'ENB_AGG', 'CELL', 'TRAFFIC_D_USER_PS_GB', 'VENDOR']]
                        df.columns = ['date', 'enb_agg', 'cell', 'traffic_d_user_ps_gb', 'vendor']

                    # Standardize date format and handle invalid dates
                    df['date'] = pd.to_datetime(df['date'], errors='coerce')
                    df = df.dropna(subset=['date'])

                    # Filter data to include only those dates greater than last_date
                    df = df[df['date'] > pd.to_datetime(last_date)]

                    if not df.empty:
                        # Remove duplicates based on 'date', 'cell', and 'vendor' columns
                        df.drop_duplicates(subset=['date', 'cell', 'vendor'], inplace=True)
                        
                        # Insert the DataFrame to PostgreSQL
                        df.to_sql('lte_cell_traffic_daily', engine, if_exists='append', index=False)
                        print(f"Successfully inserted new data from {zip_file} for dates after {last_date}")

            except SQLAlchemyError as e:
                print(f"Error inserting data into database: {e}")
            except Exception as e:
                print(f"Error processing file {zip_file}: {e}")

    print("All data has been processed insert_lte_traffic_cell_zip_file.")
