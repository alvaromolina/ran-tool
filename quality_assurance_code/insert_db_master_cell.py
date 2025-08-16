import os
import re
import psycopg2
import numpy as np
import pandas as pd
import dotenv
import time

# Load environment variables
dotenv.load_dotenv()
ROOT_DIRECTORY = os.getenv('ROOT_DIRECTORY')
POSTGRES_USERNAME = os.getenv('POSTGRES_USERNAME')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_DB = os.getenv('POSTGRES_DB')

def cell_3gH(workdir):
    """
    Function to process MasterCells_3gH.csv, filter, rename columns, and apply transformations.

    Parameters:
        workdir (str): The working directory containing the MasterCells_3gH.csv file.

    Returns:
        pd.DataFrame: Processed dataframe for MasterCells_3gH.csv.
    """
    # Define file path
    file_path = os.path.join(ROOT_DIRECTORY, "input", workdir, "MasterCells_3gH.csv")
    freq_band_path = os.path.join(ROOT_DIRECTORY, "code", "freq_band.csv")

    # Define column rules (FILTER=1 columns only)
    column_rules = [
        {'NAME': 'RNC', 'RENAME': 'rnc', 'TYPE': 'text'},
        {'NAME': 'NODEB', 'RENAME': 'node', 'TYPE': 'text'},
        {'NAME': 'CELL', 'RENAME': 'cell', 'TYPE': 'text'},
        {'NAME': 'REGION', 'RENAME': 'region', 'TYPE': 'text'},
        {'NAME': 'PROVINCE', 'RENAME': 'province', 'TYPE': 'text'},
        {'NAME': 'MUNICIPALITY', 'RENAME': 'municipality', 'TYPE': 'text'},
        {'NAME': 'LAT_WGS84', 'RENAME': 'lat_wgs84', 'TYPE': 'number'},
        {'NAME': 'LONG_WGS84', 'RENAME': 'long_wgs84', 'TYPE': 'number'},
        {'NAME': 'RNCID', 'RENAME': 'rncid', 'TYPE': 'number'},
        {'NAME': 'RBS_NAME', 'RENAME': 'rbs_name', 'TYPE': 'text'},
        {'NAME': 'LAC', 'RENAME': 'lac', 'TYPE': 'number'},
        {'NAME': 'CELLID', 'RENAME': 'cellid', 'TYPE': 'number'},
        {'NAME': 'AZIMUTH', 'RENAME': 'azimuth', 'TYPE': 'number'},
        {'NAME': 'H_BEAM', 'RENAME': 'h_beam', 'TYPE': 'number'},
        {'NAME': 'RAC', 'RENAME': 'rac', 'TYPE': 'number'},
        {'NAME': 'UARFCN', 'RENAME': 'uarfcn_dl', 'TYPE': 'number'},
        {'NAME': 'FREQ_BAND', 'RENAME': 'freq_band', 'TYPE': 'text'}
    ]

    # Read the CSV file
    df = pd.read_csv(file_path, low_memory=False)
    df = df[(df['ACTSTATUS'] == 1) & (df['UARFCN'] > 0) & (df['CELL'] != "")]

    # Read the frequency band lookup CSV
    freq_band = pd.read_csv(freq_band_path)

    # Filter and rename columns based on rules
    columns_to_keep = [rule['NAME'] for rule in column_rules]
    rename_map = {rule['NAME']: rule['RENAME'] for rule in column_rules}
    type_map = {rule['RENAME']: rule['TYPE'] for rule in column_rules}

    df = df[columns_to_keep].rename(columns=rename_map)

    # Enforce data types and apply transformations
    for col, col_type in type_map.items():
        if col_type == 'text':
            df[col] = df[col].astype(str)
        elif col_type == 'number':
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    #vendor & tech
    df['vendor'] = 'Huawei'
    df['tech'] = '3g'
    df['tac'] = ''

    # Merge with freq_band_lookup to add band_indicator and band_width
    df = df.merge(freq_band[['uarfcn_dl', 'uarfcn_ul', 'band_indicator', 'band_width']],
                  on='uarfcn_dl', how='left')

    # Replace missing values with 'not_found'
    df['uarfcn_ul'] = df['uarfcn_ul'].fillna('not_found')
    df['band_indicator'] = df['band_indicator'].fillna('not_found')
    df['band_width'] = df['band_width'].fillna('not_found')
    
    # Return the processed dataframe
    return df

def cell_4gH(workdir):
    """
    Function to process MasterCells_4gH.csv, filter, rename columns, and apply transformations.
    Adds band_indicator and band_width columns based on earfcn_dl lookup.

    Parameters:
        workdir (str): The working directory containing the MasterCells_4gH.csv file.

    Returns:
        pd.DataFrame: Processed dataframe for MasterCells_4gH.csv.
    """
    # Define file paths
    master_file_path = os.path.join(ROOT_DIRECTORY, "input", workdir, "MasterCells_4gH.csv")
    freq_band_path = os.path.join(ROOT_DIRECTORY, "code", "freq_band.csv")

    # Define column rules (FILTER=1 columns only)
    column_rules = [
        {'NAME': 'ENODEB', 'RENAME': 'node', 'TYPE': 'text'},
        {'NAME': 'CELL', 'RENAME': 'cell', 'TYPE': 'text'},
        {'NAME': 'REGION', 'RENAME': 'region', 'TYPE': 'text'},
        {'NAME': 'PROVINCE', 'RENAME': 'province', 'TYPE': 'text'},
        {'NAME': 'MUNICIPALITY', 'RENAME': 'municipality', 'TYPE': 'text'},
        {'NAME': 'LAT_WGS84', 'RENAME': 'lat_wgs84', 'TYPE': 'number'},
        {'NAME': 'LONG_WGS84', 'RENAME': 'long_wgs84', 'TYPE': 'number'},
        {'NAME': 'RBS_NAME', 'RENAME': 'rbs_name', 'TYPE': 'text'},
        {'NAME': 'TAC', 'RENAME': 'tac', 'TYPE': 'number'},
        {'NAME': 'CELLID', 'RENAME': 'cellid', 'TYPE': 'number'},
        {'NAME': 'AZIMUTH', 'RENAME': 'azimuth', 'TYPE': 'number'},
        {'NAME': 'H_BEAM', 'RENAME': 'h_beam', 'TYPE': 'number'},
        {'NAME': 'UARFCN', 'RENAME': 'earfcn_dl', 'TYPE': 'number'},
        {'NAME': 'FREQ_BAND', 'RENAME': 'freq_band', 'TYPE': 'text'},
    ]

    # Read the frequency band lookup CSV
    freq_band_lookup = pd.read_csv(freq_band_path)

    # Read the MasterCells_4gH CSV file
    df = pd.read_csv(master_file_path, low_memory=False)
    df = df[(df['ACTSTATUS'] == 1) & (df['UARFCN'] > 0) & (df['CELL'] != "")]

    # Filter and rename columns based on rules
    columns_to_keep = [rule['NAME'] for rule in column_rules]
    rename_map = {rule['NAME']: rule['RENAME'] for rule in column_rules}
    type_map = {rule['RENAME']: rule['TYPE'] for rule in column_rules}

    df = df[columns_to_keep].rename(columns=rename_map)

    # Enforce data types and apply transformations
    for col, col_type in type_map.items():
        if col_type == 'text':
            df[col] = df[col].astype(str)
        elif col_type == 'number':
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Add vendor, tech, and auxiliary columns
    df['vendor'] = 'Huawei'
    df['tech'] = '4g'
    df['lac'] = ''
    df['rac'] = ''

    # Merge with freq_band_lookup to add band_indicator and band_width
    df = df.merge(freq_band_lookup[['earfcn_dl', 'earfcn_ul', 'band_indicator', 'band_width']],
                  on='earfcn_dl', how='left')

    # Replace missing values with 'not_found'
    df['earfcn_ul'] = df['earfcn_ul'].fillna('not_found')
    df['band_indicator'] = df['band_indicator'].fillna('not_found')
    df['band_width'] = df['band_width'].fillna('not_found')

    # Return the processed dataframe
    return df


def cell_3gE(workdir):
    """
    Function to process MasterCells_3gE.csv, filter, rename columns, and apply transformations.
    Adds band_indicator and band_width columns based on uarfcn lookup.
    Special Rule: rncid is derived from rnc (extract the numeric part of rnc).

    Parameters:
        workdir (str): The working directory containing the MasterCells_3gE.csv file.

    Returns:
        pd.DataFrame: Processed dataframe for MasterCells_3gE.csv.
    """
    # Define file paths
    master_file_path = os.path.join(ROOT_DIRECTORY, "input", workdir, "MasterCells_3gE.csv")
    freq_band_path = os.path.join(ROOT_DIRECTORY, "code", "freq_band.csv")

    # Define column rules (FILTER=1 columns only)
    column_rules = [
        {'NAME': 'RNC', 'RENAME': 'rnc', 'TYPE': 'text'},
        {'NAME': 'NODEB', 'RENAME': 'node', 'TYPE': 'text'},
        {'NAME': 'CELL', 'RENAME': 'cell', 'TYPE': 'text'},
        {'NAME': 'REGION', 'RENAME': 'region', 'TYPE': 'text'},
        {'NAME': 'PROVINCE', 'RENAME': 'province', 'TYPE': 'text'},
        {'NAME': 'MUNICIPALITY', 'RENAME': 'municipality', 'TYPE': 'text'},
        {'NAME': 'LAT_WGS84', 'RENAME': 'lat_wgs84', 'TYPE': 'number'},
        {'NAME': 'LONG_WGS84', 'RENAME': 'long_wgs84', 'TYPE': 'number'},
        {'NAME': 'RNCID', 'RENAME': 'rncid', 'TYPE': 'number'},  # This will be derived from RNC
        {'NAME': 'RBS_NAME', 'RENAME': 'rbs_name', 'TYPE': 'text'},
        {'NAME': 'LAC', 'RENAME': 'lac', 'TYPE': 'number'},
        {'NAME': 'CELLID', 'RENAME': 'cellid', 'TYPE': 'number'},
        {'NAME': 'AZIMUTH', 'RENAME': 'azimuth', 'TYPE': 'number'},
        {'NAME': 'H_BEAM', 'RENAME': 'h_beam', 'TYPE': 'number'},
        {'NAME': 'RAC', 'RENAME': 'rac', 'TYPE': 'number'},
        {'NAME': 'UARFCN', 'RENAME': 'uarfcn_dl', 'TYPE': 'number'},
        {'NAME': 'FREQ_BAND', 'RENAME': 'freq_band', 'TYPE': 'text'},
    ]

    # Read the frequency band lookup CSV
    freq_band_lookup = pd.read_csv(freq_band_path)

    # Read the MasterCells_3gE CSV file
    df = pd.read_csv(master_file_path, low_memory=False)
    df = df[(df['ACTSTATUS'] == 1) & (df['UARFCN'] > 0) & (df['CELL'] != "")]

    # Filter and rename columns based on rules
    columns_to_keep = [rule['NAME'] for rule in column_rules]
    rename_map = {rule['NAME']: rule['RENAME'] for rule in column_rules}
    type_map = {rule['RENAME']: rule['TYPE'] for rule in column_rules}

    df = df[columns_to_keep].rename(columns=rename_map)

    # Enforce data types and apply transformations
    for col, col_type in type_map.items():
        if col_type == 'text':
            df[col] = df[col].astype(str)
        elif col_type == 'number':
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Special Rule: Derive rncid from rnc
    df['rncid'] = df['rnc'].str.extract(r'(\d+)$', expand=False).astype(int)

    # Add vendor, tech, and tac columns
    df['vendor'] = 'Ericsson'
    df['tech'] = '3g'
    df['tac'] = ''

    # Merge with freq_band_lookup to add band_indicator and band_width
    df = df.merge(freq_band_lookup[['uarfcn_dl', 'uarfcn_ul', 'band_indicator', 'band_width']],
                  on='uarfcn_dl', how='left')

    # Replace missing values with 'not_found'
    df['uarfcn_ul'] = df['uarfcn_ul'].fillna('not_found')
    df['band_indicator'] = df['band_indicator'].fillna('not_found')
    df['band_width'] = df['band_width'].fillna('not_found')

    # Return the processed dataframe
    return df

def cell_4gE(workdir):
    """
    Function to process MasterCells_4gE.csv, filter, rename columns, and apply transformations.
    Adds band_indicator and band_width columns based on earfcn_dl lookup.

    Parameters:
        workdir (str): The working directory containing the MasterCells_4gE.csv file.

    Returns:
        pd.DataFrame: Processed dataframe for MasterCells_4gE.csv.
    """
    # Define file paths
    master_file_path = os.path.join(ROOT_DIRECTORY, "input", workdir, "MasterCells_4gE.csv")
    freq_band_path = os.path.join(ROOT_DIRECTORY, "code", "freq_band.csv")

    # Define column rules (FILTER=1 columns only, from the refreshed table definitions)
    column_rules = [
        {'NAME': 'ENODEB', 'RENAME': 'node', 'TYPE': 'text'},
        {'NAME': 'CELL', 'RENAME': 'cell', 'TYPE': 'text'},
        {'NAME': 'REGION', 'RENAME': 'region', 'TYPE': 'text'},
        {'NAME': 'PROVINCE', 'RENAME': 'province', 'TYPE': 'text'},
        {'NAME': 'MUNICIPALITY', 'RENAME': 'municipality', 'TYPE': 'text'},
        {'NAME': 'LAT_WGS84', 'RENAME': 'lat_wgs84', 'TYPE': 'number'},
        {'NAME': 'LONG_WGS84', 'RENAME': 'long_wgs84', 'TYPE': 'number'},
        {'NAME': 'RBS_NAME', 'RENAME': 'rbs_name', 'TYPE': 'text'},
        {'NAME': 'TAC', 'RENAME': 'tac', 'TYPE': 'number'},
        {'NAME': 'CELLID', 'RENAME': 'cellid', 'TYPE': 'number'},
        {'NAME': 'AZIMUTH', 'RENAME': 'azimuth', 'TYPE': 'number'},
        {'NAME': 'H_BEAM', 'RENAME': 'h_beam', 'TYPE': 'number'},
        {'NAME': 'RAC', 'RENAME': 'rac', 'TYPE': 'number'},
        {'NAME': 'UARFCN', 'RENAME': 'earfcn_dl', 'TYPE': 'number'},
        {'NAME': 'FREQ_BAND', 'RENAME': 'freq_band', 'TYPE': 'text'},
    ]

    # Read the frequency band lookup CSV
    freq_band_lookup = pd.read_csv(freq_band_path)

    # Read the MasterCells_4gE CSV file
    df = pd.read_csv(master_file_path, low_memory=False)
    df = df[(df['ACTSTATUS'] == 1) & (df['UARFCN'] > 0) & (df['CELL'] != "")]

    # Filter and rename columns based on rules
    columns_to_keep = [rule['NAME'] for rule in column_rules]
    rename_map = {rule['NAME']: rule['RENAME'] for rule in column_rules}
    type_map = {rule['RENAME']: rule['TYPE'] for rule in column_rules}

    df = df[columns_to_keep].rename(columns=rename_map)

    # Enforce data types and apply transformations
    for col, col_type in type_map.items():
        if col_type == 'text':
            df[col] = df[col].astype(str)
        elif col_type == 'number':
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Add vendor, tech, and auxiliary columns
    df['vendor'] = 'Ericsson'
    df['tech'] = '4g'
    df['lac'] = ''
    df['rac'] = ''

    # Merge with freq_band_lookup to add band_indicator and band_width
    df = df.merge(freq_band_lookup[['earfcn_dl', 'earfcn_ul', 'band_indicator', 'band_width']],
                  on='earfcn_dl', how='left')

    # Replace missing values with 'not_found'
    df['earfcn_ul'] = df['earfcn_ul'].fillna('not_found')
    df['band_indicator'] = df['band_indicator'].fillna('not_found')
    df['band_width'] = df['band_width'].fillna('not_found')

    # Return the processed dataframe
    return df

def cell_5gE(workdir):
    """
    Function to process MasterCells_5gE.csv, filter, rename columns, and apply transformations.
    Adds band_indicator and band_width columns based on earfcn_dl lookup.

    Parameters:
        workdir (str): The working directory containing the MasterCells_5gE.csv file.

    Returns:
        pd.DataFrame: Processed dataframe for MasterCells_5gE.csv.
    """
    # Define file paths
    master_file_path = os.path.join(ROOT_DIRECTORY, "input", workdir, "MasterCells_5gE.csv")
    freq_band_path = os.path.join(ROOT_DIRECTORY, "code", "freq_band.csv")

    # Define column rules (FILTER=1 columns only, based on the refreshed table definitions)
    column_rules = [
        {'NAME': 'MANAGED_OBJECT2', 'RENAME': 'node', 'TYPE': 'text'},
        {'NAME': 'MANAGED_OBJECT3', 'RENAME': 'cell', 'TYPE': 'text'},
        {'NAME': 'REGION', 'RENAME': 'region', 'TYPE': 'text'},
        {'NAME': 'PROVINCE', 'RENAME': 'province', 'TYPE': 'text'},
        {'NAME': 'MUNICIPALITY', 'RENAME': 'municipality', 'TYPE': 'text'},
        {'NAME': 'LAT_WGS84', 'RENAME': 'lat_wgs84', 'TYPE': 'number'},
        {'NAME': 'LONG_WGS84', 'RENAME': 'long_wgs84', 'TYPE': 'number'},
        {'NAME': 'RBS_NAME', 'RENAME': 'rbs_name', 'TYPE': 'text'},
        {'NAME': 'TAC', 'RENAME': 'tac', 'TYPE': 'number'},
        {'NAME': 'CELLID', 'RENAME': 'cellid', 'TYPE': 'number'},
        {'NAME': 'AZIMUTH', 'RENAME': 'azimuth', 'TYPE': 'number'},
        {'NAME': 'H_BEAM', 'RENAME': 'h_beam', 'TYPE': 'number'},
        {'NAME': 'RAC', 'RENAME': 'rac', 'TYPE': 'number'},
        {'NAME': 'UARFCN', 'RENAME': 'earfcn_dl', 'TYPE': 'number'},
        {'NAME': 'FREQ_BAND', 'RENAME': 'freq_band', 'TYPE': 'text'},
    ]

    # Read the frequency band lookup CSV
    freq_band_lookup = pd.read_csv(freq_band_path)

    # Read the MasterCells_5gE CSV file
    df = pd.read_csv(master_file_path, low_memory=False)
    df = df[(df['ACTSTATUS'] == 1) & (df['UARFCN'] > 0) & (df['MANAGED_OBJECT3'] != "")]

    # Filter and rename columns based on rules
    columns_to_keep = [rule['NAME'] for rule in column_rules]
    rename_map = {rule['NAME']: rule['RENAME'] for rule in column_rules}
    type_map = {rule['RENAME']: rule['TYPE'] for rule in column_rules}

    df = df[columns_to_keep].rename(columns=rename_map)

    # Enforce data types and apply transformations
    for col, col_type in type_map.items():
        if col_type == 'text':
            df[col] = df[col].astype(str)
        elif col_type == 'number':
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Add vendor, tech, and auxiliary columns
    df['vendor'] = 'Ericsson'
    df['tech'] = '5g'
    df['lac'] = ''
    df['rac'] = ''

    # Merge with freq_band_lookup to add band_indicator and band_width
    df = df.merge(freq_band_lookup[['earfcn_dl', 'earfcn_ul', 'band_indicator', 'band_width']],
                  on='earfcn_dl', how='left')

    # Replace missing values with 'not_found'
    df['earfcn_ul'] = df['earfcn_ul'].fillna('not_found')
    df['band_indicator'] = df['band_indicator'].fillna('not_found')
    df['band_width'] = df['band_width'].fillna('not_found')

    # Return the processed dataframe
    return df

def cell_3gN(workdir):
    """
    Function to process MasterCells_3gN.csv, filter, rename columns, and apply transformations.
    Adds band_indicator and band_width columns based on uarfcn_dl lookup.

    Parameters:
        workdir (str): The working directory containing the MasterCells_3gN.csv file.

    Returns:
        pd.DataFrame: Processed dataframe for MasterCells_3gN.csv.
    """
    # Define file paths
    master_file_path = os.path.join(ROOT_DIRECTORY, "input", workdir, "MasterCells_3gN.csv")
    freq_band_path = os.path.join(ROOT_DIRECTORY, "code", "freq_band.csv")

    # Define column rules (FILTER=1 columns only, based on the refreshed table definitions)
    column_rules = [
        {'NAME': 'RNC', 'RENAME': 'rnc', 'TYPE': 'text'},
        {'NAME': 'NODEB', 'RENAME': 'node', 'TYPE': 'text'},
        {'NAME': 'CELL', 'RENAME': 'cell', 'TYPE': 'text'},
        {'NAME': 'REGION', 'RENAME': 'region', 'TYPE': 'text'},
        {'NAME': 'PROVINCE', 'RENAME': 'province', 'TYPE': 'text'},
        {'NAME': 'MUNICIPALITY', 'RENAME': 'municipality', 'TYPE': 'text'},
        {'NAME': 'LAT_WGS84', 'RENAME': 'lat_wgs84', 'TYPE': 'number'},
        {'NAME': 'LONG_WGS84', 'RENAME': 'long_wgs84', 'TYPE': 'number'},
        {'NAME': 'RNCID', 'RENAME': 'rncid', 'TYPE': 'number'},  # Derived from RNC
        {'NAME': 'RBS_NAME', 'RENAME': 'rbs_name', 'TYPE': 'text'},
        {'NAME': 'LAC', 'RENAME': 'lac', 'TYPE': 'number'},
        {'NAME': 'CELLID', 'RENAME': 'cellid', 'TYPE': 'number'},
        {'NAME': 'AZIMUTH', 'RENAME': 'azimuth', 'TYPE': 'number'},
        {'NAME': 'H_BEAM', 'RENAME': 'h_beam', 'TYPE': 'number'},
        {'NAME': 'RAC', 'RENAME': 'rac', 'TYPE': 'number'},
        {'NAME': 'UARFCN', 'RENAME': 'uarfcn_dl', 'TYPE': 'number'},
        {'NAME': 'FREQ_BAND', 'RENAME': 'freq_band', 'TYPE': 'text'},
    ]

    # Read the frequency band lookup CSV
    freq_band_lookup = pd.read_csv(freq_band_path)

    # Read the MasterCells_3gN CSV file
    df = pd.read_csv(master_file_path, low_memory=False)
    df = df[(df['ACTSTATUS'] == 1) & (df['UARFCN'] > 0) & (df['CELL'] != "")]

    # Filter and rename columns based on rules
    columns_to_keep = [rule['NAME'] for rule in column_rules]
    rename_map = {rule['NAME']: rule['RENAME'] for rule in column_rules}
    type_map = {rule['RENAME']: rule['TYPE'] for rule in column_rules}

    df = df[columns_to_keep].rename(columns=rename_map)

    # Enforce data types and apply transformations
    for col, col_type in type_map.items():
        if col_type == 'text':
            df[col] = df[col].astype(str)
        elif col_type == 'number':
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Add vendor, tech, and auxiliary columns
    df['vendor'] = 'Nokia'
    df['tech'] = '3g'
    df['tac'] = ''

    # Derive rncid from rnc
    df['rncid'] = df['rnc'].str.extract(r'(\d+)$', expand=False).astype(int)

    # Merge with freq_band_lookup to add band_indicator and band_width
    df = df.merge(freq_band_lookup[['uarfcn_dl', 'uarfcn_ul', 'band_indicator', 'band_width']],
                  on='uarfcn_dl', how='left')

    # Replace missing values with 'not_found'
    df['uarfcn_ul'] = df['uarfcn_ul'].fillna('not_found')
    df['band_indicator'] = df['band_indicator'].fillna('not_found')
    df['band_width'] = df['band_width'].fillna('not_found')

    # Return the processed dataframe
    return df

def cell_4gN(workdir):
    """
    Function to process MasterCells_4gN.csv, filter, rename columns, and apply transformations.
    Adds band_indicator and band_width columns based on earfcn_dl lookup.

    Parameters:
        workdir (str): The working directory containing the MasterCells_4gN.csv file.

    Returns:
        pd.DataFrame: Processed dataframe for MasterCells_4gN.csv.
    """
    # Define file paths
    master_file_path = os.path.join(ROOT_DIRECTORY, "input", workdir, "MasterCells_4gN.csv")
    freq_band_path = os.path.join(ROOT_DIRECTORY, "code", "freq_band.csv")

    # Define column rules (FILTER=1 columns only, based on the refreshed table definitions)
    column_rules = [
        {'NAME': 'ENODEB', 'RENAME': 'node', 'TYPE': 'text'},
        {'NAME': 'CELL', 'RENAME': 'cell', 'TYPE': 'text'},
        {'NAME': 'REGION', 'RENAME': 'region', 'TYPE': 'text'},
        {'NAME': 'PROVINCE', 'RENAME': 'province', 'TYPE': 'text'},
        {'NAME': 'MUNICIPALITY', 'RENAME': 'municipality', 'TYPE': 'text'},
        {'NAME': 'LAT_WGS84', 'RENAME': 'lat_wgs84', 'TYPE': 'number'},
        {'NAME': 'LONG_WGS84', 'RENAME': 'long_wgs84', 'TYPE': 'number'},
        {'NAME': 'RBS_NAME', 'RENAME': 'rbs_name', 'TYPE': 'text'},
        {'NAME': 'TAC', 'RENAME': 'tac', 'TYPE': 'number'},
        {'NAME': 'CELLID', 'RENAME': 'cellid', 'TYPE': 'number'},
        {'NAME': 'AZIMUTH', 'RENAME': 'azimuth', 'TYPE': 'number'},
        {'NAME': 'H_BEAM', 'RENAME': 'h_beam', 'TYPE': 'number'},
        {'NAME': 'UARFCN', 'RENAME': 'earfcn_dl', 'TYPE': 'number'},
        {'NAME': 'FREQ_BAND', 'RENAME': 'freq_band', 'TYPE': 'text'},
    ]

    # Read the frequency band lookup CSV
    freq_band_lookup = pd.read_csv(freq_band_path)

    # Read the MasterCells_4gN CSV file
    df = pd.read_csv(master_file_path, low_memory=False)
    df = df[(df['ACTSTATUS'] == 1) & (df['UARFCN'] > 0) & (df['CELL'] != "")]

    # Filter and rename columns based on rules
    columns_to_keep = [rule['NAME'] for rule in column_rules]
    rename_map = {rule['NAME']: rule['RENAME'] for rule in column_rules}
    type_map = {rule['RENAME']: rule['TYPE'] for rule in column_rules}

    df = df[columns_to_keep].rename(columns=rename_map)

    # Enforce data types and apply transformations
    for col, col_type in type_map.items():
        if col_type == 'text':
            df[col] = df[col].astype(str)
        elif col_type == 'number':
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Add vendor, tech, and auxiliary columns
    df['vendor'] = 'Nokia'
    df['tech'] = '4g'
    df['lac'] = ''
    df['rac'] = ''

    # Merge with freq_band_lookup to add band_indicator and band_width
    df = df.merge(freq_band_lookup[['earfcn_dl', 'earfcn_ul', 'band_indicator', 'band_width']],
                  on='earfcn_dl', how='left')

    # Replace missing values with 'not_found'
    df['earfcn_ul'] = df['earfcn_ul'].fillna('not_found')
    df['band_indicator'] = df['band_indicator'].fillna('not_found')
    df['band_width'] = df['band_width'].fillna('not_found')

    # Return the processed dataframe
    return df

def cell_5gN(workdir):
    """
    Function to process MasterCells_5gN.csv, filter, rename columns, and apply transformations.
    Adds band_indicator and band_width columns based on earfcn_dl lookup.

    Parameters:
        workdir (str): The working directory containing the MasterCells_5gN.csv file.

    Returns:
        pd.DataFrame: Processed dataframe for MasterCells_5gN.csv.
    """
    # Define file paths
    master_file_path = os.path.join(ROOT_DIRECTORY, "input", workdir, "MasterCells_5gN.csv")
    freq_band_path = os.path.join(ROOT_DIRECTORY, "code", "freq_band.csv")

    # Define column rules (FILTER=1 columns only, based on the refreshed table definitions)
    column_rules = [
        {'NAME': 'MANAGED_OBJECT2', 'RENAME': 'node', 'TYPE': 'text'},
        {'NAME': 'MANAGED_OBJECT3', 'RENAME': 'cell', 'TYPE': 'text'},
        {'NAME': 'REGION', 'RENAME': 'region', 'TYPE': 'text'},
        {'NAME': 'PROVINCE', 'RENAME': 'province', 'TYPE': 'text'},
        {'NAME': 'MUNICIPALITY', 'RENAME': 'municipality', 'TYPE': 'text'},
        {'NAME': 'LAT_WGS84', 'RENAME': 'lat_wgs84', 'TYPE': 'number'},
        {'NAME': 'LONG_WGS84', 'RENAME': 'long_wgs84', 'TYPE': 'number'},
        {'NAME': 'RBS_NAME', 'RENAME': 'rbs_name', 'TYPE': 'text'},
        {'NAME': 'TAC', 'RENAME': 'tac', 'TYPE': 'number'},
        {'NAME': 'CELLID', 'RENAME': 'cellid', 'TYPE': 'number'},
        {'NAME': 'AZIMUTH', 'RENAME': 'azimuth', 'TYPE': 'number'},
        {'NAME': 'H_BEAM', 'RENAME': 'h_beam', 'TYPE': 'number'},
        {'NAME': 'RAC', 'RENAME': 'rac', 'TYPE': 'number'},
        {'NAME': 'UARFCN', 'RENAME': 'earfcn_dl', 'TYPE': 'number'},
        {'NAME': 'FREQ_BAND', 'RENAME': 'freq_band', 'TYPE': 'text'},
    ]

    # Read the frequency band lookup CSV
    freq_band_lookup = pd.read_csv(freq_band_path)

    # Read the MasterCells_5gN CSV file
    df = pd.read_csv(master_file_path, low_memory=False)
    df = df[(df['ACTSTATUS'] == 1) & (df['UARFCN'] > 0) & (df['MANAGED_OBJECT3'] != "")]

    # Filter and rename columns based on rules
    columns_to_keep = [rule['NAME'] for rule in column_rules]
    rename_map = {rule['NAME']: rule['RENAME'] for rule in column_rules}
    type_map = {rule['RENAME']: rule['TYPE'] for rule in column_rules}

    df = df[columns_to_keep].rename(columns=rename_map)

    # Enforce data types and apply transformations
    for col, col_type in type_map.items():
        if col_type == 'text':
            df[col] = df[col].astype(str)
        elif col_type == 'number':
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Add vendor, tech, and auxiliary columns
    df['vendor'] = 'Nokia'
    df['tech'] = '5g'
    df['lac'] = ''
    df['rac'] = ''

    # Merge with freq_band_lookup to add band_indicator and band_width
    df = df.merge(freq_band_lookup[['earfcn_dl', 'earfcn_ul', 'band_indicator', 'band_width']],
                  on='earfcn_dl', how='left')

    # Replace missing values with 'not_found'
    df['earfcn_ul'] = df['earfcn_ul'].fillna('not_found')
    df['band_indicator'] = df['band_indicator'].fillna('not_found')
    df['band_width'] = df['band_width'].fillna('not_found')

    # Return the processed dataframe
    return df

def cell_4gS(workdir):
    """
    Function to process MasterCells_4gS.csv, filter, rename columns, and apply transformations.
    Adds band_indicator and band_width columns based on earfcn_dl lookup.
    Special Rule: Convert TAC from hexadecimal to decimal.

    Parameters:
        workdir (str): The working directory containing the MasterCells_4gS.csv file.

    Returns:
        pd.DataFrame: Processed dataframe for MasterCells_4gS.csv.
    """
    # Define file paths
    master_file_path = os.path.join(ROOT_DIRECTORY, "input", workdir, "MasterCells_4gS.csv")
    freq_band_path = os.path.join(ROOT_DIRECTORY, "code", "freq_band.csv")

    # Define column rules (FILTER=1 columns only, based on the refreshed table definitions)
    column_rules = [
        {'NAME': 'ENODEB', 'RENAME': 'node', 'TYPE': 'text'},
        {'NAME': 'CELL', 'RENAME': 'cell', 'TYPE': 'text'},
        {'NAME': 'REGION', 'RENAME': 'region', 'TYPE': 'text'},
        {'NAME': 'PROVINCE', 'RENAME': 'province', 'TYPE': 'text'},
        {'NAME': 'MUNICIPALITY', 'RENAME': 'municipality', 'TYPE': 'text'},
        {'NAME': 'LAT_WGS84', 'RENAME': 'lat_wgs84', 'TYPE': 'number'},
        {'NAME': 'LONG_WGS84', 'RENAME': 'long_wgs84', 'TYPE': 'number'},
        {'NAME': 'RBS_NAME', 'RENAME': 'rbs_name', 'TYPE': 'text'},
        {'NAME': 'TAC', 'RENAME': 'tac', 'TYPE': 'number'},  # Special transformation
        {'NAME': 'CELLID', 'RENAME': 'cellid', 'TYPE': 'number'},
        {'NAME': 'AZIMUTH', 'RENAME': 'azimuth', 'TYPE': 'number'},
        {'NAME': 'H_BEAM', 'RENAME': 'h_beam', 'TYPE': 'number'},
        {'NAME': 'UARFCN', 'RENAME': 'earfcn_dl', 'TYPE': 'number'},
        {'NAME': 'FREQ_BAND', 'RENAME': 'freq_band', 'TYPE': 'text'},
    ]

    # Read the frequency band lookup CSV
    freq_band_lookup = pd.read_csv(freq_band_path)

    # Read the MasterCells_4gS CSV file
    df = pd.read_csv(master_file_path, low_memory=False)
    df = df[(df['ACTSTATUS'] == 1) & (df['UARFCN'] > 0) & (df['CELL'] != "")]

    # Filter and rename columns based on rules
    columns_to_keep = [rule['NAME'] for rule in column_rules]
    rename_map = {rule['NAME']: rule['RENAME'] for rule in column_rules}
    type_map = {rule['RENAME']: rule['TYPE'] for rule in column_rules}

    df = df[columns_to_keep].rename(columns=rename_map)

    # Special Rule: Convert TAC from hex to decimal
    def hex_to_dec(value):
        """
        Convert a hexadecimal string to a decimal integer.
        If the value is not hexadecimal, return as is.
        """
        try:
            return int(value, 16)
        except (ValueError, TypeError):
            return value

    df['tac'] = df['tac'].apply(hex_to_dec)

    # Enforce data types and apply transformations
    for col, col_type in type_map.items():
        if col_type == 'text':
            df[col] = df[col].astype(str)
        elif col_type == 'number':
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Add vendor, tech, and auxiliary columns
    df['vendor'] = 'Samsung'
    df['tech'] = '4g'
    df['lac'] = ''
    df['rac'] = ''

    # Merge with freq_band_lookup to add band_indicator and band_width
    df = df.merge(freq_band_lookup[['earfcn_dl', 'earfcn_ul', 'band_indicator', 'band_width']],
                  on='earfcn_dl', how='left')

    # Replace missing values with 'not_found'
    df['earfcn_ul'] = df['earfcn_ul'].fillna('not_found')
    df['band_indicator'] = df['band_indicator'].fillna('not_found')
    df['band_width'] = df['band_width'].fillna('not_found')

    # Return the processed dataframe
    return df

def create_master_cell(workdir):
    """
    Process all MasterCells files, correct missing fields, and return:
    - df_master_cell: cleaned and merged cell-level DataFrame
    - df_master_node: node-level summary DataFrame

    Parameters:
        workdir (str): Folder under ROOT_DIRECTORY/input/ containing the input files.

    Returns:
        df_master_cell, df_master_node
    """
    # Load and merge all sources
    df_3gH = cell_3gH(workdir)
    df_4gH = cell_4gH(workdir)
    df_3gE = cell_3gE(workdir)
    df_4gE = cell_4gE(workdir)
    df_5gE = cell_5gE(workdir)
    df_3gN = cell_3gN(workdir)
    df_4gN = cell_4gN(workdir)
    df_4gS = cell_4gS(workdir)
    df_5gN = cell_5gN(workdir)

    merged_df = pd.concat([
        df_3gH, df_4gH, df_3gE, df_4gE, df_5gE, df_3gN, df_4gN, df_4gS, df_5gN
    ], ignore_index=True)

    # Filter valid nodes for master_node summary
    merged2_df = merged_df[merged_df['rbs_name'] != "nan"]

    # Correct lat/lon with max/min per rbs_name
    lat_long_updates = merged_df.groupby('rbs_name').agg({
        'lat_wgs84': 'max',
        'long_wgs84': 'min'
    }).reset_index()
    merged_df = merged_df.merge(lat_long_updates, on='rbs_name', suffixes=('', '_updated'))
    merged_df['lat_wgs84'] = merged_df['lat_wgs84_updated']
    merged_df['long_wgs84'] = merged_df['long_wgs84_updated']
    merged_df.drop(columns=['lat_wgs84_updated', 'long_wgs84_updated'], inplace=True)

    # Create master_node dataframe
    df_master_node = merged2_df.groupby('node').agg({
        'region': 'first',
        'province': 'first',
        'municipality': 'first',
        'lat_wgs84': 'max',
        'long_wgs84': 'min',
        'rbs_name': 'first',
        'vendor': 'first'
    }).reset_index()

    # Correct missing rbs_name fields from node summary
    def correct_row(row):
        if pd.isna(row['rbs_name']) or row['rbs_name'] == "nan":
            node_lookup = df_master_node[df_master_node['node'] == row['node']]
            if not node_lookup.empty:
                row.update(node_lookup.iloc[0])
        return row

    merged_df = merged_df.apply(correct_row, axis=1)

    # Create sector if pattern is found
    merged_df['sector'] = None
    merged_df = merged_df.rename(columns={'SECTOR': 'sector'})

    def extract_sector(cell):
        match = re.match(r'^[a-zA-Z0-9]+_(\d+)_', cell)
        return int(match.group(1)) if match else np.nan

    merged_df['sector'] = merged_df.apply(
        lambda row: extract_sector(row['cell']) if pd.isna(row['sector']) else row['sector'],
        axis=1
    )

    # Final reorder
    column_order = [
        'region', 'province', 'municipality', 'rbs_name', 'tech', 'node', 'cell', 'sector',
        'rnc', 'rncid', 'cellid', 'vendor', 'uarfcn_dl', 'uarfcn_ul', 'earfcn_dl', 'earfcn_ul',
        'lac', 'rac', 'tac', 'freq_band', 'band_indicator', 'band_width',
        'lat_wgs84', 'long_wgs84', 'azimuth', 'h_beam'
    ]
    df_master_cell = merged_df[column_order]

    # Rename to match DB schema
    rename_map = {
        "rbs_name": "att_name",
        "tech": "att_tech",
        "node": "node_id",
        "cell": "cell_name",
        "sector": "physical_sector",
        "rnc": "rnc_name",
        "rncid": "rnc_id",
        "cellid": "cell_id",
        "uarfcn_dl": "dl_arfcn",
        "uarfcn_ul": "ul_arfcn",
        "lat_wgs84": "latitude",
        "long_wgs84": "longitude",
        "h_beam": "beam"
    }
    df_master_cell.rename(columns=rename_map, inplace=True)

    # Rename to match DB schema
    rename_map = {
        "rbs_name": "att_name",
        "lat_wgs84": "latitude",
        "long_wgs84": "longitude"
    }
    df_master_node.rename(columns=rename_map, inplace=True)

    return df_master_cell, df_master_node


def insert_master_cell(df_master_cell):
    import psycopg2
    import numpy as np

    df_master_cell.replace({"": None, "NaN": None, np.nan: None, "-": None}, inplace=True)

    # Define all numeric columns you expect to insert as INTEGER or FLOAT
    numeric_columns = [
        "ul_arfcn", "dl_arfcn", "earfcn_dl", "earfcn_ul",
        "cell_id", "local_cell_id", "psc_pci", "rnc_id",
        "lac", "tac", "rac", "latitude", "longitude", "azimuth", "beam"
    ]

    # Safely convert to numeric; force invalid entries to NaN (which becomes None)
    for col in numeric_columns:
        if col in df_master_cell.columns:
            df_master_cell[col] = pd.to_numeric(df_master_cell[col], errors="coerce").fillna(0)

    columns = [
    "region", "province", "municipality", "att_name", "att_tech", "node_id", "cell_name",
    "physical_sector", "rnc_name", "rnc_id", "cell_id", "vendor", "dl_arfcn", "ul_arfcn",
    "earfcn_dl", "earfcn_ul", "lac", "rac", "tac", "freq_band", "band_indicator", "band_width",
    "latitude", "longitude", "azimuth", "beam"
    ]

    for col in columns:
        if col not in df_master_cell.columns:
            df_master_cell[col] = None

    try:
        with psycopg2.connect(
            user=POSTGRES_USERNAME,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB
        ) as conn, conn.cursor() as cursor:
            for row in df_master_cell.itertuples(index=False):
                values = [getattr(row, col) for col in columns]
                cursor.execute(
                    f"""
                    INSERT INTO master_cell ({', '.join(columns)})
                    VALUES ({', '.join(['%s'] * len(columns))})
                    ON CONFLICT (cell_name) DO NOTHING;
                    """,
                    values
                )
            conn.commit()
            print("Data inserted successfully into master_cell.")
    except Exception as e:
        print(f"An error occurred during insertion: {e}")


def insert_master_node(df_master_node):
    import psycopg2
    import numpy as np

    df_master_node.replace({"": None, "NaN": None, np.nan: None, "-": None}, inplace=True)

    # Ensure numeric columns are converted properly
    numeric_columns = ["latitude", "longitude"]  # adapt for master_node
    for col in numeric_columns:
        if col in df_master_node.columns:
            df_master_node[col] = pd.to_numeric(df_master_node[col], errors="coerce").fillna(0)

    columns = [
        "node", "region", "province", "municipality",
        "latitude", "longitude", "att_name", "vendor"
    ]

    for col in columns:
        if col not in df_master_node.columns:
            df_master_node[col] = None

    try:
        with psycopg2.connect(
            user=POSTGRES_USERNAME,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB
        ) as conn, conn.cursor() as cursor:
            for row in df_master_node.itertuples(index=False):
                values = [getattr(row, col) for col in columns]
                cursor.execute(
                    f"""
                    INSERT INTO master_node ({', '.join(columns)})
                    VALUES ({', '.join(['%s'] * len(columns))})
                    ON CONFLICT (node) DO NOTHING;
                    """,
                    values
                )
            conn.commit()
            print("Data inserted successfully into master_node.")
    except Exception as e:
        print(f"An error occurred during insertion: {e}")


def insert_master_cell_total(df_master_cell):
    """
    Insert data into master_cell_total table.
    
    Parameters:
        df_master_cell (pd.DataFrame): DataFrame containing master cell data
    """
    import psycopg2
    import numpy as np

    df_master_cell.replace({"": None, "NaN": None, np.nan: None, "-": None}, inplace=True)

    # Define all numeric columns you expect to insert as INTEGER or FLOAT
    numeric_columns = [
        "ul_arfcn", "dl_arfcn", "earfcn_dl", "earfcn_ul",
        "cell_id", "local_cell_id", "psc_pci", "rnc_id",
        "lac", "tac", "rac", "latitude", "longitude", "azimuth", "beam"
    ]

    # Safely convert to numeric; force invalid entries to NaN (which becomes None)
    for col in numeric_columns:
        if col in df_master_cell.columns:
            df_master_cell[col] = pd.to_numeric(df_master_cell[col], errors="coerce").fillna(0)

    columns = [
        "region", "province", "municipality", "att_name", "att_tech", "node_id", "cell_name",
        "physical_sector", "rnc_name", "rnc_id", "cell_id", "vendor", "dl_arfcn", "ul_arfcn",
        "earfcn_dl", "earfcn_ul", "lac", "rac", "tac", "freq_band", "band_indicator", "band_width",
        "latitude", "longitude", "azimuth", "beam", "period"
    ]

    for col in columns:
        if col not in df_master_cell.columns:
            df_master_cell[col] = None

    try:
        with psycopg2.connect(
            user=POSTGRES_USERNAME,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB
        ) as conn, conn.cursor() as cursor:
            for row in df_master_cell.itertuples(index=False):
                values = [getattr(row, col) for col in columns]
                cursor.execute(
                    f"""
                    INSERT INTO master_cell_total ({', '.join(columns)})
                    VALUES ({', '.join(['%s'] * len(columns))})
                    ON CONFLICT (cell_name) DO UPDATE SET
                        region = EXCLUDED.region,
                        province = EXCLUDED.province,
                        municipality = EXCLUDED.municipality,
                        att_name = EXCLUDED.att_name,
                        att_tech = EXCLUDED.att_tech,
                        node_id = EXCLUDED.node_id,
                        physical_sector = EXCLUDED.physical_sector,
                        rnc_name = EXCLUDED.rnc_name,
                        rnc_id = EXCLUDED.rnc_id,
                        cell_id = EXCLUDED.cell_id,
                        vendor = EXCLUDED.vendor,
                        dl_arfcn = EXCLUDED.dl_arfcn,
                        ul_arfcn = EXCLUDED.ul_arfcn,
                        earfcn_dl = EXCLUDED.earfcn_dl,
                        earfcn_ul = EXCLUDED.earfcn_ul,
                        lac = EXCLUDED.lac,
                        rac = EXCLUDED.rac,
                        tac = EXCLUDED.tac,
                        freq_band = EXCLUDED.freq_band,
                        band_indicator = EXCLUDED.band_indicator,
                        band_width = EXCLUDED.band_width,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        azimuth = EXCLUDED.azimuth,
                        beam = EXCLUDED.beam,
                        period = EXCLUDED.period;
                    """,
                    values
                )
            conn.commit()
            print("Data inserted successfully into master_cell_total.")
    except Exception as e:
        print(f"An error occurred during insertion to master_cell_total: {e}")


def insert_master_node_total(df_master_node):
    """
    Insert data into master_node_total table.
    
    Parameters:
        df_master_node (pd.DataFrame): DataFrame containing master node data
    """
    import psycopg2
    import numpy as np

    df_master_node.replace({"": None, "NaN": None, np.nan: None, "-": None}, inplace=True)

    # Ensure numeric columns are converted properly
    numeric_columns = ["latitude", "longitude"]  # adapt for master_node
    for col in numeric_columns:
        if col in df_master_node.columns:
            df_master_node[col] = pd.to_numeric(df_master_node[col], errors="coerce").fillna(0)

    columns = [
        "node", "region", "province", "municipality",
        "latitude", "longitude", "att_name", "vendor", "period"
    ]

    for col in columns:
        if col not in df_master_node.columns:
            df_master_node[col] = None

    try:
        with psycopg2.connect(
            user=POSTGRES_USERNAME,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB
        ) as conn, conn.cursor() as cursor:
            for row in df_master_node.itertuples(index=False):
                values = [getattr(row, col) for col in columns]
                cursor.execute(
                    f"""
                    INSERT INTO master_node_total ({', '.join(columns)})
                    VALUES ({', '.join(['%s'] * len(columns))})
                    ON CONFLICT (node) DO UPDATE SET
                        region = EXCLUDED.region,
                        province = EXCLUDED.province,
                        municipality = EXCLUDED.municipality,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        att_name = EXCLUDED.att_name,
                        vendor = EXCLUDED.vendor,
                        period = EXCLUDED.period;
                    """,
                    values
                )
            conn.commit()
            print("Data inserted successfully into master_node_total.")
    except Exception as e:
        print(f"An error occurred during insertion to master_node_total: {e}")


# -------------------------
# MAIN CASCADE CONTROLLER
# -------------------------
def process_master_cell():
    """
    Process master cell and node data for the regular master tables.
    This function uses only the 'last' directory data.
    """
    print("\nStep 1: create_master_cell")
    t1 = time.time()
    df_master_cell, df_master_node = create_master_cell("master_cells/last")
    df_master_cell.to_csv(os.path.join(ROOT_DIRECTORY, "output", "master_cell.csv"), index=False)
    df_master_node.to_csv(os.path.join(ROOT_DIRECTORY, "output", "master_node.csv"), index=False)
    elapsed1 = time.time() - t1
    m1, s1 = divmod(elapsed1, 60)
    print(f"Completed in {int(m1)}m:{int(s1)}s")

    print("\nStep 2: insert_master_cell")
    t2 = time.time()
    insert_master_cell(df_master_cell)
    elapsed2 = time.time() - t2
    m2, s2 = divmod(elapsed2, 60)
    print(f"Completed in {int(m2)}m:{int(s2)}s")

    print("\nStep 3: insert_master_node")
    t3 = time.time()
    insert_master_node(df_master_node)
    elapsed3 = time.time() - t3
    m3, s3 = divmod(elapsed3, 60)
    print(f"Completed in {int(m3)}m:{int(s3)}s")

    print("\nStep 4: insert_master_cell_total")
    t4 = time.time()
    insert_master_cell_total(df_master_cell)
    elapsed4 = time.time() - t4
    m4, s4 = divmod(elapsed4, 60)
    print(f"Completed in {int(m4)}m:{int(s4)}s")

    print("\nStep 5: insert_master_node_total")
    t5 = time.time()
    insert_master_node_total(df_master_node)
    elapsed5 = time.time() - t5
    m5, s5 = divmod(elapsed5, 60)
    print(f"Completed in {int(m5)}m:{int(s5)}s")

    print("\n===== MASTER CELL Processing Completed =====")
    return


def create_master_cell_total():
    """
    Create master_cell_total and master_node_total by merging data from both 'initial' and 'last' directories.
    The 'last' data prevails over 'initial' data for duplicates.
    Adds a 'period' column to indicate data source ('initial' or 'last').
    
    Returns:
        df_master_cell_total, df_master_node_total
    """
    print("Creating master cell data from 'initial' directory...")
    df_master_cell_initial, df_master_node_initial = create_master_cell("master_cells/initial")
    df_master_cell_initial['period'] = 'initial'
    df_master_node_initial['period'] = 'initial'
    
    print("Creating master cell data from 'last' directory...")
    df_master_cell_last, df_master_node_last = create_master_cell("master_cells/last")
    df_master_cell_last['period'] = 'last'
    df_master_node_last['period'] = 'last'
    
    # Merge cell data - 'last' prevails over 'initial'
    print("Merging cell data - 'last' data prevails over 'initial'...")
    df_master_cell_combined = pd.concat([df_master_cell_initial, df_master_cell_last], ignore_index=True)
    
    # Remove duplicates, keeping 'last' period data when duplicates exist
    df_master_cell_total = df_master_cell_combined.sort_values(['cell_name', 'period']).drop_duplicates(
        subset=['cell_name'], keep='last'
    ).reset_index(drop=True)
    
    # Merge node data - 'last' prevails over 'initial'
    print("Merging node data - 'last' data prevails over 'initial'...")
    df_master_node_combined = pd.concat([df_master_node_initial, df_master_node_last], ignore_index=True)
    
    # Remove duplicates, keeping 'last' period data when duplicates exist
    df_master_node_total = df_master_node_combined.sort_values(['node', 'period']).drop_duplicates(
        subset=['node'], keep='last'
    ).reset_index(drop=True)
    
    print(f"Master cell total records: {len(df_master_cell_total)} (initial: {len(df_master_cell_initial)}, last: {len(df_master_cell_last)})")
    print(f"Master node total records: {len(df_master_node_total)} (initial: {len(df_master_node_initial)}, last: {len(df_master_node_last)})")
    
    return df_master_cell_total, df_master_node_total


def process_master_cell_total():
    """
    Process master cell and node data specifically for the _total tables.
    This function merges data from 'initial' and 'last' directories and inserts into total tables.
    'Last' data prevails over 'initial' data for duplicates.
    """
    print("\n===== MASTER CELL TOTAL Processing Started =====")
    
    print("\nStep 1: create_master_cell_total (merging initial + last)")
    t1 = time.time()
    df_master_cell_total, df_master_node_total = create_master_cell_total()
    df_master_cell_total.to_csv(os.path.join(ROOT_DIRECTORY, "output", "master_cell_total.csv"), index=False)
    df_master_node_total.to_csv(os.path.join(ROOT_DIRECTORY, "output", "master_node_total.csv"), index=False)
    elapsed1 = time.time() - t1
    m1, s1 = divmod(elapsed1, 60)
    print(f"Completed in {int(m1)}m:{int(s1)}s")

    print("\nStep 2: insert_master_cell_total")
    t2 = time.time()
    insert_master_cell_total(df_master_cell_total)
    elapsed2 = time.time() - t2
    m2, s2 = divmod(elapsed2, 60)
    print(f"Completed in {int(m2)}m:{int(s2)}s")

    print("\nStep 3: insert_master_node_total")
    t3 = time.time()
    insert_master_node_total(df_master_node_total)
    elapsed3 = time.time() - t3
    m3, s3 = divmod(elapsed3, 60)
    print(f"Completed in {int(m3)}m:{int(s3)}s")

    print("\n===== MASTER CELL TOTAL Processing Completed =====")
    return
