import pandas as pd
import os
import dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# Load environment variables
dotenv.load_dotenv()
POSTGRES_USERNAME = os.getenv('POSTGRES_USERNAME')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_DB = os.getenv('POSTGRES_DB')

def get_engine():
    """Create database engine connection"""
    connection_string = f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    return create_engine(connection_string)

def get_cell_change_events(site_att):
    """
    Get cell change events from both UMTS and LTE tables for a given site.
    
    Args:
        site_att (str): Target site name
        
    Returns:
        pandas.DataFrame: Combined cell change events from both technologies
    """
    if not site_att or site_att.strip() == "":
        print("Error: site_att cannot be empty")
        return pd.DataFrame()
    
    engine = get_engine()
    
    try:
        # Query for UMTS cell change events
        umts_query = """
        SELECT 
            region,
            province,
            municipality,
            att_name,
            date,
            add_cell,
            delete_cell,
            total_cell,
            remark,
            'umts' as tech
        FROM umts_cell_change_event
        WHERE att_name = %(site_att)s
        ORDER BY date
        """
        
        # Query for LTE cell change events
        lte_query = """
        SELECT 
            region,
            province,
            municipality,
            att_name,
            date,
            add_cell,
            delete_cell,
            total_cell,
            remark,
            'lte' as tech
        FROM lte_cell_change_event
        WHERE att_name = %(site_att)s
        ORDER BY date
        """
        
        # Execute queries
        umts_df = pd.read_sql(umts_query, engine, params={"site_att": site_att})
        lte_df = pd.read_sql(lte_query, engine, params={"site_att": site_att})
        
        # Combine the results
        if not umts_df.empty and not lte_df.empty:
            combined_df = pd.concat([umts_df, lte_df], ignore_index=True)
        elif not umts_df.empty:
            combined_df = umts_df
        elif not lte_df.empty:
            combined_df = lte_df
        else:
            combined_df = pd.DataFrame()
            return combined_df
        
        # Sort by date and technology
        combined_df = combined_df.sort_values(['date', 'tech']).reset_index(drop=True)
        
        # Remove the last row for each technology - preserve all columns including 'tech'
        # Use a different approach to avoid the deprecation warning
        filtered_rows = []
        for tech in combined_df['tech'].unique():
            tech_data = combined_df[combined_df['tech'] == tech]
            if len(tech_data) > 1:  # Only remove last row if there's more than one row
                filtered_rows.append(tech_data.iloc[:-1])
            # If only one row, we skip it (remove it entirely)
        
        if filtered_rows:
            filtered_df = pd.concat(filtered_rows, ignore_index=True)
        else:
            filtered_df = pd.DataFrame()
        
        return filtered_df
        
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error in get_cell_change_events: {e}")
        return pd.DataFrame()

def export_cell_change_events(site_att, output_format='csv'):
    """
    Export cell change events to CSV or display as table.
    
    Args:
        site_att (str): Target site name
        output_format (str): 'csv', 'table', or 'dataframe'
        
    Returns:
        pandas.DataFrame: Cell change events data
    """
    # Get the data
    df = get_cell_change_events(site_att)
    
    if df.empty:
        if output_format.lower() != 'dataframe':
            print(f"No cell change events found for site: {site_att}")
        return df
    
    # Ensure columns are in the correct order including 'tech'
    # Only reorder if all expected columns exist
    expected_columns = ['region', 'province', 'municipality', 'att_name', 'tech', 'date', 'add_cell', 'delete_cell', 'total_cell', 'remark']
    available_columns = [col for col in expected_columns if col in df.columns]
    
    if len(available_columns) == len(expected_columns):
        df = df[expected_columns]
    else:
        if output_format.lower() != 'dataframe':
            print(f"Warning: Expected columns {expected_columns}")
            print(f"Available columns: {list(df.columns)}")
    
    # Sort the final table by date
    df = df.sort_values('date').reset_index(drop=True)
    
    # Export to CSV if requested
    if output_format.lower() == 'csv':
        filename = f"cell_change_events_{site_att}.csv"
        df.to_csv(filename, index=False)
        print(f"Data exported to {filename}")
    
    # Display the detailed records with proper formatting (only for 'table' format)
    if output_format.lower() == 'table':
        print(f"\nCell Change Events for {site_att}:")
        print("=" * 60)
        print(df.to_string(index=False, max_cols=None))
    
    return df


# Example usage and testing
if __name__ == "__main__":
    # Example site for testing
    site_name = "MEXMET0396"  # Replace with actual site name
    
    # Get and display cell change events
    export_cell_change_events(site_name, output_format='table')
