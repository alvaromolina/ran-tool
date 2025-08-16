import pandas as pd
import os
import dotenv

# Load environment variables
dotenv.load_dotenv()
ROOT_DIRECTORY = os.getenv('ROOT_DIRECTORY')

def save_to_csv(df, filename=None):
    """
    Save DataFrame to CSV file in the output directory.
    
    Args:
        df (pd.DataFrame): DataFrame to save
        filename (str): Optional filename. If not provided, uses timestamp.
    
    Returns:
        str: Path to saved file, or None if save failed
    """
    if df is None or df.empty:
        print("No data to save.")
        return None

    if filename is None:
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"cell_change_grouped_data_{timestamp}.csv"

    # Ensure output directory exists
    output_dir = os.path.join(ROOT_DIRECTORY, "output") if ROOT_DIRECTORY else "output"
    os.makedirs(output_dir, exist_ok=True)
    
    filepath = os.path.join(output_dir, filename)
    
    try:
        df.to_csv(filepath, index=False)
        print(f"Data saved to: {filepath}")
        return filepath
    except Exception as e:
        print(f"Error saving to CSV: {e}")
        return None

def save_to_excel(df, filename=None, sheet_name='CellChangeData'):
    """
    Save DataFrame to Excel file in the output directory.
    
    Args:
        df (pd.DataFrame): DataFrame to save
        filename (str): Optional filename. If not provided, uses timestamp.
        sheet_name (str): Name of the Excel sheet
    
    Returns:
        str: Path to saved file, or None if save failed
    """
    if df is None or df.empty:
        print("No data to save.")
        return None

    if filename is None:
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"cell_change_grouped_data_{timestamp}.xlsx"

    # Ensure output directory exists
    output_dir = os.path.join(ROOT_DIRECTORY, "output") if ROOT_DIRECTORY else "output"
    os.makedirs(output_dir, exist_ok=True)
    
    filepath = os.path.join(output_dir, filename)
    
    try:
        df.to_excel(filepath, sheet_name=sheet_name, index=False)
        print(f"Data saved to: {filepath}")
        return filepath
    except Exception as e:
        print(f"Error saving to Excel: {e}")
        return None

def save_summary_report(df, group_by, filename=None):
    """
    Generate and save a summary report of the cell change data.
    
    Args:
        df (pd.DataFrame): DataFrame with cell change data
        group_by (str): Grouping level used
        filename (str): Optional filename for the report
    
    Returns:
        str: Path to saved report file, or None if save failed
    """
    if df is None or df.empty:
        print("No data to generate summary report.")
        return None
    
    if filename is None:
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"cell_change_summary_{group_by}_{timestamp}.txt"
    
    # Ensure output directory exists
    output_dir = os.path.join(ROOT_DIRECTORY, "output") if ROOT_DIRECTORY else "output"
    os.makedirs(output_dir, exist_ok=True)
    
    filepath = os.path.join(output_dir, filename)
    
    try:
        with open(filepath, 'w') as f:
            f.write(f"Cell Change Evolution Summary Report\n")
            f.write(f"Generated: {pd.Timestamp.now()}\n")
            f.write(f"Grouping Level: {group_by}\n")
            f.write("=" * 50 + "\n\n")
            
            # Basic statistics
            f.write(f"Data Overview:\n")
            f.write(f"- Total records: {len(df)}\n")
            f.write(f"- Date range: {df['date'].min()} to {df['date'].max()}\n")
            f.write(f"- Data shape: {df.shape}\n\n")
            
            # Technology summary
            if 'total_cell_lte' in df.columns and 'total_cell_umts' in df.columns:
                latest_date = df['date'].max()
                latest_data = df[df['date'] == latest_date]
                
                total_lte = latest_data['total_cell_lte'].sum()
                total_umts = latest_data['total_cell_umts'].sum()
                
                f.write(f"Latest Cell Counts (as of {latest_date}):\n")
                f.write(f"- Total LTE/4G cells: {total_lte:,}\n")
                f.write(f"- Total UMTS/3G cells: {total_umts:,}\n")
                f.write(f"- Total cells: {total_lte + total_umts:,}\n\n")
            
            # Column information
            f.write(f"Available Columns:\n")
            for col in df.columns:
                f.write(f"- {col}\n")
        
        print(f"Summary report saved to: {filepath}")
        return filepath
        
    except Exception as e:
        print(f"Error saving summary report: {e}")
        return None