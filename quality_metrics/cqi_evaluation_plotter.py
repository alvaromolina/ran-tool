import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import plotly.io as pio
from datetime import datetime, timedelta
import os
import sys
from io import StringIO
from contextlib import contextmanager
from create_db_quality_metrics import get_last_date

# Configure plotly to be less verbose
pio.renderers.default = "browser"

def get_database_last_date():
    """
    Get the most recent date from all CQI daily tables in the database
    Returns: datetime object of the latest available date
    """
    tables = ['umts_cqi_daily', 'lte_cqi_daily', 'nr_cqi_daily']
    latest_date = None
    
    for table in tables:
        try:
            table_last_date = get_last_date(table)
            if table_last_date:
                if isinstance(table_last_date, str):
                    table_last_date = datetime.strptime(table_last_date, '%Y-%m-%d')
                elif not isinstance(table_last_date, datetime):
                    table_last_date = pd.to_datetime(table_last_date)
                
                if latest_date is None or table_last_date > latest_date:
                    latest_date = table_last_date
        except Exception as e:
            print(f"Warning: Could not get last date from {table}: {e}")
    
    if latest_date is None:
        print("Warning: Could not determine database last date, using current date")
        latest_date = datetime.now()
    
    return latest_date

@contextmanager
def suppress_stdout():
    """Context manager to suppress print statements from imported functions"""
    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout

# Import functions from existing modules
from umts_cqi_site_group_processor import get_umts_cqi_daily_for_site_group, get_umts_cqi_for_single_site
from lte_cqi_site_group_processor import get_lte_cqi_daily_for_site_group, get_lte_cqi_for_single_site
from nr_cqi_site_group_processor import get_nr_cqi_daily_for_site_group, get_nr_cqi_for_single_site
from master_node_neighbor_processor import get_master_node_neighbor


def get_cqi_data(site_att, input_date=None, days_before=30, radius_km=5, guard=7, period=7):
    """
    Step 1: Get all CQI data for single site and neighbor sites
    If input_date is None, uses the last available date from database
    Returns: (single_site_data, neighbor_data, neighbor_sites_list)
    """
    # Get database last date
    db_last_date = get_database_last_date()
    
    # Determine reference date for guard/period analysis and end date for data fetching
    if input_date is None:
        reference_date = db_last_date
        end_date = db_last_date
        print(f"Using database last date as reference: {reference_date.strftime('%Y-%m-%d')}")
    else:
        # Convert input_date to datetime if it's a string
        if isinstance(input_date, str):
            reference_date = datetime.strptime(input_date, '%Y-%m-%d')
        else:
            reference_date = input_date
        
        # Always fetch data up to the database last date, but use input_date as reference
        end_date = db_last_date
        print(f"Using {reference_date.strftime('%Y-%m-%d')} as reference date")
        print(f"Fetching data up to database last date: {end_date.strftime('%Y-%m-%d')}")
    
    # Calculate start date based on input_date, not database last date
    start_date = reference_date - timedelta(days=days_before)
    date_range = (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
    
    print(f"Getting data for site {site_att} from {date_range[0]} to {date_range[1]}")
    
    # Get neighbor sites
    try:
        with suppress_stdout():
            neighbors_df = get_master_node_neighbor(site_att, radius_km)
        if not neighbors_df.empty and 'att_name' in neighbors_df.columns:
            neighbor_sites = neighbors_df['att_name'].tolist()
            print(f"Found {len(neighbor_sites)} neighbors within {radius_km}km")
        else:
            neighbor_sites = []
    except Exception as e:
        print(f"Error getting neighbors: {e}")
        neighbor_sites = []
    
    # Get single site data for all technologies
    single_site_data = {}
    
    # UMTS single site
    try:
        with suppress_stdout():
            umts_df = get_umts_cqi_for_single_site(site_att, date_range, csv_export=False)
        if not umts_df.empty and 'date' in umts_df.columns and 'umts_cqi' in umts_df.columns:
            umts_df['date'] = pd.to_datetime(umts_df['date'])
            single_site_data['UMTS'] = umts_df[['date', 'umts_cqi']].rename(columns={'umts_cqi': 'cqi'})
        else:
            single_site_data['UMTS'] = pd.DataFrame()
    except Exception as e:
        print(f"Error getting UMTS single site data: {e}")
        single_site_data['UMTS'] = pd.DataFrame()
    
    # LTE single site
    try:
        with suppress_stdout():
            lte_df = get_lte_cqi_for_single_site(site_att, date_range, csv_export=False)
        if not lte_df.empty and 'date' in lte_df.columns and 'lte_cqi' in lte_df.columns:
            lte_df['date'] = pd.to_datetime(lte_df['date'])
            single_site_data['LTE'] = lte_df[['date', 'lte_cqi']].rename(columns={'lte_cqi': 'cqi'})
        else:
            single_site_data['LTE'] = pd.DataFrame()
    except Exception as e:
        print(f"Error getting LTE single site data: {e}")
        single_site_data['LTE'] = pd.DataFrame()
    
    # NR single site
    try:
        with suppress_stdout():
            nr_df = get_nr_cqi_for_single_site(site_att, date_range, csv_export=False)
        if not nr_df.empty and 'date' in nr_df.columns and 'nr_cqi' in nr_df.columns:
            nr_df['date'] = pd.to_datetime(nr_df['date'])
            single_site_data['NR'] = nr_df[['date', 'nr_cqi']].rename(columns={'nr_cqi': 'cqi'})
        else:
            single_site_data['NR'] = pd.DataFrame()
    except Exception as e:
        print(f"Error getting NR single site data: {e}")
        single_site_data['NR'] = pd.DataFrame()
    
    # Get neighbor sites data for all technologies
    neighbor_data = {}
    
    if neighbor_sites:
        # UMTS neighbors
        try:
            with suppress_stdout():
                umts_neighbors_df = get_umts_cqi_daily_for_site_group(neighbor_sites, date_range, csv_export=False)
            if not umts_neighbors_df.empty and 'date' in umts_neighbors_df.columns and 'umts_cqi' in umts_neighbors_df.columns:
                umts_neighbors_df['date'] = pd.to_datetime(umts_neighbors_df['date'])
                neighbor_data['UMTS'] = umts_neighbors_df[['date', 'umts_cqi']].rename(columns={'umts_cqi': 'cqi'})
            else:
                neighbor_data['UMTS'] = pd.DataFrame()
        except Exception as e:
            print(f"Error getting UMTS neighbor data: {e}")
            neighbor_data['UMTS'] = pd.DataFrame()
        
        # LTE neighbors
        try:
            with suppress_stdout():
                lte_neighbors_df = get_lte_cqi_daily_for_site_group(neighbor_sites, date_range, csv_export=False)
            if not lte_neighbors_df.empty and 'date' in lte_neighbors_df.columns and 'lte_cqi' in lte_neighbors_df.columns:
                lte_neighbors_df['date'] = pd.to_datetime(lte_neighbors_df['date'])
                neighbor_data['LTE'] = lte_neighbors_df[['date', 'lte_cqi']].rename(columns={'lte_cqi': 'cqi'})
            else:
                neighbor_data['LTE'] = pd.DataFrame()
        except Exception as e:
            print(f"Error getting LTE neighbor data: {e}")
            neighbor_data['LTE'] = pd.DataFrame()
        
        # NR neighbors
        try:
            with suppress_stdout():
                nr_neighbors_df = get_nr_cqi_daily_for_site_group(neighbor_sites, date_range, csv_export=False)
            if not nr_neighbors_df.empty and 'date' in nr_neighbors_df.columns and 'nr_cqi' in nr_neighbors_df.columns:
                nr_neighbors_df['date'] = pd.to_datetime(nr_neighbors_df['date'])
                neighbor_data['NR'] = nr_neighbors_df[['date', 'nr_cqi']].rename(columns={'nr_cqi': 'cqi'})
            else:
                neighbor_data['NR'] = pd.DataFrame()
        except Exception as e:
            print(f"Error getting NR neighbor data: {e}")
            neighbor_data['NR'] = pd.DataFrame()
    
    print(f"Data retrieval complete. Single site records: UMTS={len(single_site_data.get('UMTS', []))}, LTE={len(single_site_data.get('LTE', []))}, NR={len(single_site_data.get('NR', []))}")
    if neighbor_sites:
        print(f"Neighbor data records: UMTS={len(neighbor_data.get('UMTS', []))}, LTE={len(neighbor_data.get('LTE', []))}, NR={len(neighbor_data.get('NR', []))}")
    
    return single_site_data, neighbor_data, neighbor_sites, reference_date, db_last_date


def create_single_site_plot(single_site_data, site_name, input_date, db_last_date, guard=7, period=7):
    """
    Step 2a: Create plot for single site CQI data
    Input: single_site_data dict with UMTS/LTE/NR dataframes
    Returns: plotly figure
    """
    fig = go.Figure()
    
    colors = {'UMTS': '#d62728', 'LTE': '#2ca02c', 'NR': '#1f77b4'}  # Red, Green, Blue
    
    for tech, df in single_site_data.items():
        if not df.empty:
            fig.add_trace(go.Scatter(
                x=df['date'],
                y=df['cqi'],
                mode='lines+markers',
                name=f'{tech} CQI',
                line=dict(color=colors[tech], width=3),
                marker=dict(size=6, color=colors[tech])
            ))
    
    # Add horizontal reference lines
    fig.add_hline(y=73, line_dash="dash", line_color="orange", opacity=0.7, annotation_text="73%")
    fig.add_hline(y=85, line_dash="dash", line_color="gray", opacity=0.7, annotation_text="85%")
    
    # Add vertical lines for guard and period intervals
    if isinstance(input_date, str):
        ref_date = datetime.strptime(input_date, '%Y-%m-%d')
    else:
        ref_date = input_date
    
    # Add vertical lines using shapes
    # Main reference line (input_date) - black
    fig.add_shape(
        type="line",
        x0=ref_date, x1=ref_date,
        y0=0, y1=100,
        line=dict(color="black", width=2),
        name="Reference Date"
    )
    
    # Guard and period lines (thin)
    fig.add_shape(
        type="line",
        x0=ref_date - timedelta(days=period + guard), 
        x1=ref_date - timedelta(days=period + guard),
        y0=0, y1=100,
        line=dict(color="gray", width=1, dash="dot"),
        opacity=0.6
    )
    fig.add_shape(
        type="line",
        x0=ref_date - timedelta(days=guard), 
        x1=ref_date - timedelta(days=guard),
        y0=0, y1=100,
        line=dict(color="gray", width=1, dash="dot"),
        opacity=0.6
    )
    fig.add_shape(
        type="line",
        x0=ref_date + timedelta(days=guard), 
        x1=ref_date + timedelta(days=guard),
        y0=0, y1=100,
        line=dict(color="gray", width=1, dash="dot"),
        opacity=0.6
    )
    fig.add_shape(
        type="line",
        x0=ref_date + timedelta(days=guard + period), 
        x1=ref_date + timedelta(days=guard + period),
        y0=0, y1=100,
        line=dict(color="gray", width=1, dash="dot"),
        opacity=0.6
    )
    
    # Additional vertical lines for database last date analysis
    fig.add_shape(
        type="line",
        x0=db_last_date - timedelta(days=period), 
        x1=db_last_date - timedelta(days=period),
        y0=0, y1=100,
        line=dict(color="gray", width=1, dash="dot"),
        opacity=0.6
    )
    fig.add_shape(
        type="line",
        x0=db_last_date, 
        x1=db_last_date,
        y0=0, y1=100,
        line=dict(color="gray", width=1, dash="dot"),
        opacity=0.6
    )
    
    fig.update_layout(
        title=dict(
            text=f'Single Site CQI - {site_name}',
            font=dict(size=16, color='black'),
            x=0.5
        ),
        xaxis_title='',
        yaxis_title='CQI Score',
        hovermode='x unified',
        plot_bgcolor='white',
        paper_bgcolor='white',
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5
        ),
        height=500,
        width=1000,
        margin=dict(l=60, r=60, t=80, b=100)
    )
    
    # Format dates with MM/DD/YYYY and auto-adaptive spacing
    fig.update_xaxes(
        showgrid=True, 
        gridwidth=1, 
        gridcolor='lightgray',
        tickangle=45,  # 45-degree angle for better readability
        tickformat='%m/%d/%Y',  # MM/DD/YYYY format
        tickmode='auto',  # Let Plotly auto-calculate optimal spacing
        nticks=8,  # Limit to ~8 ticks to prevent overcrowding
        showline=True,
        linewidth=1,
        linecolor='lightgray'
    )
    fig.update_yaxes(
        showgrid=True, 
        gridwidth=1, 
        gridcolor='lightgray',
        showline=True,
        linewidth=1,
        linecolor='lightgray',
        range=[0, 100]
    )
    
    return fig


def create_neighbor_plot(neighbor_data, neighbor_count, input_date, db_last_date, guard=7, period=7):
    """
    Step 2b: Create plot for neighbor sites CQI data
    Input: neighbor_data dict with UMTS/LTE/NR dataframes
    Returns: plotly figure
    """
    fig = go.Figure()
    
    colors = {'UMTS': '#d62728', 'LTE': '#2ca02c', 'NR': '#1f77b4'}  # Red, Green, Blue
    
    for tech, df in neighbor_data.items():
        if not df.empty:
            fig.add_trace(go.Scatter(
                x=df['date'],
                y=df['cqi'],
                mode='lines+markers',
                name=f'{tech} CQI (Avg)',
                line=dict(color=colors[tech], width=3),
                marker=dict(size=6, color=colors[tech])
            ))
    
    # Add horizontal reference lines
    fig.add_hline(y=73, line_dash="dash", line_color="orange", opacity=0.7, annotation_text="73%")
    fig.add_hline(y=85, line_dash="dash", line_color="gray", opacity=0.7, annotation_text="85%")
    
    # Add vertical lines for guard and period intervals
    if isinstance(input_date, str):
        ref_date = datetime.strptime(input_date, '%Y-%m-%d')
    else:
        ref_date = input_date
    
    # Convert dates to string format for plotly
    ref_date_str = ref_date.strftime('%Y-%m-%d')
    
    # Main reference line (input_date) - black
    fig.add_shape(type="line", x0=ref_date, x1=ref_date, y0=0, y1=1, yref="paper",
                  line=dict(color="black", width=2))
    
    # Guard and period lines (thin)
    fig.add_shape(type="line", x0=(ref_date - timedelta(days=period + guard)), 
                  x1=(ref_date - timedelta(days=period + guard)), y0=0, y1=1, yref="paper",
                  line=dict(color="gray", width=1, dash="dot"))
    fig.add_shape(type="line", x0=(ref_date - timedelta(days=guard)), 
                  x1=(ref_date - timedelta(days=guard)), y0=0, y1=1, yref="paper",
                  line=dict(color="gray", width=1, dash="dot"))
    fig.add_shape(type="line", x0=(ref_date + timedelta(days=guard)), 
                  x1=(ref_date + timedelta(days=guard)), y0=0, y1=1, yref="paper",
                  line=dict(color="gray", width=1, dash="dot"))
    fig.add_shape(type="line", x0=(ref_date + timedelta(days=guard + period)), 
                  x1=(ref_date + timedelta(days=guard + period)), y0=0, y1=1, yref="paper",
                  line=dict(color="gray", width=1, dash="dot"))
    
    # Additional vertical lines for database last date analysis
    fig.add_shape(type="line", x0=(db_last_date - timedelta(days=period)), 
                  x1=(db_last_date - timedelta(days=period)), y0=0, y1=1, yref="paper",
                  line=dict(color="gray", width=1, dash="dot"))
    fig.add_shape(type="line", x0=db_last_date, 
                  x1=db_last_date, y0=0, y1=1, yref="paper",
                  line=dict(color="gray", width=1, dash="dot"))
    
    fig.update_layout(
        title=dict(
            text=f'Neighbor Sites CQI (Combined of {neighbor_count} sites)',
            font=dict(size=16, color='black'),
            x=0.5
        ),
        xaxis_title='',
        yaxis_title='CQI Score',
        hovermode='x unified',
        plot_bgcolor='white',
        paper_bgcolor='white',
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5
        ),
        height=500,
        width=1000,
        margin=dict(l=60, r=60, t=80, b=100)
    )
    
    # Format dates with MM/DD/YYYY and auto-adaptive spacing
    fig.update_xaxes(
        showgrid=True, 
        gridwidth=1, 
        gridcolor='lightgray',
        tickangle=45,  # 45-degree angle for better readability
        tickformat='%m/%d/%Y',  # MM/DD/YYYY format
        tickmode='auto',  # Let Plotly auto-calculate optimal spacing
        nticks=8,  # Limit to ~8 ticks to prevent overcrowding
        showline=True,
        linewidth=1,
        linecolor='lightgray'
    )
    fig.update_yaxes(
        showgrid=True, 
        gridwidth=1, 
        gridcolor='lightgray',
        showline=True,
        linewidth=1,
        linecolor='lightgray',
        range=[0, 100]
    )
    
    return fig


def display_plots(single_site_data, neighbor_data, site_name, neighbor_count, input_date, db_last_date, guard=7, period=7):
    """
    Step 3: Create and display two separate plots
    """
    # Create single site plot
    single_site_fig = create_single_site_plot(single_site_data, site_name, input_date, db_last_date, guard, period)
    
    # Display single site plot
    try:
        pio.show(single_site_fig, config={'displayModeBar': True}, auto_open=False, validate=False)
        print("Single site plot displayed successfully!")
    except Exception as e:
        print("Saving single site plot to HTML file...")
        single_site_fig.write_html(f"single_site_cqi_{site_name}.html", config={'displayModeBar': True})
        print(f"Single site plot saved as 'single_site_cqi_{site_name}.html'")
    
    # Create and display neighbor plot if data exists
    if neighbor_data and any(not df.empty for df in neighbor_data.values()):
        neighbor_fig = create_neighbor_plot(neighbor_data, neighbor_count, input_date, db_last_date, guard, period)
        
        try:
            pio.show(neighbor_fig, config={'displayModeBar': True}, auto_open=False, validate=False)
            print("Neighbor plot displayed successfully!")
        except Exception as e:
            print("Saving neighbor plot to HTML file...")
            neighbor_fig.write_html(f"neighbor_cqi_{site_name}.html", config={'displayModeBar': True})
            print(f"Neighbor plot saved as 'neighbor_cqi_{site_name}.html'")
    else:
        print("No neighbor data available for plotting")


# Main execution function
def main(site_att="AGUAGU0004", input_date="2024-09-21", days_before=30, radius_km=5, guard=7, period=7):
    """
    Main function that orchestrates the entire process
    If input_date is None, uses the last available date from database
    """
    print(f"Starting CQI analysis for site {site_att}...")
    
    # Step 1: Get all data
    single_site_data, neighbor_data, neighbor_sites, reference_date, db_last_date = get_cqi_data(site_att, input_date, days_before, radius_km, guard, period)
    
    # Step 2: Create and display separate plots using the reference date for guard/period lines
    display_plots(single_site_data, neighbor_data, site_att, len(neighbor_sites), reference_date, db_last_date, guard, period)
    
    return single_site_data, neighbor_data


# Example usage and testing
if __name__ == "__main__":
    main()
