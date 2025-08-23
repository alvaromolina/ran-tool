import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
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

# Configure plotly for browser display
pio.renderers.default = "browser"

def get_engine():
    """Create database engine connection"""
    connection_string = f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    return create_engine(connection_string)

# BLOCK 1: SQL FUNCTION
def query_sites_within_radius(site_att, radius=5):
    """
    SQL function to get all sites within radius from target site.
    
    Args:
        site_att (str): Target site name
        radius (float): Search radius in kilometers (default: 5)
        
    Returns:
        pandas.DataFrame: All columns from master_node_total for sites within radius
    """
    if not site_att or site_att.strip() == "":
        return pd.DataFrame()
    
    engine = get_engine()
    radius_meters = radius * 1000
    
    try:
        query = """
        WITH target_site AS (
            SELECT 
                latitude,
                longitude,
                ST_Transform(ST_SetSRID(ST_Point(longitude, latitude), 4326), 3857) as geom
            FROM master_node_total 
            WHERE att_name = %(site_att)s
            AND latitude IS NOT NULL 
            AND longitude IS NOT NULL
            LIMIT 1
        )
        SELECT 
            n.*,
            ROUND(
                CAST(ST_Distance(
                    ts.geom,
                    ST_Transform(ST_SetSRID(ST_Point(n.longitude, n.latitude), 4326), 3857)
                ) / 1000.0 AS numeric), 3
            ) as distance_km
        FROM target_site ts
        CROSS JOIN master_node_total n
        WHERE n.latitude IS NOT NULL 
        AND n.longitude IS NOT NULL
        AND (
            n.att_name = %(site_att)s 
            OR ST_DWithin(
                ts.geom,
                ST_Transform(ST_SetSRID(ST_Point(n.longitude, n.latitude), 4326), 3857),
                %(radius_meters)s
            )
        )
        """
        
        df = pd.read_sql(query, engine, params={
            "site_att": site_att,
            "radius_meters": radius_meters
        })
        
        if df.empty:
            print(f"No data found for site: {site_att}")
            return pd.DataFrame()
        
        # Set distance to 0 for target site
        df.loc[df['att_name'] == site_att, 'distance_km'] = 0.0
        
        print(f"Found {len(df)} records for site {site_att} within {radius}km")
        return df
        
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error in query_sites_within_radius: {e}")
        return pd.DataFrame()

# BLOCK 2: SUMMARY FUNCTION
def create_summary_table(df, csv_export=True):
    """
    Create summary table grouped by REGION, PROVINCE, MUNICIPALITY, ATT_NAME.
    
    Args:
        df (pandas.DataFrame): Input dataframe from query_sites_within_radius
        csv_export (bool): Whether to export to CSV (default: True)
        
    Returns:
        pandas.DataFrame: Grouped summary table
    """
    if df.empty:
        return pd.DataFrame()
    
    try:
        # Group by REGION, PROVINCE, MUNICIPALITY, ATT_NAME
        grouped = df.groupby(['region', 'province', 'municipality', 'att_name']).agg({
            'latitude': 'max',
            'longitude': 'max', 
            'distance_km': 'max',
            'node': lambda x: list(sorted(x.unique())),  # Create list of distinct nodes
            'vendor': lambda x: list(sorted(x.unique()))  # Create list of distinct vendors
        }).reset_index()
        
        # Convert node list to string for better display
        grouped['node'] = grouped['node'].apply(lambda x: ', '.join(x) if len(x) > 1 else x[0])
        
        # Convert vendor list to string for better display
        grouped['vendor'] = grouped['vendor'].apply(lambda x: ', '.join(x) if len(x) > 1 else x[0])
        
        # Select final columns in correct order
        summary_df = grouped[['region', 'province', 'municipality', 'att_name', 'node', 'vendor', 'latitude', 'longitude', 'distance_km']].copy()
        
        # Sort by distance
        summary_df = summary_df.sort_values('distance_km').reset_index(drop=True)
        
        # Export to CSV if requested
        if csv_export:
            filename = f"master_node_summary_{df.iloc[0]['att_name']}_radius_5km.csv"
            summary_df.to_csv(filename, index=False)
            print(f"Summary data exported to: {filename}")
        
        print(f"Created summary table with {len(summary_df)} unique sites")
        return summary_df
        
    except Exception as e:
        print(f"Error in create_summary_table: {e}")
        return pd.DataFrame()

# BLOCK 3: PLOT FUNCTION
def plot_sites_map(summary_df, site_att):
    """
    Create interactive map with site points and labels.
    
    Args:
        summary_df (pandas.DataFrame): Summary dataframe from create_summary_table
        site_att (str): Target site name to highlight in black
        
    Returns:
        plotly.graph_objects.Figure: Interactive map figure
    """
    if summary_df.empty:
        # Create empty figure with message
        fig = go.Figure()
        fig.add_annotation(
            text=f"No data found for site: {site_att}",
            x=0.5, y=0.5,
            xref="paper", yref="paper",
            showarrow=False,
            font=dict(size=16)
        )
        fig.update_layout(
            title=f"Master Node Map - {site_att} (No Data)",
            xaxis=dict(visible=False),
            yaxis=dict(visible=False)
        )
        return fig
    
    # Separate target and neighbor locations
    target_df = summary_df[summary_df['att_name'] == site_att]
    neighbor_df = summary_df[summary_df['att_name'] != site_att]
    
    # Create the map figure
    fig = go.Figure()
    
    # Add ALL sites with blue markers and blue labels (including target)
    fig.add_trace(go.Scattermapbox(
        lat=summary_df['latitude'],
        lon=summary_df['longitude'],
        mode='markers+text',
        marker=dict(
            size=8,
            color='blue',
            opacity=0.7
        ),
        text=summary_df['att_name'],  # All site names as blue labels
        textposition='bottom center',  # Position labels below points
        textfont=dict(size=9, color='blue'),
        hovertext=summary_df.apply(lambda row: 
            f"<b>{row['att_name']}</b><br>" +
            f"Node: {row['node']}<br>" +
            f"Vendor: {row['vendor']}<br>" +
            f"Region: {row['region']}<br>" +
            f"Province: {row['province']}<br>" +
            f"Municipality: {row['municipality']}<br>" +
            f"Distance: {row['distance_km']} km<br>" +
            f"Coordinates: ({row['latitude']:.6f}, {row['longitude']:.6f})"
        , axis=1),
        hovertemplate='%{hovertext}<extra></extra>',
        name=f'All Sites ({len(summary_df)})',
        showlegend=True
    ))
    
    # Add target site as BLACK MARKER ONLY (no text) on top
    if not target_df.empty:
        target_site = target_df.iloc[0]  # Get the target site data
        fig.add_trace(go.Scattermapbox(
            lat=[target_site['latitude']],
            lon=[target_site['longitude']],
            mode='markers',  # Only marker, no text
            marker=dict(
                size=15,
                color='black',
                symbol='circle'
            ),
            hovertext=[
                f"<b>TARGET: {site_att}</b><br>" +
                f"Node: {target_site['node']}<br>" +
                f"Vendor: {target_site['vendor']}<br>" +
                f"Region: {target_site['region']}<br>" +
                f"Province: {target_site['province']}<br>" +
                f"Municipality: {target_site['municipality']}<br>" +
                f"Coordinates: ({target_site['latitude']:.6f}, {target_site['longitude']:.6f})"
            ],
            hovertemplate='%{hovertext}<extra></extra>',
            name='Target Site',
            showlegend=True
        ))
        
        # Center map on target site
        center_lat = target_site['latitude']
        center_lon = target_site['longitude']
    else:
        # Fallback to data center if no target found
        center_lat = summary_df['latitude'].mean()
        center_lon = summary_df['longitude'].mean()
    
    # Fixed zoom level for 5km radius
    zoom = 13
    
    # Update layout with mapbox
    fig.update_layout(
        title=dict(
            text=f"Master Node Map - {site_att} (Radius: 5km)<br>" +
                 f"<sub>Target: 1 site, Total sites: {len(summary_df)}</sub>",
            x=0.5,
            font=dict(size=16)
        ),
        mapbox=dict(
            style="open-street-map",
            center=dict(lat=center_lat, lon=center_lon),
            zoom=zoom
        ),
        height=600,
        margin=dict(l=0, r=0, t=60, b=0),
        showlegend=True,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
            bgcolor="rgba(255,255,255,0.8)"
        )
    )
    
    return fig

# Example usage
if __name__ == "__main__":
    # Example site for testing
    site_name = "MEXMET0396"  # Replace with actual site name
    radius = 5  # km
    
    print(f"Master Node Analysis for {site_name}")
    print("=" * 50)
    
    # Three-block approach:
    # Block 1: Query sites within radius
    raw_data = query_sites_within_radius(site_name, radius)
    
    # Block 2: Create summary table with grouping
    summary_table = create_summary_table(raw_data, csv_export=True)
    
    # Block 3: Create interactive map
    map_figure = plot_sites_map(summary_table, site_name)
    
    # Display results
    if not summary_table.empty:
        print("\nSummary Table:")
        print(summary_table.to_string(index=False))
        
        print("\nDisplaying interactive map...")
        map_figure.show()
    else:
        print(f"No data found for site: {site_name}")
