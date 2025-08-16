import pandas as pd
import plotly.graph_objects as go
import os

def plot_cell_change_data(df, group_by='municipality', category='band_indicator', dates=None):
    """Plot cell change data with proper error handling"""
    
    # Add error handling for None DataFrame
    if df is None:
        print("Error: DataFrame is None")
        fig = go.Figure()
        fig.add_annotation(text="No data available", xref="paper", yref="paper", x=0.5, y=0.5)
        return fig
        
    if df.empty:
        print("Error: DataFrame is empty")
        fig = go.Figure()
        fig.add_annotation(text="No data found for selected filters", xref="paper", yref="paper", x=0.5, y=0.5)
        return fig
    
    # Define color palettes
    band_colors = {
        "b2_h4g": "#1f77b4", "b2_e4g": "#aec7e8", "b2_n4g": "#c6dbef", "b2_s4g": "#deebf7",
        "b26_h4g": "#ff7f0e", "b26_e4g": "#ffbb78", "b26_n4g": "#ffd8b1", "b26_s4g": "#ffe5cc",
        "b4_h4g": "#2ca02c", "b4_e4g": "#98df8a", "b4_n4g": "#c7e9c0", "b4_s4g": "#e5f5e0",
        "b5_h4g": "#e377c2", "b5_e4g": "#f7b6d2", "b5_n4g": "#fde0dd", "b5_s4g": "#fef0f0",
        "b7_h4g": "#17becf", "b7_e4g": "#9edae5", "b7_n4g": "#c7e9e8", "b7_s4g": "#e5f7f6",
        "b42_h4g": "#8c564b", "b42_e4g": "#c49c94", "b42_n4g": "#e7d4d1", "b42_s4g": "#f0e6e4",
        "x_h4g": "#bcbd22", "x_e4g": "#dbdb8d", "x_n4g": "#eded9f", "x_s4g": "#f7f7b6",
        "b2_h3g": "#1f77b4", "b2_e3g": "#aec7e8", "b2_n3g": "#c6dbef",
        "b4_h3g": "#2ca02c", "b4_e3g": "#98df8a", "b4_n3g": "#c7e9c0",
        "b5_h3g": "#e377c2", "b5_e3g": "#f7b6d2", "b5_n3g": "#fde0dd",
        "x_h3g": "#bcbd22", "x_e3g": "#dbdb8d", "x_n3g": "#eded9f"
    }
    
    band_indicator_colors = {
        "B2": "#1f77b4", "B4": "#2ca02c", "B5": "#e377c2", "B7": "#17becf",
        "B26": "#ff7f0e", "B42": "#8c564b", "X": "#bcbd22"
    }
    
    vendor_colors = {
        "H4G": "#1f77b4", "E4G": "#ff7f0e", "N4G": "#2ca02c", "S4G": "#d62728",
        "H3G": "#9467bd", "E3G": "#8c564b", "N3G": "#e377c2"
    }
    
    technology_colors = {
        "4G": "#2ca02c",  # Green for LTE/4G
        "3G": "#1f77b4"   # Blue for UMTS/3G
    }
    
    color_palettes = {
        "band": band_colors, 
        "band_indicator": band_indicator_colors, 
        "vendor": vendor_colors,
        "technology": technology_colors
    }
    
    # Convert date to string format
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    
    # Define band columns based on what's available in the DataFrame
    lte_columns = [col for col in df.columns if col.startswith(('b2_', 'b4_', 'b5_', 'b7_', 'b26_', 'b42', 'x_')) and col.endswith(('h4g', 'e4g', 'n4g', 's4g'))]
    umts_columns = [col for col in df.columns if col.startswith(('b2_', 'b4_', 'b5_', 'x_')) and col.endswith(('h3g', 'e3g', 'n3g'))]
    band_columns = lte_columns + umts_columns
    
    if not band_columns:
        print("Error: No band columns found in DataFrame")
        fig = go.Figure()
        fig.add_annotation(text="No band data available", xref="paper", yref="paper", x=0.5, y=0.5)
        return fig
    
    # Get the grouping column name
    group_column_map = {'network': 'network_level', 'region': 'region', 'province': 'province', 'municipality': 'municipality'}
    group_column = group_column_map.get(group_by, 'municipality')
    
    # If there's a grouping column, aggregate by it, otherwise use all data
    if group_column in df.columns:
        unique_groups = df[group_column].unique()
        if len(unique_groups) > 1:
            df_plot = df.groupby('date')[band_columns].sum().reset_index()
            title_suffix = f"All {group_by.title()}s"
        else:
            df_plot = df.copy()
            title_suffix = str(unique_groups[0])
    else:
        df_plot = df.copy()
        title_suffix = "National"
    
    # Melt to long format
    df_melted = df_plot[["date"] + band_columns].melt(id_vars="date", var_name="band", value_name="cells")
    
    # Extract band characteristics
    df_melted["band_indicator"] = df_melted["band"].str.extract(r"(b\d+|x)")[0].str.upper()
    df_melted["vendor"] = df_melted["band"].str.extract(r"_([hens]\d+g)$")[0].str.upper()
    df_melted["technology"] = df_melted["band"].str.extract(r"_[hens](\d+)g$")[0].apply(
        lambda x: "4G" if x == "4" else "3G" if x == "3" else "Unknown"
    )
    
    if category not in ["band", "band_indicator", "vendor", "technology"]:
        category = "band_indicator"
    
    color_map = color_palettes.get(category, {})
    
    # Group and pivot
    df_grouped = df_melted.groupby(["date", category])["cells"].sum().reset_index()
    df_pivot = df_grouped.pivot(index="date", columns=category, values="cells").fillna(0)
    
    # Apply date filtering if specified
    if dates and len(dates) == 2:
        try:
            df_pivot = df_pivot.loc[dates[0]:dates[1]]
        except KeyError:
            pass  # Silently use all available dates
    
    df_pivot = df_pivot.sort_index()
    
    if df_pivot.empty:
        print("Error: No data after processing")
        fig = go.Figure()
        fig.add_annotation(text="No data after processing", xref="paper", yref="paper", x=0.5, y=0.5)
        return fig
    
    # Create the plot
    fig = go.Figure()
    
    for col in df_pivot.columns:
        fig.add_trace(go.Bar(
            x=df_pivot.index,
            y=df_pivot[col],
            name=str(col),
            marker_color=color_map.get(str(col), "#999999")
        ))
    
    fig.update_layout(
        barmode='stack',
        title=f"Cell Change Evolution - {title_suffix} (by {category})",
        xaxis=dict(title="Date", type='category'),
        yaxis_title="Total Cells",
        hovermode="x unified",
        template="plotly_white",
        height=600
    )
    
    return fig

def save_plot_html(fig, filename, output_dir, suppress_output=True):
    """Save plotly figure as HTML file with optional output suppression"""
    if fig is None:
        print("No figure to save.")
        return None
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    plot_file = os.path.join(output_dir, filename)
    
    if suppress_output:
        import io
        import sys
        
        # Temporarily capture stdout to suppress Plotly's verbose output
        old_stdout = sys.stdout
        sys.stdout = io.StringIO()
        
        try:
            fig.write_html(plot_file, include_plotlyjs='inline')
        finally:
            sys.stdout = old_stdout
    else:
        fig.write_html(plot_file, include_plotlyjs='inline')
    
    print(f"Plot saved to: {plot_file}")
    return plot_file

def show_plot(fig, suppress_output=True):
    """Display plotly figure with optional output suppression"""
    if fig is None:
        print("No figure to show.")
        return
    
    if suppress_output:
        import io
        import sys
        
        try:
            # Suppress stdout for fig.show()
            old_stdout = sys.stdout
            sys.stdout = io.StringIO()
            fig.show()
            sys.stdout = old_stdout
            print("Plot displayed successfully.")
        except Exception as e:
            sys.stdout = old_stdout
            print(f"Could not display plot: {e}")
    else:
        try:
            fig.show()
            print("Plot displayed successfully.")
        except Exception as e:
            print(f"Could not display plot: {e}")


def plot_site_cqi_daily(df):
    """Plot CQI daily data for a site with line and circle markers"""
    
    # Add error handling for None DataFrame
    if df is None:
        fig = go.Figure()
        fig.add_annotation(text="No data available", xref="paper", yref="paper", x=0.5, y=0.5)
        return fig
        
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(text="No data found for selected site", xref="paper", yref="paper", x=0.5, y=0.5)
        return fig
    
    # Convert time to datetime first, then to string for X-axis (like other charts)
    df = df.copy()
    df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d', errors='coerce')
    df['time_str'] = df['time'].dt.strftime('%Y-%m-%d')  # Convert to string for X-axis
    
    # Define CQI columns to plot
    cqi_columns = {
        'umts_cqi': {'name': 'UMTS CQI', 'color': '#1f77b4'},
        'lte_cqi': {'name': 'LTE CQI', 'color': '#2ca02c'},
        'nr_cqi': {'name': 'NR CQI', 'color': '#ff7f0e'}
    }
    
    # Create the plot
    fig = go.Figure()
    
    # Check if any CQI data exists
    has_data = False
    
    # Add traces for each CQI type
    for col, config in cqi_columns.items():
        if col in df.columns:
            mask = df[col].notna()
            if mask.any():
                has_data = True
                
                fig.add_trace(go.Scatter(
                    x=df.loc[mask, 'time_str'],  # Use string dates instead of datetime
                    y=df.loc[mask, col],
                    mode='lines+markers',
                    name=config['name'],
                    line=dict(color=config['color'], width=2),
                    marker=dict(
                        color=config['color'],
                        size=6,
                        symbol='circle'
                    ),
                    hovertemplate='<b>%{trace.name}</b><br>' +
                                'Date: %{x}<br>' +
                                'CQI: %{y:.2f}<br>' +
                                '<extra></extra>'
                ))
    
    # Get site_att for title
    site_att = df['site_att'].iloc[0] if 'site_att' in df.columns and not df.empty else 'Unknown Site'
    
    # If no data found, show clean empty plot
    if not has_data:
        fig.add_annotation(
            text=f"No CQI data available for site: {site_att}",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16, color='gray')
        )
    
    # Calculate Y-axis range safely
    ymax = 100  # default
    try:
        available_cqi_cols = [col for col in ['umts_cqi', 'lte_cqi', 'nr_cqi'] if col in df.columns]
        if available_cqi_cols and has_data:
            ymax = df[available_cqi_cols].max().max() * 1.1
    except Exception as e:
        ymax = 100
    
    # Update layout - treat X-axis as category (text) instead of date
    fig.update_layout(
        title=f"CQI Daily Evolution - Site: {site_att}",
        xaxis=dict(
            title="Date",
            type='category',  # Changed from 'date' to 'category'
            tickangle=-45
        ),
        yaxis=dict(
            title="CQI Value",
            range=[0, ymax]
        ),
        hovermode="x unified",
        template="plotly_white",
        height=600,
        legend=dict(
            x=0.02,
            y=0.98,
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="rgba(0,0,0,0.2)",
            borderwidth=1
        )
    )
    
    # Add grid
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='rgba(128,128,128,0.2)')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='rgba(128,128,128,0.2)')
    
    return fig

def plot_site_data_traffic_daily(df):
    """Plot data traffic daily data for a site with stacked bars"""
    
    if df is None:
        fig = go.Figure()
        fig.add_annotation(text="No data available", xref="paper", yref="paper", x=0.5, y=0.5)
        return fig
        
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(text="No data found for selected site", xref="paper", yref="paper", x=0.5, y=0.5)
        return fig
    
    # Convert time to datetime first, then to string for X-axis
    df = df.copy()
    df['time'] = pd.to_datetime(df['time'])
    df['time_str'] = df['time'].dt.strftime('%Y-%m-%d')  # Convert to string for X-axis
    
    # Define data traffic columns to plot
    traffic_columns = {
        # 3G traffic
        'h3g_traffic_d_user_ps_gb': {'name': '3G Data Traffic (H)', 'color': '#1f77b4'},
        'e3g_traffic_d_user_ps_gb': {'name': '3G Data Traffic (E)', 'color': '#aec7e8'},
        'n3g_traffic_d_user_ps_gb': {'name': '3G Data Traffic (N)', 'color': '#c6dbef'},
        
        # 4G traffic
        'h4g_traffic_d_user_ps_gb': {'name': '4G Data Traffic (H)', 'color': '#2ca02c'},
        's4g_traffic_d_user_ps_gb': {'name': '4G Data Traffic (S)', 'color': '#98df8a'},
        'e4g_traffic_d_user_ps_gb': {'name': '4G Data Traffic (E)', 'color': '#c7e9c0'},
        'n4g_traffic_d_user_ps_gb': {'name': '4G Data Traffic (N)', 'color': '#e5f5e0'},
        
        # 5G traffic
        'e5g_nsa_traffic_pdcp_gb_5gendc_4glegn': {'name': '5G NSA Traffic (E) - 4G Leg', 'color': '#ff7f0e'},
        'n5g_nsa_traffic_pdcp_gb_5gendc_4glegn': {'name': '5G NSA Traffic (N) - 4G Leg', 'color': '#ffbb78'},
        'e5g_nsa_traffic_pdcp_gb_5gendc_5gleg': {'name': '5G NSA Traffic (E) - 5G Leg', 'color': '#d62728'},
        'n5g_nsa_traffic_pdcp_gb_5gendc_5gleg': {'name': '5G NSA Traffic (N) - 5G Leg', 'color': '#ff9896'}
    }
    
    # Create the plot
    fig = go.Figure()
    has_data = False
    
    # Add traces for each traffic type as stacked bars
    for col, config in traffic_columns.items():
        if col in df.columns:
            values = df[col].fillna(0)
            if values.sum() > 0:  # Only add if there's actual data
                has_data = True
                fig.add_trace(go.Bar(
                    x=df['time_str'],  # Use string dates instead of datetime
                    y=values,
                    name=config['name'],
                    marker_color=config['color'],
                    hovertemplate='<b>%{fullData.name}</b><br>' +
                                'Date: %{x}<br>' +
                                'Traffic: %{y:.2f} GB<br>' +
                                '<extra></extra>'
                ))
    
    # Get site_att for title
    site_att = df['site_att'].iloc[0] if 'site_att' in df.columns and not df.empty else 'Unknown Site'
    
    # If no data, show clean message
    if not has_data:
        fig.add_annotation(
            text=f"No traffic data available for site: {site_att}",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16, color='gray')
        )
    
    # Update layout for stacked bars - treat X-axis as category
    fig.update_layout(
        barmode='stack',
        title=f"Data Traffic Daily Evolution - Site: {site_att}",
        xaxis=dict(
            title="Date",
            type='category',  # Changed from 'date' to 'category'
            tickangle=-45
        ),
        yaxis=dict(
            title="Traffic (GB)"
        ),
        hovermode="x unified",
        template="plotly_white",
        height=600,
        legend=dict(
            x=0.02,
            y=0.98,
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="rgba(0,0,0,0.2)",
            borderwidth=1
        )
    )
    
    # Add grid
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='rgba(128,128,128,0.2)')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='rgba(128,128,128,0.2)')
    
    return fig

def plot_site_voice_traffic_daily(df):
    """Plot voice traffic daily data for a site with stacked bars"""
    
    # Add error handling for None DataFrame
    if df is None:
        fig = go.Figure()
        fig.add_annotation(text="No data available", xref="paper", yref="paper", x=0.5, y=0.5)
        return fig
        
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(text="No data found for selected site", xref="paper", yref="paper", x=0.5, y=0.5)
        return fig
    
    # Convert time to datetime first, then to string for X-axis
    df = df.copy()
    df['time'] = pd.to_datetime(df['time'])
    df['time_str'] = df['time'].dt.strftime('%Y-%m-%d')  # Convert to string for X-axis
    
    # Define voice traffic columns to plot
    voice_columns = {
        # VoLTE traffic (4G)
        'user_traffic_volte_e': {'name': 'VoLTE Traffic (E)', 'color': '#2ca02c'},
        'user_traffic_volte_h': {'name': 'VoLTE Traffic (H)', 'color': '#98df8a'},
        'user_traffic_volte_n': {'name': 'VoLTE Traffic (N)', 'color': '#c7e9c0'},
        'user_traffic_volte_s': {'name': 'VoLTE Traffic (S)', 'color': '#e5f5e0'},
        
        # Voice traffic (3G)
        'h3g_traffic_v_user_cs': {'name': '3G Voice Traffic (H)', 'color': '#1f77b4'},
        'e3g_traffic_v_user_cs': {'name': '3G Voice Traffic (E)', 'color': '#aec7e8'},
        'n3g_traffic_v_user_cs': {'name': '3G Voice Traffic (N)', 'color': '#c6dbef'}
    }
    
    # Create the plot
    fig = go.Figure()
    has_data = False
    
    # Add traces for each voice traffic type as stacked bars
    for col, config in voice_columns.items():
        if col in df.columns:
            values = df[col].fillna(0)
            if values.sum() > 0:  # Only add if there's actual data
                has_data = True
                fig.add_trace(go.Bar(
                    x=df['time_str'],  # Use string dates instead of datetime
                    y=values,
                    name=config['name'],
                    marker_color=config['color'],
                    hovertemplate='<b>%{fullData.name}</b><br>' +
                                'Date: %{x}<br>' +
                                'Voice Traffic: %{y:.2f}<br>' +
                                '<extra></extra>'
                ))
    
    # Get site_att for title
    site_att = df['site_att'].iloc[0] if 'site_att' in df.columns and not df.empty else 'Unknown Site'
    
    # If no data, show clean message
    if not has_data:
        fig.add_annotation(
            text=f"No voice traffic data available for site: {site_att}",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16, color='gray')
        )
    
    # Update layout for stacked bars - treat X-axis as category
    fig.update_layout(
        barmode='stack',
        title=f"Voice Traffic Daily Evolution - Site: {site_att}",
        xaxis=dict(
            title="Date",
            type='category',  # Changed from 'date' to 'category'
            tickangle=-45
        ),
        yaxis=dict(
            title="Voice Traffic"
        ),
        hovermode="x unified",
        template="plotly_white",
        height=600,
        legend=dict(
            x=0.02,
            y=0.98,
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="rgba(0,0,0,0.2)",
            borderwidth=1
        )
    )
    
    # Add grid
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='rgba(128,128,128,0.2)')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='rgba(128,128,128,0.2)')
    
    return fig

if __name__ == "__main__":
    # Test all plotting functions
    import sys
    import os
    
    # Add the current directory to Python path to import required modules
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    
    from select_db_cqi_daily import get_cqi_daily
    from select_db_cell_period import get_cell_change_data_grouped, expand_dates
    
    # Test with example site for CQI plots
    site_att = 'DIFALO0002'
    print(f"Fetching CQI data for site: {site_att}")
    
    # Get the CQI data
    cqi_data = get_cqi_daily(site_att)
    
    if cqi_data is not None and not cqi_data.empty:
        print(f"CQI Data fetched successfully. Shape: {cqi_data.shape}")
        print(f"Columns: {list(cqi_data.columns)}")
        print(f"Date range: {cqi_data['time'].min()} to {cqi_data['time'].max()}")
        
        # Create and display CQI plot
        print("\n1. Creating CQI Daily plot...")
        fig_cqi = plot_site_cqi_daily(cqi_data)
        show_plot(fig_cqi)
        save_plot_html(fig_cqi, f"cqi_daily_{site_att}.html", "output")
        
        # Create and display Data Traffic plot
        print("\n2. Creating Data Traffic Daily plot...")
        fig_traffic = plot_site_data_traffic_daily(cqi_data)
        show_plot(fig_traffic)
        save_plot_html(fig_traffic, f"data_traffic_daily_{site_att}.html", "output")
        
        # Create and display Voice Traffic plot
        print("\n3. Creating Voice Traffic Daily plot...")
        fig_voice = plot_site_voice_traffic_daily(cqi_data)
        show_plot(fig_voice)
        save_plot_html(fig_voice, f"voice_traffic_daily_{site_att}.html", "output")
        
        print(f"\nCQI and traffic plots created and saved for site: {site_att}")
        
    else:
        print("No CQI data found or error fetching data.")
    
    # Test cell change data plot
    print("\n" + "="*50)
    print("Testing Cell Change Data Plot")
    print("="*50)
    
    # Configuration for cell change data (using same pattern as select_db_cell_period)
    group_level = 'network'
    region_list = []
    province_list = []
    municipality_list = []
    site_list = ['DIFALO0002']
    technology_list = []  # Empty list means ALL
    vendor_list = []      # Empty list means ALL
    
    print(f"Starting cell change data retrieval with grouping by {group_level}...")
    
    # Get cell change data using the correct function signature
    cell_data = get_cell_change_data_grouped(
        group_by=group_level,
        site_list=site_list,
        region_list=region_list,
        province_list=province_list,
        municipality_list=municipality_list,
        technology_list=technology_list,
        vendor_list=vendor_list
    )
    
    if cell_data is not None and not cell_data.empty:
        print(f"Cell change data fetched successfully. Shape: {cell_data.shape}")
        print(f"Columns: {list(cell_data.columns)}")
        
        # Expand dates to fill gaps (like in the reference)
        cell_data_expanded = expand_dates(cell_data, group_by=group_level)
        
        # Create and display Cell Change plot
        print("\n4. Creating Cell Change Evolution plot...")
        fig_cell = plot_cell_change_data(
            df=cell_data_expanded,
            group_by=group_level,
            category='band_indicator',
            dates=None
        )
        show_plot(fig_cell)
        save_plot_html(fig_cell, f"cell_change_evolution_{group_level}.html", "output")
        
        print(f"\nCell change plot created and saved for {group_level}")
        
    else:
        print("No cell change data found or error fetching data.")
    
    print("\nAll test plots completed!")