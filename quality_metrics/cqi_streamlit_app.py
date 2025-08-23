import streamlit as st
import pandas as pd
import plotly.io as pio
from datetime import datetime

# Import the required modules
from cqi_evaluation_plotter import get_cqi_data, create_single_site_plot, create_neighbor_plot
from umts_cqi_evaluation_processor import umts_cqi_site_evaluation, umts_cqi_neighbor_evaluation
from lte_cqi_evaluation_processor import lte_cqi_site_evaluation, lte_cqi_neighbor_evaluation
from nr_cqi_evaluation_processor import nr_cqi_site_evaluation, nr_cqi_neighbor_evaluation
from master_node_plotter import query_sites_within_radius, create_summary_table, plot_sites_map
from cell_change_event_processor import export_cell_change_events

# Configure Streamlit page
st.set_page_config(
    page_title="RAN Quality Evaluator",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS to reduce top padding and make interface more compact
st.markdown("""
<style>
    .block-container {
        padding-top: 1rem;
        padding-bottom: 0rem;
        padding-left: 5rem;
        padding-right: 5rem;
    }
    .element-container {
        margin-bottom: 0.5rem;
    }
</style>
""", unsafe_allow_html=True)

# Main title
st.title("RAN Quality Evaluator")

# Sidebar for inputs
st.sidebar.header("Site Configuration")
site_att = st.sidebar.text_input("Site Name", value="MEXMET0396", help="Enter the site name (e.g., MEXMET0396)")

# Add button to get cell change events for date recommendation
if st.sidebar.button("Get Cell Change Events", type="primary", help="View historical cell changes to help select reference date"):
    if site_att.strip():
        with st.spinner("Fetching cell change events..."):
            try:
                # Use export_cell_change_events function like in the notebook
                cell_events_df = export_cell_change_events(site_att, output_format='dataframe')
                
                # Store in session state
                st.session_state.cell_events_df = cell_events_df
                st.session_state.cell_events_fetched = True
                
                if not cell_events_df.empty:
                    st.sidebar.success(f"Found {len(cell_events_df)} cell change events")
                else:
                    st.sidebar.info("No cell change events found")
                    
            except Exception as e:
                st.sidebar.error(f"Error fetching cell events: {str(e)}")
    else:
        st.sidebar.error("Please enter a valid site name")

input_date = st.sidebar.date_input("Reference Date", value=datetime(2024, 9, 21), help="Select the reference date for analysis")

# Convert date to string format
input_date_str = input_date.strftime('%Y-%m-%d')

# Analysis parameters (expandable section)
with st.sidebar.expander("Analysis Parameters", expanded=False):
    guard_days = st.number_input("Guard Period (days)", min_value=1, max_value=30, value=7)
    period_days = st.number_input("Analysis Period (days)", min_value=1, max_value=30, value=7)
    thd_low = st.number_input("Low Threshold (%)", min_value=1.0, max_value=20.0, value=5.0, step=0.5)
    thd_high = st.number_input("High Threshold (%)", min_value=1.0, max_value=20.0, value=5.0, step=0.5)
    radius_km = st.number_input("Neighbor Radius (km)", min_value=1.0, max_value=20.0, value=5.0, step=0.5)

# Run analysis button
if st.sidebar.button("Run Analysis", type="primary"):
    if site_att.strip():
        # Store results in session state
        st.session_state.analysis_complete = False
        st.session_state.site_att = site_att
        st.session_state.input_date_str = input_date_str
        
        with st.spinner("Running CQI analysis... This may take a few moments."):
            try:
                # Get CQI data for plotting
                single_site_data, neighbor_data, neighbor_sites, reference_date, db_last_date = get_cqi_data(
                    site_att, input_date_str, days_before=30, radius_km=radius_km, guard=guard_days, period=period_days
                )
                
                # Run evaluations
                umts_site_result = umts_cqi_site_evaluation(input_date_str, site_att, guard=guard_days, period=period_days, thd_low=thd_low, thd_high=thd_high)
                lte_site_result = lte_cqi_site_evaluation(input_date_str, site_att, guard=guard_days, period=period_days, thd_low=thd_low, thd_high=thd_high)
                nr_site_result = nr_cqi_site_evaluation(input_date_str, site_att, guard=guard_days, period=period_days, thd_low=thd_low, thd_high=thd_high)
                
                umts_neighbor_result = umts_cqi_neighbor_evaluation(input_date_str, site_att, radius=radius_km, guard=guard_days, period=period_days, thd_low=thd_low, thd_high=thd_high)
                lte_neighbor_result = lte_cqi_neighbor_evaluation(input_date_str, site_att, radius=radius_km, guard=guard_days, period=period_days, thd_low=thd_low, thd_high=thd_high)
                nr_neighbor_result = nr_cqi_neighbor_evaluation(input_date_str, site_att, radius=radius_km, guard=guard_days, period=period_days, thd_low=thd_low, thd_high=thd_high)
                
                # Get master node data
                raw_data = query_sites_within_radius(site_att, radius=radius_km)
                summary_table = create_summary_table(raw_data, csv_export=False)
                
                # Store results in session state
                st.session_state.single_site_data = single_site_data
                st.session_state.neighbor_data = neighbor_data
                st.session_state.neighbor_sites = neighbor_sites
                st.session_state.reference_date = reference_date
                st.session_state.db_last_date = db_last_date
                
                st.session_state.umts_site_result = umts_site_result
                st.session_state.lte_site_result = lte_site_result
                st.session_state.nr_site_result = nr_site_result
                st.session_state.umts_neighbor_result = umts_neighbor_result
                st.session_state.lte_neighbor_result = lte_neighbor_result
                st.session_state.nr_neighbor_result = nr_neighbor_result
                
                # Store master node data
                st.session_state.raw_data = raw_data
                st.session_state.summary_table = summary_table
                st.session_state.radius_km = radius_km
                
                st.session_state.analysis_complete = True
                st.success("Analysis completed successfully!")
                
            except Exception as e:
                st.error(f"Error during analysis: {str(e)}")
    else:
        st.sidebar.error("Please enter a valid site name")

# Display results if analysis is complete
if hasattr(st.session_state, 'analysis_complete') and st.session_state.analysis_complete:
    
    # Create tabs with Cell Events as first tab
    tab1, tab2, tab3 = st.tabs(["Cell Events", "CQI Results", "Site Map"])
    
    with tab1:
        st.header("Cell Change Events")
        st.markdown("Historical record of cell additions and deletions to help select appropriate reference dates.")
        
        if hasattr(st.session_state, 'cell_events_fetched') and st.session_state.cell_events_fetched:
            if hasattr(st.session_state, 'cell_events_df') and not st.session_state.cell_events_df.empty:
                st.subheader(f"Cell Change History for {st.session_state.site_att}")
                
                # Display the cell events table
                st.dataframe(
                    st.session_state.cell_events_df,
                    use_container_width=True,
                    hide_index=True
                )
                
                # Add recommendation text
                st.info("""
                **💡 Date Selection Recommendation:**
                - Choose a reference date **after** major cell changes (additions/deletions)
                - Avoid dates immediately following large cell modifications
                - Consider dates with stable cell configurations for meaningful analysis
                """)
                
                # Download button for cell events
                csv_cell_events = st.session_state.cell_events_df.to_csv(index=False)
                st.download_button(
                    label="Download Cell Events as CSV",
                    data=csv_cell_events,
                    file_name=f"cell_change_events_{st.session_state.site_att}.csv",
                    mime="text/csv"
                )
            else:
                st.info(f"No cell change events found for site: {st.session_state.site_att}")
        else:
            st.info("Click 'Get Cell Change Events' in the sidebar to view historical cell changes and get date recommendations.")
    
    with tab2:
        st.header("CQI Evaluation Results")
        
        # Helper functions
        def format_date_range(period_str):
            start_date, end_date = period_str.split(' to ')
            start_formatted = '/'.join(start_date.split('-')[::-1])
            end_formatted = '/'.join(end_date.split('-')[::-1])
            return f"{start_formatted} - {end_formatted}"
        
        def safe_format(value):
            return f"{value:.4f}" if value is not None else "N/A"
        
        # Create evaluation table
        evaluation_data = []
        
        # Site rows
        for result, tech in [(st.session_state.umts_site_result, 'umts_cqi'), 
                           (st.session_state.lte_site_result, 'lte_cqi'), 
                           (st.session_state.nr_site_result, 'nr_cqi')]:
            evaluation_data.append({
                'Level': 'Site',
                'KPI': tech,
                'Before': safe_format(result['before_avg']),
                'After': safe_format(result['after_avg']),
                'Last': safe_format(result['last_avg']),
                'Pattern Type': result['pattern'],
                'Pattern Result': result['evaluation']
            })
        
        # Neighbor rows
        for result, tech in [(st.session_state.umts_neighbor_result, 'umts_cqi'), 
                           (st.session_state.lte_neighbor_result, 'lte_cqi'), 
                           (st.session_state.nr_neighbor_result, 'nr_cqi')]:
            evaluation_data.append({
                'Level': 'Neighbor',
                'KPI': tech,
                'Before': safe_format(result['before_avg']),
                'After': safe_format(result['after_avg']),
                'Last': safe_format(result['last_avg']),
                'Pattern Type': result['pattern'],
                'Pattern Result': result['evaluation']
            })
        
        # Calculate final result
        all_evaluations = [st.session_state.umts_site_result['evaluation'], 
                          st.session_state.lte_site_result['evaluation'], 
                          st.session_state.nr_site_result['evaluation'],
                          st.session_state.umts_neighbor_result['evaluation'], 
                          st.session_state.lte_neighbor_result['evaluation'], 
                          st.session_state.nr_neighbor_result['evaluation']]
        
        fail_count = all_evaluations.count('FAIL')
        if fail_count > 0:
            final_result = 'FAIL'
        elif all_evaluations.count('PASS') == len(all_evaluations):
            final_result = 'PASS'
        else:
            final_result = 'RESTORE'
        
        # Add final result row
        evaluation_data.append({
            'Level': 'Final Result',
            'KPI': '',
            'Before': '',
            'After': '',
            'Last': '',
            'Pattern Type': '',
            'Pattern Result': final_result
        })
        
        # Create DataFrame
        evaluation_df = pd.DataFrame(evaluation_data)
        
        # Rename columns with proper headers
        evaluation_df.columns = [
            'Level', 
            'KPI', 
            f"Before ({format_date_range(st.session_state.umts_site_result['before_period'])})",
            f"After ({format_date_range(st.session_state.umts_site_result['after_period'])})",
            f"Last ({format_date_range(st.session_state.umts_site_result['last_period'])})",
            'Pattern Type', 
            'Pattern Result'
        ]
        
        # Display summary info
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Site", st.session_state.site_att)
        with col2:
            st.metric("Reference Date", st.session_state.input_date_str)
        with col3:
            # Color code the final result
            if final_result == 'PASS':
                st.success(f"Final Result: {final_result}")
            elif final_result == 'FAIL':
                st.error(f"Final Result: {final_result}")
            else:
                st.warning(f"Final Result: {final_result}")
        
        # Display the table
        st.dataframe(
            evaluation_df, 
            use_container_width=True,
            hide_index=True
        )
        
        # Add download button
        csv = evaluation_df.to_csv(index=False)
        st.download_button(
            label="Download Results as CSV",
            data=csv,
            file_name=f"cqi_evaluation_{st.session_state.site_att}_{st.session_state.input_date_str}.csv",
            mime="text/csv"
        )
        
        # Add separator and visualization section
        st.markdown("---")
        st.header("CQI Visualization")
        
        # Create and display single site plot
        st.subheader("Single Site CQI Trends")
        single_site_fig = create_single_site_plot(
            st.session_state.single_site_data, 
            st.session_state.site_att, 
            st.session_state.reference_date, 
            st.session_state.db_last_date
        )
        st.plotly_chart(single_site_fig, use_container_width=True)
        
        # Create and display neighbor plot if data exists
        if st.session_state.neighbor_data and any(not df.empty for df in st.session_state.neighbor_data.values()):
            st.subheader("Neighbor Sites CQI Trends")
            neighbor_fig = create_neighbor_plot(
                st.session_state.neighbor_data, 
                len(st.session_state.neighbor_sites), 
                st.session_state.reference_date, 
                st.session_state.db_last_date
            )
            st.plotly_chart(neighbor_fig, use_container_width=True)
        else:
            st.info("No neighbor data available for plotting")
    
    with tab3:
        st.header("Master Node Visualization")
        st.markdown(f"Interactive map and summary table showing target site and all neighbors within {st.session_state.radius_km}km radius.")
        
        # Display the map first
        if hasattr(st.session_state, 'summary_table') and not st.session_state.summary_table.empty:
            st.subheader("Site Location Map")
            map_figure = plot_sites_map(st.session_state.summary_table, st.session_state.site_att)
            st.plotly_chart(map_figure, use_container_width=True)
            
            # Display the summary table
            st.subheader(f"Summary Table - {len(st.session_state.summary_table)} sites within {st.session_state.radius_km}km")
            st.dataframe(
                st.session_state.summary_table,
                use_container_width=True,
                hide_index=True
            )
            
            # Add download button for the summary table
            csv_summary = st.session_state.summary_table.to_csv(index=False)
            st.download_button(
                label="Download Site Summary as CSV",
                data=csv_summary,
                file_name=f"master_node_summary_{st.session_state.site_att}_{st.session_state.radius_km}km.csv",
                mime="text/csv"
            )
        else:
            st.warning(f"No neighbor sites found within {st.session_state.radius_km}km of {st.session_state.site_att}")

else:
    # Display instructions when no analysis has been run
    st.info("Please configure the site parameters in the sidebar and click 'Run Analysis' to begin.")
    
    # Show cell events if they were fetched
    if hasattr(st.session_state, 'cell_events_fetched') and st.session_state.cell_events_fetched:
        st.header("Cell Change Events")
        st.markdown("Historical record of cell additions and deletions to help select appropriate reference dates.")
        
        if hasattr(st.session_state, 'cell_events_df') and not st.session_state.cell_events_df.empty:
            st.subheader(f"Cell Change History for {site_att}")
            
            # Display the cell events table
            st.dataframe(
                st.session_state.cell_events_df,
                use_container_width=True,
                hide_index=True
            )
            
            # Add recommendation text
            st.info("""
            **💡 Date Selection Recommendation:**
            - Choose a reference date **after** major cell changes (additions/deletions)
            - Avoid dates immediately following large cell modifications
            - Consider dates with stable cell configurations for meaningful analysis
            """)
            
            # Download button for cell events
            csv_cell_events = st.session_state.cell_events_df.to_csv(index=False)
            st.download_button(
                label="Download Cell Events as CSV",
                data=csv_cell_events,
                file_name=f"cell_change_events_{site_att}.csv",
                mime="text/csv"
            )
        else:
            st.info(f"No cell change events found for site: {site_att}")
    
    # Show some example information
    st.markdown("### How to use this application:")
    st.markdown("""
    1. **Enter Site Name**: Input the site identifier (e.g., MEXMET0396)
    2. **Get Cell Change Events**: Click to view historical cell changes for date guidance
    3. **Select Reference Date**: Choose the date for analysis based on cell change recommendations
    4. **Adjust Parameters** (optional): Modify analysis parameters if needed
    5. **Run Analysis**: Click the button to start the analysis
    6. **View Results**: Switch between tabs to see results
    """)
    
    st.markdown("### Analysis Output:")
    st.markdown("""
    - **Cell Events**: Historical cell changes to guide reference date selection
    - **CQI Results**: Comprehensive evaluation showing CQI patterns, results, and visualizations for all technologies
    - **Site Map**: Interactive map showing site locations and neighbor analysis
    """)

# Footer
st.markdown("---")
st.markdown("*RAN Quality Evaluator - OTLKM Team*")

# To run this Streamlit web interface, use the following command in terminal:
# cd C:\python\quality_metrics\code; python -m streamlit run cqi_streamlit_app.py
