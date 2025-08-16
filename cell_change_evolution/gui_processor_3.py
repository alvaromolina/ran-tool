import os
import asyncio
import pandas as pd
from shiny import App, ui, reactive, render
from shinywidgets import output_widget, render_widget  
from dotenv import load_dotenv
from select_db_master_node import get_max_date, get_att_names
from select_db_cell_period import get_cell_change_data_grouped, expand_dates
from select_db_cqi_daily import get_cqi_daily, get_traffic_data_daily, get_traffic_voice_daily
from select_db_neighbor_cqi_daily import get_neighbor_cqi_daily, get_neighbor_traffic_data, get_neighbor_traffic_voice
from plot_processor import plot_cell_change_data, plot_site_cqi_daily, plot_site_data_traffic_daily, plot_site_voice_traffic_daily
import plotly.graph_objects as go

# Load environment variables
dotenv_path = os.path.join("C:\\python\\cell_change_evolution\\code", ".env")
load_dotenv(dotenv_path)
ROOT_DIRECTORY = os.getenv("ROOT_DIRECTORY")
INPUT_DIRECTORY = os.path.join(ROOT_DIRECTORY, "input")
OUTPUT_DIRECTORY = os.path.join(ROOT_DIRECTORY, "output")
TMP_DIRECTORY = os.path.join(INPUT_DIRECTORY, "tmp")

app_ui = ui.page_fillable(
    ui.panel_title(None, window_title="CQI Impact Analysis"),
    ui.navset_card_pill( 
        ui.nav_panel("CELLCHANGE", 
            ui.card(  
                ui.layout_sidebar(  
                    ui.sidebar(
                        ui.input_select("select_category", "Select Type", {
                            'band_indicator':'band_indicator', 
                            'vendor':'vendor', 
                            'band':'band', 
                            'technology':'technology'
                        }, selected='band_indicator'),
                        ui.input_select("select_technology", "Technology Filter", {
                            'ALL':'ALL', '3G':'3G', '4G':'4G'
                        }, selected='ALL'),
                        ui.input_select("select_vendor", "Vendor Filter", {
                            'ALL':'ALL', 'huawei':'huawei', 'ericsson':'ericsson', 'nokia':'nokia', 'samsung':'samsung'
                        }, selected='ALL'),
                        ui.output_ui("dynamic_select_site_att"),
                        ui.output_ui("dynamic_select_min_date"),
                        ui.output_ui("dynamic_select_max_date"),
                        bg="#f8f8f8"
                    ),
                    ui.card(  
                        output_widget("chart01"),
                    ),
                ),  
            ),  
        ),

        ui.nav_panel("CQI", 
            ui.card(  
                ui.layout_sidebar(  
                    ui.sidebar(
                        ui.input_select("select_technology_cqi", "Technology Filter", {
                            'ALL':'ALL', '3G':'3G', '4G':'4G', '5G':'5G'
                        }, selected='ALL'),
                        ui.output_ui("dynamic_select_site_att_cqi"),
                        ui.output_ui("dynamic_select_min_date_cqi"),
                        ui.output_ui("dynamic_select_max_date_cqi"),
                        ui.input_numeric("radius_cqi", "Neighbor Radius (km)", value=5, min=1, max=50),
                        bg="#f8f8f8"
                    ),
                    ui.layout_column_wrap(
                        ui.card(
                            ui.card_header("Site CQI"),
                            output_widget("chart_cqi"),
                        ),
                        ui.card(
                            ui.card_header("Neighbor CQI"),
                            output_widget("chart_cqi_neighbor"),
                        ),
                        width=1/2
                    ),
                ),  
            ),  
        ),

        ui.nav_panel("TRAFFICDATA", 
            ui.card(  
                ui.layout_sidebar(  
                    ui.sidebar(
                        ui.input_select("select_technology_traffic", "Technology Filter", {
                            'ALL':'ALL', '3G':'3G', '4G':'4G', '5G':'5G'
                        }, selected='ALL'),
                        ui.output_ui("dynamic_select_site_att_traffic"),
                        ui.output_ui("dynamic_select_min_date_traffic"),
                        ui.output_ui("dynamic_select_max_date_traffic"),
                        ui.input_numeric("radius_traffic", "Neighbor Radius (km)", value=5, min=1, max=50),
                        bg="#f8f8f8"
                    ),
                    ui.layout_column_wrap(
                        ui.card(
                            ui.card_header("Site Traffic Data"),
                            output_widget("chart_traffic_data"),
                        ),
                        ui.card(
                            ui.card_header("Neighbor Traffic Data"),
                            output_widget("chart_traffic_data_neighbor"),
                        ),
                        width=1/2
                    ),
                ),  
            ),  
        ),

        ui.nav_panel("TRAFFICVOICE", 
            ui.card(  
                ui.layout_sidebar(  
                    ui.sidebar(
                        ui.input_select("select_technology_voice", "Technology Filter", {
                            'ALL':'ALL', '3G':'3G', '4G':'4G'
                        }, selected='ALL'),
                        ui.output_ui("dynamic_select_site_att_voice"),
                        ui.output_ui("dynamic_select_min_date_voice"),
                        ui.output_ui("dynamic_select_max_date_voice"),
                        ui.input_numeric("radius_voice", "Neighbor Radius (km)", value=5, min=1, max=50),
                        bg="#f8f8f8"
                    ),
                    ui.layout_column_wrap(
                        ui.card(
                            ui.card_header("Site Voice Traffic"),
                            output_widget("chart_traffic_voice"),
                        ),
                        ui.card(
                            ui.card_header("Neighbor Voice Traffic"),
                            output_widget("chart_traffic_voice_neighbor"),
                        ),
                        width=1/2
                    ),
                ),  
            ),  
        ),

        ui.nav_panel("DOWNLOAD", 
            ui.card(  
                ui.layout_sidebar( 
                    ui.sidebar( 
                        bg="#f8f8f8"
                     ), 
                     ui.download_button("download", "Download All Results csv files"),
                ),  
            ),  
        ),
        ui.nav_panel("UPDATE", 
            ui.card(  
                ui.layout_sidebar(  
                    ui.sidebar(
                        ui.input_action_button("update", "UPDATE"),
                        bg="#f8f8f8"
                    ),  
                    ui.output_text_verbatim("nc_processing_status"),
                ),  
            ) 
        ), 
        ui.nav_menu(
            "Other links",
            ui.nav_control(
                ui.a("Quality_Assurance", href="https://10.150.59.134/qos/login", target="_blank")
            ),
            ui.nav_control(
                ui.a("Quality_Assurance_Local", href="http://localhost:3000/?orgId=1", target="_blank")
            ),
        ),
        id="tab",
        title='DataAnalyticLab - CQI Impact Analytic'
    )  
)

def server(input, output, session):
    # Data loading
    def date_max():
        max_date = get_max_date()
        if max_date:
            return max_date.strftime('%Y-%m-%d')
        return "2024-01-01"
    
    date_max = date_max()
    sites = get_att_names()
    dict_site_att = {site: site for site in sites}
    
    # Reactive values
    selected_site_att = reactive.Value(None)
    progress_message_list = reactive.Value("execute update to start processing")
    
    # Reactive effects
    @reactive.Effect
    def update_site_att_selection():
        selected_site_att.set(input.select_site_att())
    
    # Dynamic UI
    @output
    @render.ui
    def dynamic_select_min_date():
        return ui.input_date("min_date", "Select Min Date", value="2024-01-01", min="2024-01-01", max=date_max)

    @output
    @render.ui
    def dynamic_select_max_date():
        return ui.input_date("max_date", "Select Max Date", value=date_max, min="2024-01-01", max=date_max)

    @output
    @render.ui
    def dynamic_select_site_att():
        return ui.input_select("select_site_att", "Select site_att", dict_site_att, selected='DIFALO0001')

    @output
    @render.ui
    def dynamic_select_site_att_cqi():
        return ui.input_select("select_site_att_cqi", "Select Site", dict_site_att, selected='DIFALO0001')
    
    @output
    @render.ui
    def dynamic_select_min_date_cqi():
        return ui.input_date("min_date_cqi", "Select Min Date", value="2024-01-01", min="2024-01-01", max=date_max)

    @output
    @render.ui
    def dynamic_select_max_date_cqi():
        return ui.input_date("max_date_cqi", "Select Max Date", value=date_max, min="2024-01-01", max=date_max)

    @output
    @render.ui
    def dynamic_select_site_att_traffic():
        return ui.input_select("select_site_att_traffic", "Select Site", dict_site_att, selected='DIFALO0001')
    
    @output
    @render.ui
    def dynamic_select_min_date_traffic():
        return ui.input_date("min_date_traffic", "Select Min Date", value="2024-01-01", min="2024-01-01", max=date_max)

    @output
    @render.ui
    def dynamic_select_max_date_traffic():
        return ui.input_date("max_date_traffic", "Select Max Date", value=date_max, min="2024-01-01", max=date_max)

    @output
    @render.ui
    def dynamic_select_site_att_voice():
        return ui.input_select("select_site_att_voice", "Select Site", dict_site_att, selected='DIFALO0001')
    
    @output
    @render.ui
    def dynamic_select_min_date_voice():
        return ui.input_date("min_date_voice", "Select Min Date", value="2024-01-01", min="2024-01-01", max=date_max)

    @output
    @render.ui
    def dynamic_select_max_date_voice():
        return ui.input_date("max_date_voice", "Select Max Date", value=date_max, min="2024-01-01", max=date_max)

    # Chart functions
    @output
    @render_widget
    def chart01():
        site_att = selected_site_att.get()
        if site_att is None:
            site_att = 'DIFALO0001'

        min_date = input.min_date().strftime("%Y-%m-%d")
        max_date = input.max_date().strftime("%Y-%m-%d")
        dates = [min_date, max_date]
        
        category = input.select_category()
        technology = input.select_technology()
        vendor = input.select_vendor()
        
        technology_list = None if technology == 'ALL' else [technology]
        vendor_list = None if vendor == 'ALL' else [vendor]

        df = get_cell_change_data_grouped(group_by='network', site_list=[site_att], technology_list=technology_list, vendor_list=vendor_list)
        df_expanded = expand_dates(df, group_by='network')
        
        fig01 = plot_cell_change_data(
            df=df_expanded,
            group_by='network',
            category=category,
            dates=dates
        )
        
        fig01.update_xaxes(
            tickformat="%Y-%m-%d",
            title_text=None,
            tickangle=-45
        )

        fig01.update_yaxes(
            showgrid=True
        )

        fig01.update_layout(
            title_text=f"Cumulative New LTE Cell per {category} per Date - Site {site_att} ({technology}, {vendor}):",
            title_x=0.5
        )
        
        return fig01

    @output
    @render_widget
    def chart_cqi():
        site_att = input.select_site_att_cqi() or 'DIFALO0001'
        min_date = input.min_date_cqi().strftime('%Y-%m-%d')
        max_date = input.max_date_cqi().strftime('%Y-%m-%d')
        technology_cqi = input.select_technology_cqi()
        
        technology_filter = None if technology_cqi == 'ALL' else technology_cqi
        
        cqi_data = get_cqi_daily(
            att_name=site_att,
            min_date=min_date, 
            max_date=max_date, 
            technology=technology_filter
        )
        
        fig_cqi = plot_site_cqi_daily(cqi_data)
        
        fig_cqi.update_layout(
            title_text=f"CQI Analysis - Site {site_att} ({technology_cqi} Technology)",
            title_x=0.5
        )
        
        return fig_cqi

    @output
    @render_widget
    def chart_cqi_neighbor():
        site_att = input.select_site_att_cqi() or 'DIFALO0001'
        min_date = input.min_date_cqi().strftime('%Y-%m-%d')
        max_date = input.max_date_cqi().strftime('%Y-%m-%d')
        technology_cqi = input.select_technology_cqi()
        radius = input.radius_cqi()
        
        technology_filter = None if technology_cqi == 'ALL' else technology_cqi
        
        neighbor_cqi_data = get_neighbor_cqi_daily(
            site_list=[site_att],
            min_date=min_date, 
            max_date=max_date, 
            technology=technology_filter,
            radius_km=radius
        )
        
        fig_neighbor_cqi = plot_site_cqi_daily(neighbor_cqi_data)
        
        fig_neighbor_cqi.update_layout(
            title_text=f"Neighbor CQI Analysis - {radius}km from {site_att} ({technology_cqi})",
            title_x=0.5
        )
        
        return fig_neighbor_cqi

    @output
    @render_widget
    def chart_traffic_data():
        site_att = input.select_site_att_traffic() or 'DIFALO0001'
        min_date = input.min_date_traffic().strftime('%Y-%m-%d')
        max_date = input.max_date_traffic().strftime('%Y-%m-%d')
        technology_traffic = input.select_technology_traffic()
        
        technology_filter = None if technology_traffic == 'ALL' else technology_traffic
        
        traffic_data = get_traffic_data_daily(
            att_name=site_att,
            min_date=min_date, 
            max_date=max_date, 
            technology=technology_filter,
            vendor=None
        )
        
        fig_traffic = plot_site_data_traffic_daily(traffic_data)
        
        fig_traffic.update_layout(
            title_text=f"Data Traffic Analysis - Site {site_att} ({technology_traffic} Technology)",
            title_x=0.5
        )
        
        return fig_traffic

    @output
    @render_widget
    def chart_traffic_data_neighbor():
        site_att = input.select_site_att_traffic() or 'DIFALO0001'
        min_date = input.min_date_traffic().strftime('%Y-%m-%d')
        max_date = input.max_date_traffic().strftime('%Y-%m-%d')
        technology_traffic = input.select_technology_traffic()
        radius = input.radius_traffic()
        
        technology_filter = None if technology_traffic == 'ALL' else technology_traffic
        
        neighbor_traffic_data = get_neighbor_traffic_data(
            site_list=[site_att],
            min_date=min_date, 
            max_date=max_date, 
            technology=technology_filter,
            radius_km=radius,
            vendor=None
        )
        
        fig_neighbor_traffic = plot_site_data_traffic_daily(neighbor_traffic_data)
        
        fig_neighbor_traffic.update_layout(
            title_text=f"Neighbor Data Traffic - {radius}km from {site_att} ({technology_traffic})",
            title_x=0.5
        )
        
        return fig_neighbor_traffic

    @output
    @render_widget
    def chart_traffic_voice():
        site_att = input.select_site_att_voice() or 'DIFALO0001'
        min_date = input.min_date_voice().strftime('%Y-%m-%d')
        max_date = input.max_date_voice().strftime('%Y-%m-%d')
        technology_voice = input.select_technology_voice()
        
        technology_filter = None if technology_voice == 'ALL' else technology_voice
        
        voice_data = get_traffic_voice_daily(
            att_name=site_att,
            min_date=min_date, 
            max_date=max_date, 
            technology=technology_filter,
            vendor=None
        )
        
        fig_voice = plot_site_voice_traffic_daily(voice_data)
        
        fig_voice.update_layout(
            title_text=f"Voice Traffic Analysis - Site {site_att} ({technology_voice} Technology)",
            title_x=0.5
        )
        
        return fig_voice

    @output
    @render_widget
    def chart_traffic_voice_neighbor():
        site_att = input.select_site_att_voice() or 'DIFALO0001'
        min_date = input.min_date_voice().strftime('%Y-%m-%d')
        max_date = input.max_date_voice().strftime('%Y-%m-%d')
        technology_voice = input.select_technology_voice()
        radius = input.radius_voice()
        
        technology_filter = None if technology_voice == 'ALL' else technology_voice
        
        neighbor_voice_data = get_neighbor_traffic_voice(
            site_list=[site_att],
            min_date=min_date, 
            max_date=max_date, 
            technology=technology_filter,
            radius_km=radius,
            vendor=None
        )
        
        fig_neighbor_voice = plot_site_voice_traffic_daily(neighbor_voice_data)
        
        fig_neighbor_voice.update_layout(
            title_text=f"Neighbor Voice Traffic - {radius}km from {site_att} ({technology_voice})",
            title_x=0.5
        )
        
        return fig_neighbor_voice

    # UPDATE functionality
    @output
    @render.text
    def nc_processing_status():
        return progress_message_list()

    @reactive.effect
    @reactive.event(input.update)  
    async def nc_processing():
        with ui.Progress(min=1, max=8) as p:
            p.set(message="Calculation in progress", detail="This may take a while...")
            progress_message_list.set("Processing started...")


app = App(app_ui, server)

if __name__ == "__main__":
    import shiny
    shiny.run_app(app, port=8083)