import os
import asyncio
import pandas as pd
from shiny import App, ui, reactive, render
from shinywidgets import output_widget, render_widget  
from dotenv import load_dotenv
from select_db_master_node import get_max_date, get_provinces, get_municipalities, get_att_names
from select_db_cell_period import get_cell_change_data_grouped, expand_dates
from plot_processor import plot_cell_change_data

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
        ui.nav_panel("NATIONAL", 
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
                        ui.output_ui("dynamic_select_min_date"),    # NEW: min_date
                        ui.output_ui("dynamic_select_max_date"),    # RENAMED: max_date
                        bg="#f8f8f8"
                    ),
                    ui.card(  
                        output_widget("chart01"),
                    ),
                ),  
            ),  
        ),

        ui.nav_panel("REGION", 
            ui.card(  
                ui.layout_sidebar(  
                    ui.sidebar(
                        ui.input_select("select_category1", "Select Type", {
                            'band_indicator':'band_indicator', 
                            'vendor':'vendor', 
                            'band':'band', 
                            'technology':'technology'
                        }, selected='band_indicator'),
                        ui.input_select("select_technology1", "Technology Filter", {
                            'ALL':'ALL', '3G':'3G', '4G':'4G'
                        }, selected='ALL'),
                        ui.input_select("select_vendor1", "Vendor Filter", {
                            'ALL':'ALL', 'huawei':'huawei', 'ericsson':'ericsson', 'nokia':'nokia', 'samsung':'samsung'
                        }, selected='ALL'),
                        ui.output_ui("dynamic_select_region"),
                        ui.output_ui("dynamic_select_min_date1"),   # REGION
                        ui.output_ui("dynamic_select_max_date1"),   # RENAMED: max_date
                        bg="#f8f8f8"
                    ),
                    ui.card(  
                        output_widget("chart11"),
                    ),
                ),  
            ),  
        ),

        ui.nav_panel("PROVINCE", 
            ui.card(  
                ui.layout_sidebar(  
                    ui.sidebar(
                        ui.input_select("select_category2", "Select Type", {
                            'band_indicator':'band_indicator', 
                            'vendor':'vendor', 
                            'band':'band', 
                            'technology':'technology'
                        }, selected='band_indicator'),
                        ui.input_select("select_technology2", "Technology Filter", {
                            'ALL':'ALL', '3G':'3G', '4G':'4G'
                        }, selected='ALL'),
                        ui.input_select("select_vendor2", "Vendor Filter", {
                            'ALL':'ALL', 'huawei':'huawei', 'ericsson':'ericsson', 'nokia':'nokia', 'samsung':'samsung'
                        }, selected='ALL'),
                        ui.output_ui("dynamic_select_province"),
                        ui.output_ui("dynamic_select_min_date2"),   # PROVINCE  
                        ui.output_ui("dynamic_select_max_date2"),   # RENAMED: max_date
                        bg="#f8f8f8"
                    ),
                    ui.card(  
                        output_widget("chart21"),
                    ),
                ),  
            ),  
        ),

        ui.nav_panel("MUNICIPALITY", 
            ui.card(  
                ui.layout_sidebar(  
                    ui.sidebar(
                        ui.input_select("select_category3", "Select Type", {
                            'band_indicator':'band_indicator', 
                            'vendor':'vendor', 
                            'band':'band', 
                            'technology':'technology'
                        }, selected='band_indicator'),
                        ui.input_select("select_technology3", "Technology Filter", {
                            'ALL':'ALL', '3G':'3G', '4G':'4G'
                        }, selected='ALL'),
                        ui.input_select("select_vendor3", "Vendor Filter", {
                            'ALL':'ALL', 'huawei':'huawei', 'ericsson':'ericsson', 'nokia':'nokia', 'samsung':'samsung'
                        }, selected='ALL'),
                        ui.output_ui("dynamic_select_municipality"),
                        ui.output_ui("dynamic_select_min_date3"),   # MUNICIPALITY
                        ui.output_ui("dynamic_select_max_date3"),   # RENAMED: max_date
                        bg="#f8f8f8"
                    ),
                    ui.card(  
                        output_widget("chart31"),
                    ),
                ),  
            ),  
        ),

        ui.nav_panel("SITE", 
            ui.card(  
                ui.layout_sidebar(  
                    ui.sidebar(
                        ui.input_select("select_category4", "Select Type", {
                            'band_indicator':'band_indicator', 
                            'vendor':'vendor', 
                            'band':'band', 
                            'technology':'technology'
                        }, selected='band_indicator'),
                        ui.input_select("select_technology4", "Technology Filter", {
                            'ALL':'ALL', '3G':'3G', '4G':'4G'
                        }, selected='ALL'),
                        ui.input_select("select_vendor4", "Vendor Filter", {
                            'ALL':'ALL', 'huawei':'huawei', 'ericsson':'ericsson', 'nokia':'nokia', 'samsung':'samsung'
                        }, selected='ALL'),
                        ui.output_ui("dynamic_select_site_att"),
                        ui.output_ui("dynamic_select_min_date4"),   # SITE
                        ui.output_ui("dynamic_select_max_date4"),   # RENAMED: max_date
                        bg="#f8f8f8"
                    ),
                    ui.card(  
                        output_widget("chart41"),
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

    def date_max():
        max_date = get_max_date()
        if max_date:
            return max_date.strftime('%Y-%m-%d')
        return "2024-01-01"
    
    date_max = date_max()

    regions = ['NORTH', 'NORTH_WEST', 'PACIFIC', 'CENTRO', 'SOUTH']
    dict_region = {region: region for region in regions}

    provinces = get_provinces()
    dict_province = {province: province for province in provinces}

    municipalities = get_municipalities()
    dict_municipality = {municipality: municipality for municipality in municipalities}

    sites = get_att_names()
    dict_site_att = {site: site for site in sites}
    
    progress_message_list = reactive.Value("execute update to start processing")
    
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
    def dynamic_select_min_date1():
        return ui.input_date("min_date1", "Select Min Date", value="2024-01-01", min="2024-01-01", max=date_max)

    @output
    @render.ui
    def dynamic_select_max_date1():
        return ui.input_date("max_date1", "Select Max Date", value=date_max, min="2024-01-01", max=date_max)

    @output
    @render.ui
    def dynamic_select_min_date2():
        return ui.input_date("min_date2", "Select Min Date", value="2024-01-01", min="2024-01-01", max=date_max)

    @output
    @render.ui
    def dynamic_select_max_date2():
        return ui.input_date("max_date2", "Select Max Date", value=date_max, min="2024-01-01", max=date_max)

    @output
    @render.ui
    def dynamic_select_min_date3():
        return ui.input_date("min_date3", "Select Min Date", value="2024-01-01", min="2024-01-01", max=date_max)

    @output
    @render.ui
    def dynamic_select_max_date3():
        return ui.input_date("max_date3", "Select Max Date", value=date_max, min="2024-01-01", max=date_max)

    @output
    @render.ui
    def dynamic_select_min_date4():
        return ui.input_date("min_date4", "Select Min Date", value="2024-01-01", min="2024-01-01", max=date_max)

    @output
    @render.ui
    def dynamic_select_max_date4():
        return ui.input_date("max_date4", "Select Max Date", value=date_max, min="2024-01-01", max=date_max)

    @output
    @render.ui
    def dynamic_select_region():
        return ui.input_select("select_region", "Select Region", dict_region, selected='CENTRO')
    
    @output
    @render.ui
    def dynamic_select_province():
        return ui.input_select("select_province", "Select Province", dict_province, selected='DISTRITO_FEDERAL')
    
    @output
    @render.ui
    def dynamic_select_municipality():
        return ui.input_select("select_municipality", "Select municipality", dict_municipality, selected='ALVARO_OBREGON-DF')

    @output
    @render.ui
    def dynamic_select_site_att():
        return ui.input_select("select_site_att", "Select site_att", dict_site_att, selected='DIFALO0001')
        
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

    @output
    @render_widget
    def chart01():
        min_date = input.min_date().strftime("%Y-%m-%d")
        max_date = input.max_date().strftime("%Y-%m-%d")
        dates = [min_date, max_date]
        category = input.select_category()
        technology = input.select_technology()
        vendor = input.select_vendor()
        
        technology_list = None if technology == 'ALL' else [technology]
        vendor_list = None if vendor == 'ALL' else [vendor]

        df = get_cell_change_data_grouped(group_by='network', technology_list=technology_list, vendor_list=vendor_list)
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
            title_text=f"Cumulative New LTE Cell per {category} per Date - National ({technology}, {vendor}):",
            title_x=0.5
        )
        
        return fig01

    selected_region = reactive.Value(None)

    @reactive.Effect
    def update_region_selection():
        selected_region.set(input.select_region())
  
    @output
    @render_widget
    def chart11():
        region = selected_region.get()
        if region is None:
            region = 'CENTRO'
        min_date1 = input.min_date1().strftime("%Y-%m-%d")
        max_date1 = input.max_date1().strftime("%Y-%m-%d")
        dates1 = [min_date1, max_date1]
        category1 = input.select_category1()
        technology1 = input.select_technology1()
        vendor1 = input.select_vendor1()
        
        technology_list = None if technology1 == 'ALL' else [technology1]
        vendor_list = None if vendor1 == 'ALL' else [vendor1]

        df = get_cell_change_data_grouped(group_by='region', region_list=[region], technology_list=technology_list, vendor_list=vendor_list)
        df_expanded = expand_dates(df, group_by='region')
        
        fig11 = plot_cell_change_data(
            df=df_expanded,
            group_by='region',
            category=category1,
            dates=dates1
        )
        
        fig11.update_xaxes(
            tickformat="%Y-%m-%d",
            title_text=None,
            tickangle=-45
        )

        fig11.update_yaxes(
            showgrid=True
        )

        fig11.update_layout(
            title_text=f"Cumulative New LTE Cell per {category1} per Date - Region {region} ({technology1}, {vendor1}):",
            title_x=0.5
        )
        
        return fig11

    selected_province = reactive.Value(None)

    @reactive.Effect
    def update_province_selection():
        selected_province.set(input.select_province())
  
    @output
    @render_widget
    def chart21():
        province = selected_province.get()
        if province is None:
            province = 'DISTRITO_FEDERAL'
        min_date2 = input.min_date2().strftime("%Y-%m-%d")
        max_date2 = input.max_date2().strftime("%Y-%m-%d")
        dates2 = [min_date2, max_date2]
        category2 = input.select_category2()
        technology2 = input.select_technology2()
        vendor2 = input.select_vendor2()
        
        technology_list = None if technology2 == 'ALL' else [technology2]
        vendor_list = None if vendor2 == 'ALL' else [vendor2]

        df = get_cell_change_data_grouped(group_by='province', province_list=[province], technology_list=technology_list, vendor_list=vendor_list)
        df_expanded = expand_dates(df, group_by='province')
        
        fig21 = plot_cell_change_data(
            df=df_expanded,
            group_by='province',
            category=category2,
            dates=dates2
        )
        
        fig21.update_xaxes(
            tickformat="%Y-%m-%d",
            title_text=None,
            tickangle=-45
        )

        fig21.update_yaxes(
            showgrid=True
        )

        fig21.update_layout(
            title_text=f"Cumulative New LTE Cell per {category2} per Date - Province {province} ({technology2}, {vendor2}):",
            title_x=0.5
        )

        return fig21

    selected_municipality = reactive.Value(None)

    @reactive.Effect
    def update_municipality_selection():
        selected_municipality.set(input.select_municipality())
  
    @output
    @render_widget
    def chart31():
        municipality = selected_municipality.get()
        if municipality is None:
            municipality = 'ALVARO_OBREGON-DF'
        min_date3 = input.min_date3().strftime("%Y-%m-%d")
        max_date3 = input.max_date3().strftime("%Y-%m-%d")
        dates3 = [min_date3, max_date3]
        category3 = input.select_category3()
        technology3 = input.select_technology3()
        vendor3 = input.select_vendor3()
        
        technology_list = None if technology3 == 'ALL' else [technology3]
        vendor_list = None if vendor3 == 'ALL' else [vendor3]

        df = get_cell_change_data_grouped(group_by='municipality', municipality_list=[municipality], technology_list=technology_list, vendor_list=vendor_list)
        df_expanded = expand_dates(df, group_by='municipality')
        
        fig31 = plot_cell_change_data(
            df=df_expanded,
            group_by='municipality',
            category=category3,
            dates=dates3
        )
        
        fig31.update_xaxes(
            tickformat="%Y-%m-%d",
            title_text=None,
            tickangle=-45
        )

        fig31.update_yaxes(
            showgrid=True
        )

        fig31.update_layout(
            title_text=f"Cumulative New LTE Cell per {category3} per Date - Municipality {municipality} ({technology3}, {vendor3}):",
            title_x=0.5
        )

        return fig31

    selected_site_att = reactive.Value(None)

    @reactive.Effect
    def update_site_att_selection():
        selected_site_att.set(input.select_site_att())
  
    @output
    @render_widget
    def chart41():
        site_att = selected_site_att.get()
        if site_att is None:
            site_att = 'DIFALO0001'
        min_date4 = input.min_date4().strftime("%Y-%m-%d")
        max_date4 = input.max_date4().strftime("%Y-%m-%d")
        dates4 = [min_date4, max_date4]
        category4 = input.select_category4()
        technology4 = input.select_technology4()
        vendor4 = input.select_vendor4()
        
        technology_list = None if technology4 == 'ALL' else [technology4]
        vendor_list = None if vendor4 == 'ALL' else [vendor4]

        df = get_cell_change_data_grouped(group_by='network', site_list=[site_att], technology_list=technology_list, vendor_list=vendor_list)
        df_expanded = expand_dates(df, group_by='network')
        
        fig41 = plot_cell_change_data(
            df=df_expanded,
            group_by='network',
            category=category4,
            dates=dates4
        )
        
        fig41.update_xaxes(
            tickformat="%Y-%m-%d",
            title_text=None,
            tickangle=-45
        )

        fig41.update_yaxes(
            showgrid=True
        )

        fig41.update_layout(
            title_text=f"Cumulative New LTE Cell per {category4} per Date - Site {site_att} ({technology4}, {vendor4}):",
            title_x=0.5
        )

        return fig41


app = App(app_ui, server)

if __name__ == "__main__":
    import shiny
    shiny.run_app(app, port=8081)