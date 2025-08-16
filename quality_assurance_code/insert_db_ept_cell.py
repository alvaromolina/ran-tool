import pandas as pd
import numpy as np
import os
import dotenv
from shapely.geometry import Point, Polygon
from geopy.distance import distance
import psycopg2
import time

# Load environment variables
dotenv.load_dotenv()
ROOT_DIRECTORY = os.getenv('ROOT_DIRECTORY')
POSTGRES_USERNAME = os.getenv('POSTGRES_USERNAME')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_DB = os.getenv('POSTGRES_DB')


# -------------------------
# Step 1: CREATE EPT_CELL  
# -------------------------
def create_ept_cell(workdir):
    """
    Processes Excel data files to extract and clean LTE site data, including calculating radio values
    and renaming beam_width as beam.
    """
    input_file_path = os.path.join(ROOT_DIRECTORY, "input", workdir, "EPT.xlsx")

    if not os.path.isfile(input_file_path):
        print(f"Input file {input_file_path} not found.")
        return

    expected_columns = [
        "AT&T_Site_Name", "AT&T Name", "AT&T_Tech", "Latitud", "Longitud", "State", "Country", "Region", "Coverage",
        "Traffic_Of_Last_Month", "Band_Indicator", "Band_width", "UL_UARFCN/EARFCN", "DL_UARFCN/EARFCN",
        "CellName", "Node_B_ID", "Physical_Sector", "CELL_ID", "Local_CELL_ID", "PSC/PCI",
        "RNC", "RNC_ID", "LAC/TAL", "TAC", "RAC", "URA", "SAC", "Geographic_Azimuth", "Beam_width", "Vendor", "Tracker"
    ]

    column_rename_map = {
        "AT&T_Site_Name": "att_site", "AT&T Name": "att_name", "AT&T_Tech": "att_tech",
        "Latitud": "latitude", "Longitud": "longitude", "State": "state", "Country": "province", "Region": "region",
        "Coverage": "coverage", "Traffic_Of_Last_Month": "status", "Band_Indicator": "band_indicator",
        "Band_width": "band_width", "UL_UARFCN/EARFCN": "ul_arfcn", "DL_UARFCN/EARFCN": "dl_arfcn",
        "CellName": "cell_name", "Node_B_ID": "node_id", "Physical_Sector": "physical_sector", "CELL_ID": "cell_id",
        "Local_CELL_ID": "local_cell_id", "PSC/PCI": "psc", "RNC": "rnc_name", "RNC_ID": "rnc_id",
        "LAC/TAL": "lac", "TAC": "tac", "RAC": "rac", "URA": "ura", "SAC": "sac",
        "Geographic_Azimuth": "azimuth", "Beam_width": "beam", "Vendor": "vendor", "Tracker": "tracker"
    }

    sheets = {
        'outdoor': pd.read_excel(input_file_path, sheet_name='EPT_3G_LTE_OUTDOOR'),
        'indoor': pd.read_excel(input_file_path, sheet_name='EPT_3G_LTE_INDOOR')
    }

    for name, df in sheets.items():
        missing = [col for col in expected_columns if col not in df.columns]
        if missing:
            raise ValueError(f"Sheet '{name}' is missing columns: {missing}")

    df_combined = pd.concat(
        [sheets['outdoor'][expected_columns], sheets['indoor'][expected_columns]],
        ignore_index=True
    )

    df_combined.rename(columns=column_rename_map, inplace=True)

    for col in df_combined.columns:
        if col not in ["latitude", "longitude"]:
            df_combined[col] = df_combined[col].astype(str)

    df_combined.replace("-", "", inplace=True)
    df_combined.fillna("", inplace=True)
    df_combined.dropna(subset=["cell_name"], inplace=True)
    df_combined = df_combined[df_combined["cell_name"] != ""]

    for col in ["coverage", "vendor", "band_indicator"]:
        df_combined[col] = df_combined[col].str.lower()

    df_combined["latitude"] = pd.to_numeric(df_combined["latitude"], errors="coerce")
    df_combined["longitude"] = pd.to_numeric(df_combined["longitude"], errors="coerce")

    def calculate_radio(row):
        rules = {
            "indoor": {"band 2_pcs": 35, "band 26_800": 45, "band 4_aws": 30,
                       "band 5_850": 40, "band 7_2600": 25, "default": 25},
            "outdoor": {"band 2_pcs": 80, "band 26_800": 100, "band 4_aws": 70,
                        "band 42_3.5": 0, "band 5_850": 90, "band 7_2600": 60, "default": 50}
        }
        coverage = row["coverage"]
        band = row["band_indicator"]
        return rules.get(coverage, {}).get(band, rules.get(coverage, {}).get("default", 50))

    df_combined["radio"] = df_combined.apply(calculate_radio, axis=1)

    grouped_df = df_combined.groupby("cell_name").agg({
        "att_site": "first", "att_name": "first", "att_tech": "first", "latitude": "max", "longitude": "max",
        "state": "first", "province": "first", "region": "first", "coverage": "first", "status": "first",
        "band_indicator": "first", "band_width": "first", "ul_arfcn": "first", "dl_arfcn": "first",
        "node_id": "first", "physical_sector": "first", "cell_id": "first", "local_cell_id": "first",
        "psc": "first", "rnc_name": "first", "rnc_id": "first", "lac": "min", "tac": "min", "rac": "min",
        "ura": "first", "sac": "first", "azimuth": "first", "beam": "first", "vendor": "first", "tracker": "first",
        "radio": "first"
    }).reset_index()

    print("Dataframe 'ept_cell' created successfully.")
    return grouped_df


def build_sector_cone(lat, lon, azimuth, beam, radius_m, steps=10):
    center = Point(lon, lat)
    start_angle = azimuth - beam / 2
    end_angle = azimuth + beam / 2
    angles = np.linspace(start_angle, end_angle, steps)

    arc_points = []
    for angle in angles:
        dest = distance(meters=radius_m).destination((lat, lon), angle)
        arc_points.append(Point(dest.longitude, dest.latitude))

    polygon = Polygon([center] + arc_points + [center])
    return polygon


# -------------------------
# Step 2: INSERT EPT_CELL  
# -------------------------
def insert_ept_cell(df):
    numeric_fields = [
        "latitude", "longitude", "ul_arfcn", "dl_arfcn", "node_id", "cell_id", "local_cell_id", "psc",
        "rnc_id", "lac", "tac", "rac", "azimuth", "beam", "radio"
    ]
    for col in numeric_fields:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    text_fields = [
        "cell_name", "att_site", "att_name", "att_tech", "state", "province", "region",
        "coverage", "status", "band_indicator", "band_width", "physical_sector", "rnc_name",
        "ura", "sac", "vendor", "tracker"
    ]
    for col in text_fields:
        df[col] = df[col].fillna("").astype(str)

    df["geom_cell"] = df.apply(lambda row: Point(row["longitude"], row["latitude"]).wkt, axis=1)
    df["geom_sector"] = df.apply(
        lambda row: build_sector_cone(row["latitude"], row["longitude"], row["azimuth"], row["beam"], row["radio"]).wkt,
        axis=1
    )

    columns = [
        "cell_name", "att_site", "att_name", "att_tech", "latitude", "longitude",
        "state", "province", "region", "coverage", "status", "band_indicator", "band_width",
        "ul_arfcn", "dl_arfcn", "node_id", "physical_sector", "cell_id", "local_cell_id",
        "psc", "rnc_name", "rnc_id", "lac", "tac", "rac", "ura", "sac", "vendor", "tracker",
        "azimuth", "beam", "radio", "geom_cell", "geom_sector"
    ]

    try:
        conn = psycopg2.connect(
            user=POSTGRES_USERNAME,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB
        )
        cursor = conn.cursor()

        for _, row in df.iterrows():
            values = [row[col] if col not in ["geom_cell", "geom_sector"] else None for col in columns]
            geom_cell_wkt = row["geom_cell"]
            geom_sector_wkt = row["geom_sector"]

            sql = f"""
                INSERT INTO ept_cell ({', '.join(columns)})
                VALUES ({', '.join(['%s'] * (len(columns) - 2))}, ST_GeomFromText(%s, 4326), ST_GeomFromText(%s, 4326))
                ON CONFLICT (cell_name) DO NOTHING;
            """
            cursor.execute(sql, values[:-2] + [geom_cell_wkt, geom_sector_wkt])

        conn.commit()
        cursor.close()
        conn.close()
        print("Data inserted successfully into ept_cell.")

    except Exception as e:
        print(f"An error occurred during insertion: {e}")


# -------------------------
# MAIN CASCADE CONTROLLER
# -------------------------
def process_ept_cell(workdir):
    print("\nStep 1: create_ept_cell")
    t1 = time.time()
    df_ept = create_ept_cell(workdir)
    df_ept.to_csv(os.path.join(ROOT_DIRECTORY, "output", "ept.csv"), index=False)
    elapsed1 = time.time() - t1
    m1, s1 = divmod(elapsed1, 60)
    print(f"Completed in {int(m1)}m:{int(s1)}s")

    print("\nStep 2: insert_ept_cell")
    t2 = time.time()
    insert_ept_cell(df_ept)
    elapsed2 = time.time() - t2
    m2, s2 = divmod(elapsed2, 60)
    print(f"Completed in {int(m2)}m:{int(s2)}s")

    print(f"\n===== EPT Processing Completed =====")
    return
