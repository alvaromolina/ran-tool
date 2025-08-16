from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Query
import pandas as pd
import numpy as np
import math
from sqlalchemy import text

# Reuse existing data-access logic from analysis modules
from cell_change_evolution.select_db_master_node import get_max_date
from cell_change_evolution.select_db_cell_period import (
    get_cell_change_data_grouped,
    expand_dates,
    create_zero_filled_result,
)
from cell_change_evolution.select_db_cqi_daily import (
    get_cqi_daily,
    get_traffic_data_daily,
    get_traffic_voice_daily,
)
from cell_change_evolution.select_db_neighbor_cqi_daily import (
    get_neighbor_sites,
    get_neighbor_cqi_daily,
    get_neighbor_traffic_data,
    get_neighbor_traffic_voice,
    create_connection,
)

router = APIRouter(prefix="/sites", tags=["sites"])

# Utility: ensure DataFrame is JSON-safe (no NaN/Inf) and time serialized
def df_json_records(df: pd.DataFrame) -> list:
    if df is None:
        return []
    # 1) Replace +/-inf with NaN
    df = df.replace([np.inf, -np.inf], np.nan)
    # 2) Convert to object to allow None
    df = df.astype(object)
    # 3) Replace NaN/NA with None (ensure pd.NA also becomes None)
    df = df.where(pd.notnull(df), None)
    df = df.replace({pd.NA: None})
    # 4) Ensure time/date strings
    if 'time' in df.columns:
        df['time'] = df['time'].astype(str)
    if 'date' in df.columns:
        df['date'] = df['date'].astype(str)
    records = df.to_dict(orient='records')
    # 5) Final safety: replace any lingering NaNs in serialized records
    for rec in records:
        for k, v in rec.items():
            if isinstance(v, (float,)) and math.isnan(v):
                rec[k] = None
            elif isinstance(v, np.floating) and np.isnan(v):
                rec[k] = None
    return records


@router.get("/search")
def search_sites(q: str = Query(..., min_length=1), limit: int = Query(10, ge=1, le=50)):
    """Autocomplete site IDs from master_node_total by prefix (case-insensitive).
    Detects identifier column (att_name or node).
    """
    engine = create_connection()
    if engine is None:
        return []
    try:
        cols_query = text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'master_node_total'
            """
        )
        with engine.connect() as conn:
            cols = {row[0] for row in conn.execute(cols_query)}

        id_col = 'att_name' if 'att_name' in cols else ('node' if 'node' in cols else None)
        if not id_col:
            return []

        sql = text(
            f"""
            SELECT DISTINCT {id_col} AS site_id
            FROM public.master_node_total
            WHERE {id_col} IS NOT NULL AND {id_col} ILIKE :pattern
            ORDER BY {id_col}
            LIMIT :limit
            """
        )
        pattern = f"{q}%"
        df = pd.read_sql_query(sql, engine, params={"pattern": pattern, "limit": limit})
        return df["site_id"].dropna().astype(str).tolist()
    finally:
        engine.dispose()


@router.get("/{site_att}/ranges")
def get_site_ranges(site_att: str, input_date: Optional[date] = Query(None)):
    """
    MVP: return the global max available date to bound UI date-pickers.
    Later: compute per-site before/after ranges based on traffic periods and events.
    """
    max_date = get_max_date()
    return {
        "site_att": site_att,
        "input_date": str(input_date) if input_date else None,
        "max_date": str(max_date) if max_date else None,
    }


@router.get("/{site_att}/cell-changes")
def get_site_cell_changes(
    site_att: str,
    group_by: str = Query("network", pattern="^(network|region|province|municipality)$"),
    regions: Optional[List[str]] = Query(None),
    provinces: Optional[List[str]] = Query(None),
    municipalities: Optional[List[str]] = Query(None),
    technologies: Optional[List[str]] = Query(None, description="e.g. 3G,4G"),
    vendors: Optional[List[str]] = Query(None, description="e.g. huawei,ericsson,nokia,samsung"),
    expand_missing_dates: bool = Query(True),
    limit: Optional[int] = Query(2000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
):
    df = get_cell_change_data_grouped(
        group_by=group_by,
        site_list=[site_att],
        region_list=regions or [],
        province_list=provinces or [],
        municipality_list=municipalities or [],
        technology_list=technologies or [],
        vendor_list=vendors or [],
    )

    if df is None or df.empty:
        if regions or provinces or municipalities or technologies or vendors or site_att:
            df = create_zero_filled_result(
                group_by=group_by,
                region_list=regions or [],
                province_list=provinces or [],
                municipality_list=municipalities or [],
                site_list=[site_att],
            )
        else:
            return []

    if expand_missing_dates:
        df = expand_dates(df, group_by=group_by)

    # Optional pagination (post-aggregation)
    if limit is not None:
        df = df.iloc[offset : offset + limit]

    # Ensure date serialization and JSON-safe
    if 'date' in df.columns:
        df['date'] = df['date'].astype(str)
    return df_json_records(df)


@router.get("/{site_att}/cqi")
def get_site_cqi(
    site_att: str,
    from_date: Optional[date] = Query(None),
    to_date: Optional[date] = Query(None),
    technology: Optional[str] = Query(None, pattern="^(3G|4G|5G)$"),
    limit: Optional[int] = Query(5000, ge=1, le=100000),
    offset: int = Query(0, ge=0),
):
    df = get_cqi_daily(
        att_name=site_att,
        min_date=str(from_date) if from_date else None,
        max_date=str(to_date) if to_date else None,
        technology=technology,
    )
    if df is None:
        return []
    if limit is not None:
        df = df.iloc[offset : offset + limit]
    return df_json_records(df)


# --- Neighbors Endpoints (M2) ---

@router.get("/{site_att}/neighbors")
def get_neighbors(
    site_att: str,
    radius_km: float = Query(5, ge=0.1, le=50, description="Search radius in km"),
):
    neighbors = get_neighbor_sites(site_att, radius_km=radius_km)
    return {"site_att": site_att, "radius_km": radius_km, "neighbors": neighbors}


@router.get("/{site_att}/neighbors/geo")
def get_neighbors_geo(
    site_att: str,
    radius_km: float = Query(5, ge=0.1, le=50),
):
    """Return center site and neighbors with latitude/longitude."""
    engine = create_connection()
    if engine is None:
        return []
    try:
        radius_meters = radius_km * 1000

        # Detect actual column names in master_node_total
        cols_query = text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'master_node_total'
            """
        )
        with engine.connect() as conn:
            cols = {row[0] for row in conn.execute(cols_query)}

        id_col = 'att_name' if 'att_name' in cols else ('node' if 'node' in cols else None)
        lat_col = 'latitude' if 'latitude' in cols else ('lat_wgs84' if 'lat_wgs84' in cols else None)
        lon_col = 'longitude' if 'longitude' in cols else ('long_wgs84' if 'long_wgs84' in cols else None)

        if not id_col or not lat_col or not lon_col:
            return []

        sql = f"""
            WITH center AS (
                SELECT {id_col} AS id, {lat_col} AS lat, {lon_col} AS lon
                FROM public.master_node_total
                WHERE {id_col} = :site
                AND {lat_col} IS NOT NULL AND {lon_col} IS NOT NULL
            ), neighbors AS (
                SELECT DISTINCT m.{id_col} AS id, m.{lat_col} AS lat, m.{lon_col} AS lon
                FROM public.master_node_total m
                CROSS JOIN center c
                WHERE m.{id_col} IS NOT NULL
                AND m.{lat_col} IS NOT NULL AND m.{lon_col} IS NOT NULL
                AND m.{id_col} <> c.id
                AND ST_DWithin(
                    ST_GeogFromText('POINT(' || c.lon || ' ' || c.lat || ')'),
                    ST_GeogFromText('POINT(' || m.{lon_col} || ' ' || m.{lat_col} || ')'),
                    :radius
                )
            )
            SELECT 'center' AS role, id AS att_name, lat AS latitude, lon AS longitude FROM center
            UNION ALL
            SELECT 'neighbor' AS role, id AS att_name, lat AS latitude, lon AS longitude FROM neighbors
            ORDER BY role DESC, att_name ASC
        """

        df = pd.read_sql_query(text(sql), engine, params={"site": site_att, "radius": radius_meters})
        return df.to_dict(orient="records")
    finally:
        engine.dispose()


@router.get("/{site_att}/neighbors/cqi")
def get_neighbors_cqi(
    site_att: str,
    from_date: Optional[date] = Query(None),
    to_date: Optional[date] = Query(None),
    technology: Optional[str] = Query(None, pattern="^(3G|4G|5G)$"),
    radius_km: float = Query(5, ge=0.1, le=50),
    limit: Optional[int] = Query(5000, ge=1, le=100000),
    offset: int = Query(0, ge=0),
):
    df = get_neighbor_cqi_daily(
        site_list=site_att,
        min_date=str(from_date) if from_date else None,
        max_date=str(to_date) if to_date else None,
        technology=technology,
        radius_km=radius_km,
    )
    if df is None:
        return []
    if limit is not None:
        df = df.iloc[offset : offset + limit]
    return df_json_records(df)


@router.get("/{site_att}/neighbors/traffic")
def get_neighbors_traffic(
    site_att: str,
    from_date: Optional[date] = Query(None),
    to_date: Optional[date] = Query(None),
    technology: Optional[str] = Query(None, pattern="^(3G|4G|5G)$"),
    vendor: Optional[str] = Query(None),
    radius_km: float = Query(5, ge=0.1, le=50),
    limit: Optional[int] = Query(5000, ge=1, le=100000),
    offset: int = Query(0, ge=0),
):
    df = get_neighbor_traffic_data(
        site_list=site_att,
        min_date=str(from_date) if from_date else None,
        max_date=str(to_date) if to_date else None,
        technology=technology,
        radius_km=radius_km,
        vendor=vendor,
    )
    if df is None:
        return []
    if limit is not None:
        df = df.iloc[offset : offset + limit]
    return df_json_records(df)


@router.get("/{site_att}/neighbors/traffic/voice")
def get_neighbors_voice(
    site_att: str,
    from_date: Optional[date] = Query(None),
    to_date: Optional[date] = Query(None),
    technology: Optional[str] = Query(None, pattern="^(3G|4G)$"),
    vendor: Optional[str] = Query(None),
    radius_km: float = Query(5, ge=0.1, le=50),
    limit: Optional[int] = Query(5000, ge=1, le=100000),
    offset: int = Query(0, ge=0),
):
    df = get_neighbor_traffic_voice(
        site_list=site_att,
        min_date=str(from_date) if from_date else None,
        max_date=str(to_date) if to_date else None,
        technology=technology,
        radius_km=radius_km,
        vendor=vendor,
    )
    if df is None:
        return []
    if limit is not None:
        df = df.iloc[offset : offset + limit]
    return df_json_records(df)


@router.get("/{site_att}/traffic")
def get_site_traffic_data(
    site_att: str,
    from_date: Optional[date] = Query(None),
    to_date: Optional[date] = Query(None),
    technology: Optional[str] = Query(None, pattern="^(3G|4G|5G)$"),
    vendor: Optional[str] = Query(None, description="huawei|ericsson|nokia|samsung for 4G; huawei|ericsson|nokia for 3G"),
    limit: Optional[int] = Query(5000, ge=1, le=100000),
    offset: int = Query(0, ge=0),
):
    df = get_traffic_data_daily(
        att_name=site_att,
        min_date=str(from_date) if from_date else None,
        max_date=str(to_date) if to_date else None,
        technology=technology,
        vendor=vendor,
    )
    if df is None:
        return []
    if limit is not None:
        df = df.iloc[offset : offset + limit]
    return df_json_records(df)


@router.get("/{site_att}/traffic/voice")
def get_site_traffic_voice(
    site_att: str,
    from_date: Optional[date] = Query(None),
    to_date: Optional[date] = Query(None),
    technology: Optional[str] = Query(None, pattern="^(3G|4G)$"),
    vendor: Optional[str] = Query(None),
    limit: Optional[int] = Query(5000, ge=1, le=100000),
    offset: int = Query(0, ge=0),
):
    df = get_traffic_voice_daily(
        att_name=site_att,
        min_date=str(from_date) if from_date else None,
        max_date=str(to_date) if to_date else None,
        technology=technology,
        vendor=vendor,
    )
    if df is None:
        return []
    if limit is not None:
        df = df.iloc[offset : offset + limit]
    return df_json_records(df)
