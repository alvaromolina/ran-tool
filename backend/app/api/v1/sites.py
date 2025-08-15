from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Query

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

router = APIRouter(prefix="/sites", tags=["sites"])


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

    # Ensure date serialization
    if 'date' in df.columns:
        df['date'] = df['date'].astype(str)

    return df.to_dict(orient='records')


@router.get("/{site_att}/cqi")
def get_site_cqi(
    site_att: str,
    from_date: Optional[date] = Query(None),
    to_date: Optional[date] = Query(None),
    technology: Optional[str] = Query(None, pattern="^(3G|4G|5G)$"),
):
    df = get_cqi_daily(
        att_name=site_att,
        min_date=str(from_date) if from_date else None,
        max_date=str(to_date) if to_date else None,
        technology=technology,
    )
    if df is None:
        return []
    if 'time' in df.columns:
        df['time'] = df['time'].astype(str)
    return df.to_dict(orient='records')


@router.get("/{site_att}/traffic")
def get_site_traffic_data(
    site_att: str,
    from_date: Optional[date] = Query(None),
    to_date: Optional[date] = Query(None),
    technology: Optional[str] = Query(None, pattern="^(3G|4G|5G)$"),
    vendor: Optional[str] = Query(None, description="huawei|ericsson|nokia|samsung for 4G; huawei|ericsson|nokia for 3G"),
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
    if 'time' in df.columns:
        df['time'] = df['time'].astype(str)
    return df.to_dict(orient='records')


@router.get("/{site_att}/traffic/voice")
def get_site_traffic_voice(
    site_att: str,
    from_date: Optional[date] = Query(None),
    to_date: Optional[date] = Query(None),
    technology: Optional[str] = Query(None, pattern="^(3G|4G)$"),
    vendor: Optional[str] = Query(None),
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
    if 'time' in df.columns:
        df['time'] = df['time'].astype(str)
    return df.to_dict(orient='records')
