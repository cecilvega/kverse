from datetime import datetime

import dagster as dg
import pandas as pd

from kdags.config.masterdata import MasterData
from pathlib import Path
from kdags.assets.maintenance.icc import extract_technical_report_data, parse_filename
from kdags.resources import MSGraph, DataLake
from io import BytesIO


@dg.asset
def cc_summary(read_raw_cc):
    taxonomy_df = MasterData.taxonomy()
    df = read_raw_cc.copy()
    df = df.loc[df["changeout_date"] >= datetime(2024, 9, 26)]
    df = pd.merge(
        df,
        taxonomy_df,
        how="left",
        on=["component_name", "subcomponent_name", "position_name"],
        validate="m:1",
    ).dropna(subset=["component_code"])
    df = df.assign(
        position_code=df["position_code"].astype(int),
        equipment_hours=pd.to_numeric(df["equipment_hours"]).round(0).astype(int),
        component_hours=pd.to_numeric(df["component_hours"]).round(0).fillna(-1).astype(int),
    )
    df = df.sort_values(
        [
            "changeout_date",
            "equipment_name",
            "component_name",
            "subcomponent_name",
        ]
    ).drop_duplicates(["equipment_name", "component_name", "changeout_date", "component_hours"])

    return df


@dg.asset
def gather_icc_reports():
    icc_files = [
        f
        for f in Path(r"C:\Users\andmn\OneDrive - Komatsu Ltd\INFORMES_CAMBIO_DE_COMPONENTE").rglob("*")
        if ((f.is_file()) & (f.suffix == ".pdf"))
    ]
    data = []
    for i in icc_files:
        try:
            i_data = extract_technical_report_data(i)
            i_data = {**i_data, **parse_filename(i)}
            data.append(i_data)
        except Exception as e:
            print(i)
            raise e
    icc_df = pd.DataFrame(data)[
        [
            "equipment_name",
            "equipment_hours",
            "report_date",
            "changeout_date",
            "filename",
            "icc_number",
            "component_code",
            "position_code",
            "file_path",
        ]
    ]
    return icc_df


@dg.asset
def reconciled_icc(cc_summary, gather_icc_reports):
    df = pd.merge(
        gather_icc_reports,
        cc_summary[
            [
                "equipment_name",
                "component_code",
                "position_code",
                "changeout_date",
                "customer_work_order",
                "equipment_hours",
                "component_name",
                "position_name",
                "failure_description",
            ]
        ],
        on=[
            "equipment_name",
            "component_code",
            "position_code",
            "changeout_date",
        ],
        how="outer",
        suffixes=("_icc", "_plan"),
    ).sort_values("changeout_date")[
        [
            # Equipment identification
            "equipment_name",
            "equipment_hours_icc",
            "equipment_hours_plan",
            # Component information
            "component_name",
            "component_code",
            "position_name",
            "position_code",
            # Dates and events
            "report_date",
            "changeout_date",
            # Work order information
            "customer_work_order",
            # "icc_number",
            # File metadata
            "filename",
            # Additional details
            "failure_description",
        ]
    ]
    return df


@dg.asset
def materialize_icc(reconciled_icc):
    """
    Exports reconciled component reports to both SharePoint (Excel) and Data Lake (Parquet).

    Args:
        reconciled_icc: DataFrame containing reconciled ICC data

    Returns:
        dict: Information about both export operations
    """
    result = {}

    # 1. Upload to SharePoint as Excel
    msgraph = MSGraph()
    sharepoint_result = msgraph.upload_tibble(
        site_id="KCHCLSP00022",
        file_path="/01. ÁREAS KCH/1.6 CONFIABILIDAD/CAEX/ANTECEDENTES/MANTENIMIENTO/ICC/icc.xlsx",
        df=reconciled_icc,
        format="excel",
    )
    result["sharepoint"] = {"file_url": sharepoint_result.web_url, "format": "excel"}

    # 2. Upload to Data Lake as Parquet
    datalake = DataLake()
    datalake_path = datalake.upload_tibble(
        container="kcc-analytics-data", file_path="BHP/MAINTENANCE/ICC/icc.parquet", df=reconciled_icc, format="parquet"
    )
    result["datalake"] = {"path": datalake_path, "format": "parquet"}

    # Add record count
    result["count"] = len(reconciled_icc)

    return result
