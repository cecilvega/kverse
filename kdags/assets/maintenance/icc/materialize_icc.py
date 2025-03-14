import os
from datetime import datetime
from pathlib import Path

import dagster as dg
import pandas as pd

from kdags.assets.maintenance.icc.utils import extract_technical_report_data, parse_filename
from kdags.config.masterdata import MasterData
from kdags.resources import DataLake


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
        for f in (Path(os.environ["ONEDRIVE_LOCAL_PATH"]) / "INFORMES_CAMBIO_DE_COMPONENTE").rglob("*")
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
    reconciled_icc.to_excel(Path(os.environ["ONEDRIVE_LOCAL_PATH"]) / "icc.xlsx", index=False)
    file_url = (
        "https://globalkomatsu.sharepoint.com/sites/KCHCLSP00022/Shared%20Documents/"
        "01.%20%C3%81REAS%20KCH/1.6%20CONFIABILIDAD/CAEX/ANTECEDENTES/MAINTENANCE/ICC/icc.xlsx"
    )

    result["sharepoint"] = {"file_url": file_url, "format": "excel"}

    # 2. Upload to Data Lake as Parquet
    datalake = DataLake()
    datalake_path = datalake.upload_tibble(
        uri="abfs://bhp-analytics-data/MAINTENANCE/ICC/icc.parquet",
        df=reconciled_icc,
        format="parquet",
    )
    result["datalake"] = {"path": datalake_path, "format": "parquet"}

    # Add record count
    result["count"] = len(reconciled_icc)

    return result
