import os
from datetime import datetime
from pathlib import Path

import dagster as dg
import pandas as pd

from kdags.assets.reliability.icc.utils import extract_technical_report_data, parse_filename
from kdags.resources.tidyr import DataLake, MSGraph, MasterData
import polars as pl


# @dg.asset
# def cc_summary(read_cc):
#     taxonomy_df = MasterData.taxonomy()
#     df = read_cc.copy()
#     df = df.loc[df["changeout_date"] >= datetime(2024, 9, 26)]
#     df = pd.merge(
#         df,
#         taxonomy_df,
#         how="left",
#         on=["component_name", "subcomponent_name", "position_name"],
#         validate="m:1",
#     ).dropna(subset=["component_code"])
#     df = df.assign(
#         position_code=df["position_code"].astype(int),
#         equipment_hours=pd.to_numeric(df["equipment_hours"]).round(0).astype(int),
#         component_hours=pd.to_numeric(df["component_hours"]).round(0).fillna(-1).astype(int),
#     )
#     df = df.sort_values(
#         [
#             "changeout_date",
#             "equipment_name",
#             "component_name",
#             "subcomponent_name",
#         ]
#     ).drop_duplicates(["equipment_name", "component_name", "changeout_date", "component_hours"])
#
#     return df


@dg.asset
def cc_summary(read_cc):
    taxonomy_df = MasterData.taxonomy()
    df = read_cc.clone()  # Polars uses clone() instead of copy()

    # Filter rows
    df = df.filter(pl.col("changeout_date") >= datetime(2024, 9, 26))

    # Join with taxonomy_df
    df = df.join(taxonomy_df, on=["component_name", "subcomponent_name", "position_name"], how="left").drop_nulls(
        subset=["component_code"]
    )

    # Assign new values to columns
    df = df.with_columns(
        [
            pl.col("position_code").cast(pl.Int64),
            pl.col("equipment_hours").cast(pl.Float64).round(0).cast(pl.Int64),
            pl.col("component_hours").cast(pl.Float64).round(0).fill_null(-1).cast(pl.Int64),
        ]
    )

    # Sort and drop duplicates
    df = df.sort(["changeout_date", "equipment_name", "component_name", "subcomponent_name"]).unique(
        subset=["equipment_name", "component_name", "changeout_date", "component_hours"], keep="first"
    )

    return df


@dg.asset
def gather_icc_reports(context: dg.AssetExecutionContext):
    # Find all PDF and DOCX files
    icc_files_all = [
        f
        for f in (Path(os.environ["ONEDRIVE_LOCAL_PATH"]) / "INFORMES_CAMBIO_DE_COMPONENTE").rglob("*")
        if ((f.is_file()) & (f.suffix.lower() in [".pdf", ".docx"]) & (f.stem.lower().startswith("icc")))
    ]

    # Group files by stem (filename without extension)
    files_by_stem = {}
    for file_path in icc_files_all:
        stem = file_path.stem.lower()
        if stem not in files_by_stem:
            files_by_stem[stem] = []
        files_by_stem[stem].append(file_path)

    # For each group, prioritize PDF over DOCX
    icc_files = []
    for stem, files in files_by_stem.items():
        pdf_files = [f for f in files if f.suffix.lower() == ".pdf"]
        if pdf_files:
            # If PDF exists, use only the PDF file(s)
            icc_files.extend(pdf_files)
        else:
            # If no PDF, use the DOCX file(s)
            icc_files.extend([f for f in files if f.suffix.lower() == ".docx"])

    data = []
    for file_path in icc_files:
        try:
            i_data = {}

            # Only extract report data if it's a PDF file
            if file_path.suffix.lower() == ".pdf":
                i_data = extract_technical_report_data(file_path)

            # Parse filename for both PDF and DOCX files
            i_data = {**i_data, **parse_filename(file_path)}

            # Add file type to the data
            i_data["file_type"] = file_path.suffix.lower().replace(".", "")

            data.append(i_data)
        except Exception as e:
            context.log.error(file_path)
            raise e

    # Create Polars DataFrame from list of dictionaries
    icc_df = (
        pl.from_dicts(data)
        .select(
            [
                "equipment_name",
                "equipment_hours",
                "report_date",
                "changeout_date",
                "filename",
                "icc_number",
                "component_code",
                "position_code",
                "filepath",
                "file_type",  # Added file_type column
            ]
        )
        .with_columns(changeout_date=pl.col("changeout_date").cast(pl.Date))
    )

    return icc_df


@dg.asset
def reconciled_icc(cc_summary, gather_icc_reports):
    df = (
        cc_summary.select(
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
        )
        .join(
            gather_icc_reports.rename({"equipment_hours": "equipment_hours_icc"}),
            on=[
                "equipment_name",
                "component_code",
                "position_code",
                "changeout_date",
            ],
            how="outer",
        )
        .sort("changeout_date")
        .select(
            [
                # Equipment identification
                "equipment_name",
                "equipment_hours",
                "equipment_hours_icc",
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
                "file_type",
                # Additional details
                "failure_description",
            ]
        )
    )
    return df


@dg.asset
def spawn_icc(reconciled_icc):
    """
    Exports reconciled component reports to both SharePoint (Excel) and Data Lake (Parquet).

    Args:
        reconciled_icc: DataFrame containing reconciled ICC data

    Returns:
        dict: Information about both export operations
    """
    result = {}

    # 1. Upload to SharePoint as Excel
    sharepoint_result = MSGraph().upload_tibble(
        site_id="KCHCLSP00022",
        filepath="/01. ÁREAS KCH/1.6 CONFIABILIDAD/CAEX/ANTECEDENTES/RELIABILITY/ICC/icc.xlsx",
        df=reconciled_icc,
        format="excel",
    )
    result["sharepoint"] = {"file_url": sharepoint_result.web_url, "format": "excel"}

    # 2. Upload to Data Lake as Parquet
    datalake = DataLake()
    datalake_path = datalake.upload_tibble(
        uri="abfs://bhp-analytics-data/RELIABILITY/ICC/icc.parquet",
        df=reconciled_icc,
        format="parquet",
    )
    result["datalake"] = {"path": datalake_path, "format": "parquet"}

    # Add record count
    result["count"] = reconciled_icc.shape[0]

    return result


@dg.asset
def read_icc():
    dl = DataLake()

    uri = "abfs://bhp-analytics-data/RELIABILITY/ICC/icc.parquet"
    if dl.uri_exists(uri):
        return dl.read_tibble(uri=uri)
    else:
        return pl.DataFrame()
