import os
from datetime import datetime
from pathlib import Path

import dagster as dg
import pandas as pd

from kdags.assets.reliability.icc.utils import extract_technical_report_data, parse_filename
from kdags.resources.tidyr import DataLake, MSGraph, MasterData
import polars as pl
from datetime import date


ICC_ANALYTICS_PATH = "az://bhp-analytics-data/RELIABILITY/ICC/icc.parquet"


def get_shift_dates():
    # --- Configuration ---
    start_date = date(2020, 1, 1)
    end_date = date(2030, 12, 31)

    # Reference point: Wednesday, April 9, 2025 starts an 'M' shift week.
    ref_date_wednesday = date(2025, 4, 9)
    ref_shift_label = "M"
    other_shift_label = "N"

    # Get the absolute ordinal day number for the reference Wednesday
    ref_ordinal_wednesday = ref_date_wednesday.toordinal()
    # --- End Configuration ---

    # 1. Generate all dates
    df = pl.DataFrame({"date": pl.date_range(start_date, end_date, interval="1d", eager=True)})

    # 2. Calculate ISO Year and Week
    df = df.with_columns(iso_year=pl.col("date").dt.iso_year(), iso_week=pl.col("date").dt.week())

    # 3. Calculate shift label - Using Mon=1 convention for weekday()

    # 3a. Calculate offset days to subtract to get to previous Wednesday (assuming Wed=3)
    df = df.with_columns(days_to_subtract=(pl.col("date").dt.weekday() - 3 + 7) % 7)

    # 3b. Calculate the shift week start date
    df = df.with_columns(shift_week_start=pl.col("date") - pl.duration(days=pl.col("days_to_subtract")))

    # 3c. Calculate the absolute ordinal for the shift week start date
    df = df.with_columns(
        shift_start_ordinal=pl.col("shift_week_start").map_elements(lambda d: d.toordinal(), return_dtype=pl.Int64)
    )

    # 3d. Calculate week difference using absolute ordinals
    df = df.with_columns(week_difference_ord=(pl.col("shift_start_ordinal") - ref_ordinal_wednesday) // 7)

    # 3e. Assign shift based on ordinal week difference parity
    df = df.with_columns(
        shift=pl.when(pl.col("week_difference_ord") % 2 == 0)
        .then(pl.lit(ref_shift_label))  # Even diff -> 'M'
        .otherwise(pl.lit(other_shift_label))  # Odd diff -> 'N'
    )

    # 4. Select final columns
    final_df = df.select(["date", "iso_year", "iso_week", "shift"])
    return final_df


@dg.asset
def cc_summary(read_component_changeouts):
    taxonomy_df = MasterData.taxonomy()
    df = read_component_changeouts.clone()  # Polars uses clone() instead of copy()

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
def icc(context: dg.AssetExecutionContext, cc_summary, gather_icc_reports):
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

    df = df.join(get_shift_dates(), how="left", left_on="changeout_date", right_on="date").sort(
        "changeout_date", descending=True
    )

    datalake = DataLake()  # Direct instantiation
    context.log.info(f"Writing {df.height} records to {ICC_ANALYTICS_PATH}")

    datalake.upload_tibble(df=df, az_path=ICC_ANALYTICS_PATH, format="parquet")
    context.add_output_metadata(
        {  # Add metadata on success
            "az_path": ICC_ANALYTICS_PATH,
            "rows_written": df.height,
        }
    )

    return df


@dg.asset
def publish_sp_icc(context: dg.AssetExecutionContext, icc: pl.DataFrame):
    df = icc.clone()
    msgraph = MSGraph()
    sp_results = []
    sp_results.extend(msgraph.upload_tibble("sp://KCHCLGR00058/___/CONFIABILIDAD/informes_cambio_componentes.xlsx", df))
    sp_results.extend(
        msgraph.upload_tibble(
            "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/CONFIABILIDAD/informes_cambio_componentes.xlsx",
            df,
        )
    )
    return sp_results


@dg.asset(
    description="Reads the consolidated oil analysis data from the ADLS analytics layer.",
)
def read_icc(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake()
    if dl.az_path_exists(ICC_ANALYTICS_PATH):
        df = dl.read_tibble(az_path=ICC_ANALYTICS_PATH)
        context.log.info(f"Read {df.height} records from {ICC_ANALYTICS_PATH}.")
        return df
    else:
        context.log.warning(f"Data file not found at {ICC_ANALYTICS_PATH}. Returning empty DataFrame.")
        return pl.DataFrame()
