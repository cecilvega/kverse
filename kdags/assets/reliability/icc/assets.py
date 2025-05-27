import os
from datetime import date
from datetime import datetime
from pathlib import Path

import dagster as dg
import polars as pl

from kdags.assets.reliability.icc.utils import parse_filename
from kdags.config import DATA_CATALOG
from kdags.resources.tidyr import DataLake
from kdags.resources.tidyr import MasterData


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
        icc_date=(pl.col("iso_year").cast(pl.String) + "-W" + pl.col("iso_week").cast(pl.String).str.zfill(2)),
        work_shift=pl.when(pl.col("week_difference_ord") % 2 == 0)
        .then(pl.lit(ref_shift_label))  # Even diff -> 'M'
        .otherwise(pl.lit(other_shift_label)),  # Odd diff -> 'N'
    )

    # 4. Select final columns
    final_df = df.select(["date", "icc_date", "work_shift"])
    return final_df


def filter_cc_icc(component_changeouts):
    taxonomy_df = MasterData.taxonomy()
    components_df = (
        MasterData.components()
        .filter(pl.col("subcomponent_main").is_not_null())
        .select(["component_name", "subcomponent_name"])
    )
    df = component_changeouts.clone()

    # Filter rows
    df = df.filter(pl.col("changeout_date") >= datetime(2024, 9, 26))

    # Get component_name and subcomponent_name from component_code and position_code
    df = df.join(taxonomy_df, on=["component_name", "subcomponent_name", "position_name"], how="left").drop_nulls(
        subset=["component_code"]
    )

    df = df.with_columns(
        [
            pl.col("position_code").cast(pl.Int64),
            pl.col("equipment_hours").cast(pl.Float64).round(0).cast(pl.Int64),
            pl.col("component_hours").cast(pl.Float64).round(0).fill_null(-1).cast(pl.Int64),
        ]
    )

    # Sort and drop duplicates
    df = df.join(components_df, how="inner", on=["component_name", "subcomponent_name"])

    df = df.select(
        [
            "equipment_name",
            "component_name",
            "component_code",
            "position_name",
            "position_code",
            "changeout_date",
            "failure_description",
        ]
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

            # Desabilitando la extracción de información
            # # Only extract report data if it's a PDF file
            # if file_path.suffix.lower() == ".pdf":
            #     i_data = extract_technical_report_data(file_path)

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
                # "equipment_hours",
                # "report_date",
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


@dg.asset(group_name="reliability")
def mutate_icc(context: dg.AssetExecutionContext, component_changeouts, gather_icc_reports):
    cc_summary = filter_cc_icc(component_changeouts)
    df = cc_summary.join(
        gather_icc_reports,
        on=[
            "equipment_name",
            "component_code",
            "position_code",
            "changeout_date",
        ],
        how="outer",
    ).sort("changeout_date")

    df = df.join(get_shift_dates(), how="left", left_on="changeout_date", right_on="date").sort(
        "changeout_date", descending=True
    )
    df = df.select(
        [
            "icc_date",
            "work_shift",
            # Equipment identification
            "equipment_name",
            # Component information
            "component_name",
            "component_code",
            "position_name",
            "position_code",
            # Dates and events
            # "report_date",
            "changeout_date",
            # Work order information
            # "customer_work_order",
            # "icc_number",
            # File metadata
            "filename",
            "file_type",
            # Additional details
            "failure_description",
        ]
    )

    datalake = DataLake(context)

    datalake.upload_tibble(tibble=df, az_path=DATA_CATALOG["icc"]["analytics_path"])

    return df


@dg.asset(
    group_name="readr",
    description="Reads the consolidated oil analysis data from the ADLS analytics layer.",
)
def icc(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake(context)
    df = dl.read_tibble(az_path=DATA_CATALOG["icc"]["analytics_path"])
    return df
