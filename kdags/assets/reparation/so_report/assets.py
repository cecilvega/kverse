import os
import re
from datetime import date
from datetime import datetime

import dagster as dg
import polars as pl

from kdags.config import DATA_CATALOG
from kdags.resources.tidyr import DataLake
from .constants import *


def get_year_and_new_path(az_path: str, datalake_instance):

    print(f"Processing: {az_path}")

    df_excel = datalake_instance.read_tibble(
        az_path,
        columns=["Reception Date"],
        # sheet_name='YourSheet' # Add if needed
    )

    if not isinstance(df_excel["Reception Date"].dtype, (pl.Date, pl.Datetime)):
        df_excel = df_excel.with_columns(pl.col("Reception Date").str.strptime(pl.Datetime, strict=False).cast(pl.Date))

    unique_years = df_excel.get_column("Reception Date").dt.year().drop_nulls().unique().to_list()

    # === Condition Check ===
    # Check the logical condition: exactly one unique year
    if len(unique_years) == 1:
        reception_year = unique_years[0]
        print(f"  -> Found unique reception year: {reception_year}")

        # === Path Generation ===
        dir_path = os.path.dirname(az_path)
        original_filename = os.path.basename(az_path)

        # Regex to replace content of the last parentheses before .xlsx
        new_filename, n_subs = re.subn(
            r"\([^)]*\)(\.xlsx)$",
            f"({reception_year})\\1",
            original_filename,
            flags=re.IGNORECASE,
        )

        # Check if regex substitution worked - raise error if it failed unexpectedly
        if n_subs != 1:
            raise RuntimeError(
                f"Regex substitution failed for filename '{original_filename}' in path {az_path}. Pattern not found as expected."
            )

        new_az_path = f"{dir_path}/{new_filename}"
        print(f"  -> Proposed new path: {new_az_path}")
        return new_az_path
    else:
        raise ValueError(f"Expected exactly one unique year, found {unique_years}")


def fix_naming():
    dl = DataLake()
    base_raw_path = "az://bhp-raw-data/RESO/SERVICE_ORDER_REPORT"

    files_df = dl.list_paths(base_raw_path)
    files_df = files_df.with_columns(
        partition_date=pl.col("az_path")
        .str.extract(r"(y=\d{4}/m=\d{2}/d=\d{2})", 1)
        .str.strptime(pl.Date, format="y=%Y/m=%m/d=%d", strict=False),
        component_year=pl.col("az_path").str.extract(r"\((\d{4})\)", 1).cast(pl.Int32, strict=False),
    )
    to_rename_files = files_df.filter(pl.col("component_year").is_null())["az_path"].to_list()
    if to_rename_files:
        for az_path in to_rename_files:
            new_az_path = get_year_and_new_path(az_path, dl)
            dl.rename_file(
                source_az_path=az_path,
                destination_az_path=new_az_path,
            )


@dg.asset(group_name="reparation")
# Add context back for logging
def raw_so_report(context: dg.AssetExecutionContext) -> pl.DataFrame:
    fix_naming()
    datalake = DataLake(context=context)  # Direct instantiation
    base_raw_path = "az://bhp-raw-data/RESO/SERVICE_ORDER_REPORT"

    files_df = datalake.list_paths(base_raw_path)

    parsed_data = []
    for path in files_df["az_path"].to_list():
        component_year_match = re.search(r"\((\d{4})\)", path)
        component_year = int(component_year_match.group(1)) if component_year_match else None
        partition_match = re.search(r"y=(\d{4})/m=(\d{2})/d=(\d{2})", path)
        partition_date = datetime(*map(int, partition_match.groups())) if partition_match else None
        if component_year and partition_date:
            parsed_data.append({"az_path": path, "component_year": component_year, "partition_date": partition_date})

    paths_df = pl.DataFrame(parsed_data)
    latest_paths_df = (
        paths_df.group_by("component_year")
        .agg(pl.col("partition_date").max())
        .join(paths_df, on=["component_year", "partition_date"])
    )
    paths_to_read = latest_paths_df["az_path"].to_list()

    tibbles = [
        datalake.read_tibble(az_path=p, columns=SELECTED_RAW_COLUMNS, infer_schema_length=0) for p in paths_to_read
    ]
    if not tibbles:
        return pl.DataFrame()

    df = pl.concat(tibbles, how="vertical_relaxed")
    context.log.info(f"Read {len(paths_to_read)} raw files, total rows: {df.height}")
    return df


def process_so_report(context: dg.AssetExecutionContext, raw_so_report: pl.DataFrame) -> pl.DataFrame:

    df = raw_so_report.clone()

    available_raw_cols = [col for col in SELECTED_RAW_COLUMNS if col in df.columns]

    # 1. Load initial data and add a unique ID BEFORE filtering
    df = df.with_row_index("original_row_id")  # Add index

    mapping_subset = {k: v for k, v in COLUMN_MAPPING.items()}

    # 4. Run the main processing pipeline
    # *Use strict=False for service_order DURING analysis*
    df = (
        df.select(["original_row_id"] + available_raw_cols)  # Keep row id
        .rename(mapping_subset)
        .with_columns(sap_equipment_name=pl.col("sap_equipment_name").str.strip_suffix(".0"))
        # int conversion
        .with_columns(
            [
                pl.col(c)
                .str.split_exact("-", 1)
                .struct.field("field_0")
                .str.split_exact(".", 1)
                .struct.field("field_0")
                .str.to_integer(strict=False)
                .replace(0, -1)
                .fill_null(-1)
                .alias(c)
                for c in INT_CONVERSION_COLUMNS
            ]
        )
        # date conversion
        .with_columns(
            [
                pl.col(c).str.split(" ").list.get(0).str.strptime(pl.Date, format="%Y-%m-%d", strict=False).alias(c)
                for c in DATE_CONVERSION_COLUMNS
            ]
        )
        .with_columns(reso_component_serial=pl.col("component_serial"))
        .with_columns(
            [
                pl.col(c).str.strip_chars().str.replace("\t", "").str.replace_all("#", "")
                # .str.split(" ")
                # .list.first()
                .alias(c)
                for c in ["component_serial"]
            ]
        )
    )

    df = df.with_columns(update_date=pl.lit(date.today()))

    context.log.info(f"Mutation complete. Output rows: {df.height}")
    return df


@dg.asset(group_name="reparation")
def mutate_so_report(context: dg.AssetExecutionContext, raw_so_report: pl.DataFrame) -> pl.DataFrame:

    df = raw_so_report.clone()  # Use the input parameter name
    df = process_so_report(context, df)

    df = df.with_columns(
        load_preliminary_report_date=pl.col("load_preliminary_report_in_review").fill_null(
            pl.col("load_preliminary_report_revised")
        ),
        load_final_report_date=pl.col("load_final_report_in_review").fill_null(pl.col("load_final_report_revised")),
        component_status=pl.col("component_status").replace({"Delivery Proccess": "Delivered"}),
    )

    # Reparar serie componentes
    df = df.with_columns(component_serial=pl.col("component_serial").str.replace(r"\s*\([^)]*\)", ""))

    df = (
        df.sort("reception_date")
        .filter((pl.col("service_order") != -1))
        .unique(
            subset=["service_order", "main_component", "component_serial", "sap_equipment_name"],
            keep="last",
            maintain_order=True,
        )
    )

    assert (
        df.filter(
            pl.struct(["service_order", "main_component", "component_serial", "sap_equipment_name"]).is_duplicated()
        ).height
        == 0
    )

    datalake = DataLake(context=context)
    datalake.upload_tibble(tibble=df, az_path=DATA_CATALOG["so_report"]["analytics_path"])

    return df


@dg.asset(group_name="readr")
def so_report(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake(context=context)
    df = dl.read_tibble(az_path=DATA_CATALOG["so_report"]["analytics_path"])
    return df
