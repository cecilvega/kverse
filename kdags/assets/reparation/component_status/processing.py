import polars as pl
from kdags.resources.tidyr import DataLake
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from io import StringIO
import dagster as dg
from datetime import datetime
import re

from . import constants

COMPONENT_STATUS_ANALYTICS_PATH = "abfs://bhp-analytics-data/REPARATION/COMPONENT_STATUS/component_status.parquet"


@dg.asset(description="Reads raw component status parquet files based on the latest partitions.")  # Changed to dg.asset
# Add context back for logging
def raw_component_status(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """
    Reads the latest partitioned raw component status data.
    Instantiates DataLake directly. Minimal logging.
    """
    datalake = DataLake()  # Direct instantiation
    base_raw_path = "abfs://bhp-raw-data/RESO/COMPONENT_STATUS"

    files_df = datalake.list_paths(base_raw_path)
    if files_df.is_empty():
        context.log.warning(f"No files found in {base_raw_path}")
        return pl.DataFrame()

    parsed_data = []
    for path in files_df["abfs_path"].to_list():
        component_year_match = re.search(r"component_status_(\d{4})\.parquet", path)
        component_year = int(component_year_match.group(1)) if component_year_match else None
        partition_match = re.search(r"y=(\d{4})/m=(\d{2})/d=(\d{2})", path)
        partition_date = datetime(*map(int, partition_match.groups())) if partition_match else None
        if component_year and partition_date:
            parsed_data.append({"abfs_path": path, "component_year": component_year, "partition_date": partition_date})

    if not parsed_data:
        context.log.warning(f"No valid partitioned files found under {base_raw_path}")
        return pl.DataFrame()

    paths_df = pl.DataFrame(parsed_data)
    latest_paths_df = (
        paths_df.group_by("component_year")
        .agg(pl.col("partition_date").max())
        .join(paths_df, on=["component_year", "partition_date"])
    )
    paths_to_read = latest_paths_df["abfs_path"].to_list()

    tibbles = [datalake.read_tibble(abfs_path=p) for p in paths_to_read]
    if not tibbles:
        return pl.DataFrame()

    df = pl.concat(tibbles, how="vertical_relaxed")
    context.log.info(f"Read {len(paths_to_read)} raw files, total rows: {df.height}")
    return df


@dg.asset(description="Selects relevant columns, renames, and performs type conversions.")  # Changed to dg.asset
# Use direct parameter passing for dependency
# Add context back for logging
def mutate_component_status(context: dg.AssetExecutionContext, raw_component_status: pl.DataFrame) -> pl.DataFrame:
    """
    Applies renaming and type casting based on defined constants.
    Uses 'mutate_' prefix as preferred. Clones input. Minimal logging.
    """
    if raw_component_status.is_empty():
        context.log.warning("Input raw_component_status is empty.")
        return pl.DataFrame()

    df = raw_component_status.clone()  # Clone input

    available_raw_cols = [col for col in constants.SELECTED_RAW_COLUMNS if col in df.columns]
    mapping_subset = {k: v for k, v in constants.COLUMN_MAPPING.items() if k in available_raw_cols}

    if len(available_raw_cols) < len(constants.SELECTED_RAW_COLUMNS):
        context.log.warning("Not all expected raw columns were present in the input DataFrame.")

    df = (
        df.select(available_raw_cols)
        .rename(mapping_subset)
        .with_columns(
            [
                pl.col(c)
                .str.split_exact("-", 1)
                .struct.field("field_0")
                .str.split_exact(".", 1)
                .struct.field("field_0")
                .str.to_integer(strict=False)
                .alias(c)
                for c in constants.INT_CONVERSION_COLUMNS
                if c in mapping_subset.values()
            ]
        )
        .with_columns(
            [
                pl.col(c).str.to_date(format="%d/%m/%Y", strict=False).alias(c)
                for c in constants.DATE_CONVERSION_COLUMNS
                if c in mapping_subset.values()
            ]
        )
    )
    context.log.info(f"Mutation complete. Output rows: {df.height}")
    return df


@dg.asset(  # Changed to dg.asset
    description="Writes the final component status data to the ADLS analytics layer.",
)
# Use direct parameter passing for dependency, update name here
# Add context back for logging / metadata
def component_status(context: dg.AssetExecutionContext, mutate_component_status: pl.DataFrame) -> pl.DataFrame:
    """
    Takes the processed data from mutate_component_status and writes it to the final
    analytics path, overwriting any existing file. Instantiates DataLake directly. Minimal logging.
    """
    df = mutate_component_status.clone()  # Use the input parameter name

    if df.is_empty():
        context.log.warning("Received empty DataFrame from mutate_component_status. Skipping write.")
        context.add_output_metadata({"status": "skipped_empty_input", "rows_written": 0})
        return None

    datalake = DataLake()  # Direct instantiation
    context.log.info(f"Writing {df.height} records to {COMPONENT_STATUS_ANALYTICS_PATH}")

    datalake.upload_tibble(df=df, abfs_path=COMPONENT_STATUS_ANALYTICS_PATH, format="parquet")
    context.add_output_metadata(
        {  # Add metadata on success
            "abfs_path": COMPONENT_STATUS_ANALYTICS_PATH,
            "rows_written": df.height,
        }
    )
    return df


@dg.asset(
    description="Reads the consolidated oil analysis data from the ADLS analytics layer.",
)
def read_component_status(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake()
    if dl.abfs_path_exists(COMPONENT_STATUS_ANALYTICS_PATH):
        df = dl.read_tibble(abfs_path=COMPONENT_STATUS_ANALYTICS_PATH)
        context.log.info(f"Read {df.height} records from {COMPONENT_STATUS_ANALYTICS_PATH}.")
        return df
    else:
        context.log.warning(f"Data file not found at {COMPONENT_STATUS_ANALYTICS_PATH}. Returning empty DataFrame.")
        return pl.DataFrame()
