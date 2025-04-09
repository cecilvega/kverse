import polars as pl
from kdags.resources.tidyr import DataLake
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from io import StringIO
import dagster as dg
from datetime import datetime
import re

from .constants import *

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

    available_raw_cols = [col for col in SELECTED_RAW_COLUMNS if col in df.columns]
    mapping_subset = {k: v for k, v in COLUMN_MAPPING.items() if k in available_raw_cols}

    if len(available_raw_cols) < len(SELECTED_RAW_COLUMNS):
        context.log.warning("Not all expected raw columns were present in the input DataFrame.")

    # 1. Load initial data and add a unique ID BEFORE filtering
    df = df.with_row_count("original_row_id")  # Add index

    # 2. Prepare column lists and mappings based on available columns
    available_raw_cols = [col for col in SELECTED_RAW_COLUMNS if col in df.columns]
    mapping_subset = {k: v for k, v in COLUMN_MAPPING.items() if k in available_raw_cols}

    # Get the final column names AFTER mapping/renaming
    final_int_cols = [v for k, v in mapping_subset.items() if v in INT_CONVERSION_COLUMNS]
    final_date_cols = [v for k, v in mapping_subset.items() if v in DATE_CONVERSION_COLUMNS]
    final_service_order_col = mapping_subset.get("YourRawServiceOrderColumnName", "service_order")  # Get final name

    # 3. Store original string values for comparison later
    # Select the ID and the raw columns that will be converted/renamed
    original_cols_to_keep = ["original_row_id"] + available_raw_cols
    df_original_strings = (
        df.select(original_cols_to_keep).rename(mapping_subset)  # Apply mapping to match final names
        # Rename columns to avoid name clash during join - ADD SUFFIX
        .rename({col: f"{col}_original_str" for col in mapping_subset.values()})
    )

    # 4. Run the main processing pipeline
    # *Use strict=False for service_order DURING analysis*
    df_processed = (
        df.filter(pl.col("Location").str.contains("ESCONDIDA"))  # Start with original data + row id
        .select(["original_row_id"] + available_raw_cols)  # Keep row id
        .rename(mapping_subset)
        .with_columns(
            # Use strict=False for analysis to avoid stopping; use True in production if needed
            service_order=pl.col(final_service_order_col).str.to_integer(strict=False)
        )
        .with_columns(  # Other Integer Conversions
            [
                pl.col(c)
                .str.split_exact("-", 1)
                .struct.field("field_0")
                .str.split_exact(".", 1)
                .struct.field("field_0")
                .str.to_integer(strict=False)  # Keep strict=False
                .alias(c)
                for c in final_int_cols
                # Ensure not reprocessing service_order if it's in this list
                if c != final_service_order_col
            ]
        )
        .with_columns(  # Date Conversions (using your split + coalesce method)
            [
                pl.coalesce(
                    pl.col(c)
                    .str.split(" ")
                    .list.get(0)
                    .str.strptime(pl.Date, format="%m/%d/%Y", strict=False),  # Keep strict=False
                    pl.col(c)
                    .str.split(" ")
                    .list.get(0)
                    .str.strptime(pl.Date, format="%d-%m-%Y", strict=False),  # Keep strict=False
                ).alias(c)
                for c in final_date_cols
            ]
        )
    )

    # 5. Perform Validation: Join processed data with original strings
    df_comparison = df_processed.join(
        df_original_strings.filter(pl.col("original_row_id").is_in(df_processed["original_row_id"])),
        on="original_row_id",
    )

    # 6. Find and Report Failures

    print("\n--- Conversion Validation Report ---")
    conversion_failures = {}

    # Check Service Order Integer Conversion
    original_so_col = f"{final_service_order_col}_original_str"
    so_failures = df_comparison.filter(
        pl.col(original_so_col).is_not_null()
        & pl.col(final_service_order_col).is_null()
        & (pl.col(original_so_col) != "")  # Check original wasn't null/empty
    )
    if not so_failures.is_empty():
        print(f"\n[WARNING] Found {len(so_failures)} rows where '{final_service_order_col}' integer parsing failed:")
        print(so_failures.select("original_row_id", original_so_col, final_service_order_col).head())
        conversion_failures[final_service_order_col] = so_failures

    # Check other Integer Conversions
    for col in final_int_cols:
        if col == final_service_order_col:
            continue
        original_col = f"{col}_original_str"
        processed_col = col
        failures = df_comparison.filter(
            pl.col(original_col).is_not_null() & pl.col(processed_col).is_null() & (pl.col(original_col) != "")
        )
        if not failures.is_empty():
            print(f"\n[WARNING] Found {len(failures)} rows where '{processed_col}' integer parsing failed:")
            print(failures.select("original_row_id", original_col, processed_col).head())
            conversion_failures[processed_col] = failures

    # Check Date Conversions
    for col in final_date_cols:
        original_col = f"{col}_original_str"
        processed_col = col
        failures = df_comparison.filter(
            pl.col(original_col).is_not_null() & pl.col(processed_col).is_null() & (pl.col(original_col) != "")
        )
        if not failures.is_empty():
            print(
                f"\n[WARNING] Found {len(failures)} rows where '{processed_col}' date parsing failed (unhandled format?):"
            )
            print(failures.select("original_row_id", original_col, processed_col).head())
            conversion_failures[processed_col] = failures

    if not conversion_failures:
        print("\nNo conversion failures detected (original non-null values -> converted null values).")

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
