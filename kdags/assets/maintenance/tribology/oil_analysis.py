import os
from io import BytesIO
from datetime import datetime, timedelta
from typing import Optional, List
import polars as pl
import dagster as dg
from kdags.resources import DataLake


@dg.asset
def read_raw_oil_analysis(
    context: dg.AssetExecutionContext,
    only_recent: bool = True,
    days_lookback: int = 60,
) -> pl.DataFrame:
    """
    Reads raw oil analysis Excel files from Azure Data Lake.

    Args:
        context: Dagster execution context



    Returns:
        DataFrame containing the raw oil analysis data
    """
    dl = DataLake()
    uri = "abfs://bhp-raw-data/LUBE_ANALYST/SCAAE"
    # List all files in the specified path
    files_df = dl.list_partitioned_paths(
        "abfs://bhp-raw-data/LUBE_ANALYST/SCAAE",
        only_recent=only_recent,
        days_lookback=days_lookback,
    )
    context.log.info(f"Found {len(files_df)} files in {uri}")

    file_paths = files_df["uri"].to_list()

    # Read all selected files
    all_dfs = []

    for file_path in file_paths:
        # Get file metadata
        file_name = os.path.basename(file_path)
        date_str = file_name[:8] if len(file_name) >= 8 and file_name[:8].isdigit() else None
        df = dl.read_tibble(uri=file_path, include_uri=True, use_polars=True, infer_schema_length=0)

        # Add metadata columns
        if date_str:
            df = df.with_columns(
                pl.lit(date_str).str.strptime(pl.Date, format="%Y%m%d").alias("file_date"),
            )
        else:
            df = df.with_columns(
                pl.lit(None).cast(pl.Date).alias("file_date"),
            )

        all_dfs.append(df)
        # context.log.info(f"Read {file_path}: {df.height} rows")

    # Handle the case of no files found
    if not all_dfs:
        context.log.warning("No files were found or successfully processed")
        # Return an empty dataframe with expected schema
        return pl.DataFrame(schema={"uri": pl.Utf8, "file_date": pl.Date})

    # Concatenate all DataFrames
    result_df = pl.concat(all_dfs)
    context.log.info(f"Total rows: {result_df.height}")

    return result_df
