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
    months_lookback: int = 2,
) -> pl.DataFrame:
    """
    Reads raw oil analysis Excel files from Azure Data Lake.

    Args:
        context: Dagster execution context
        only_recent: If True, only processes files from recent months
        months_lookback: Number of months to look back if only_recent is True


    Returns:
        DataFrame containing the raw oil analysis data
    """
    dl = DataLake()
    uri = "abfs://bhp-raw-data/LUBE_ANALYST/SCAAE"
    # List all files in the specified path
    files_df = dl.list_paths(uri="abfs://bhp-raw-data/LUBE_ANALYST/SCAAE")
    context.log.info(f"Found {len(files_df)} files in {uri}")

    # Filter to only Excel files
    excel_mask = files_df["file_path"].str.ends_with(".xls") | files_df["file_path"].str.ends_with(".xlsx")
    files_df = files_df.filter(excel_mask)
    context.log.info(f"Found {len(files_df)} Excel files")

    # If only recent files are needed, filter based on date in filename
    if only_recent:
        cutoff_date = datetime.now() - timedelta(days=30 * months_lookback)

        # Extract dates from filenames (assuming YYYYMMDD format at the start)
        dates = []
        file_paths = []

        for file_path in files_df["file_path"]:
            filename = os.path.basename(file_path)
            # Try to extract date from filename
            if len(filename) >= 8 and filename[:8].isdigit():
                try:
                    file_date = datetime.strptime(filename[:8], "%Y%m%d")
                    if file_date >= cutoff_date:
                        dates.append(file_date)
                        file_paths.append(file_path)
                except ValueError:
                    # Skip files with invalid date format
                    pass

        context.log.info(f"Filtered to {len(file_paths)} files from the last {months_lookback} months")
    else:
        file_paths = files_df["file_path"].to_list()

    # Read all selected files
    all_dfs = []

    for file_path in file_paths:
        # Get file metadata
        file_name = os.path.basename(file_path)
        date_str = file_name[:8] if len(file_name) >= 8 and file_name[:8].isdigit() else None
        df = dl.read_tibble(urei=file_path, use_polars=True, infer_schema_length=0)

        # Add metadata columns
        if date_str:
            df = df.with_columns(
                [
                    pl.lit(file_path).alias("file_path"),
                    pl.lit(date_str).str.strptime(pl.Date, format="%Y%m%d").alias("file_date"),
                ]
            )
        else:
            df = df.with_columns(
                [
                    pl.lit(file_path).alias("file_path"),
                    pl.lit(None).cast(pl.Date).alias("file_date"),
                ]
            )

        all_dfs.append(df)
        context.log.info(f"Read {file_path}: {df.height} rows")

    # Handle the case of no files found
    if not all_dfs:
        context.log.warning("No files were found or successfully processed")
        # Return an empty dataframe with expected schema
        return pl.DataFrame(schema={"file_path": pl.Utf8, "file_date": pl.Date})

    # Concatenate all DataFrames
    result_df = pl.concat(all_dfs)
    context.log.info(f"Total rows: {result_df.height}")

    return result_df
