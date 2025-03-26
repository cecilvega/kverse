import os

import dagster as dg
import polars as pl

from kdags.resources.dplyr import upsert_tibbles
from kdags.resources.tidyr import DataLake, MSGraph


class ReadRawOilAnalysisConfig(dg.Config):
    only_recent: bool = False
    days_lookback: int = 30


@dg.asset
def read_raw_oil_analysis(
    context: dg.AssetExecutionContext,
    config: ReadRawOilAnalysisConfig,
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
        only_recent=config.only_recent,
        days_lookback=config.days_lookback,
    )
    context.log.info(f"Found {len(files_df)} files in {uri}")

    filepaths = files_df["uri"].to_list()

    # Read all selected files
    all_dfs = []

    for filepath in filepaths:
        # Get file metadata
        filename = os.path.basename(filepath)
        date_str = filename[:8] if len(filename) >= 8 and filename[:8].isdigit() else None
        df = dl.read_tibble(uri=filepath, include_uri=True, use_polars=True, infer_schema_length=0)

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

    # Handle the case of no files found
    if not all_dfs:
        context.log.warning("No files were found or successfully processed")
        # Return an empty dataframe with expected schema
        return pl.DataFrame(schema={"uri": pl.Utf8, "file_date": pl.Date})

    # Concatenate all DataFrames
    result_df = pl.concat(all_dfs)
    context.log.info(f"Total rows: {result_df.shape[0]}")

    return result_df


@dg.asset
def mutate_oil_analysis(read_raw_oil_analysis):
    column_mapping = {
        "ID": "sample_id",
        "Unidad": "equipment_name",
        "Componente": "component",
        "Fecha Toma": "sample_date",
        "Fecha Rece": "received_date",
        "Fecha Anal": "analysis_date",
        "PM": "pm_type",
        "Horometro": "equipment_hours",
        "Fierro": "fe",  # Iron
        "Cromo": "cr",  # Chromium
        "Plomo": "pb",  # Lead
        "Cobre": "cu",  # Copper
        "Estaño": "sn",  # Tin
        "Aluminio": "al",  # Aluminum
        "Niquel": "ni",  # Nickel
        "Plata": "ag",  # Silver
        "Silicio": "si",  # Silicon
        "Boro": "b",  # Boron
        "Sodio": "na",  # Sodium
        "Magnesio": "mg",  # Magnesium
        "Calcio": "ca",  # Calcium
        "Bario": "ba",  # Barium
        "Fosforo": "p",  # Phosphorus
        "Zinc": "zn",  # Zinc
        "Molibdeno": "mo",  # Molybdenum
        "Titanio": "ti",  # Titanium
        "Vanadio": "v",  # Vanadium
        "Potasio": "k",  # Potassium
        "Oxidación": "oxidation",
        "Nitración": "nitration",
        "Sulfatación": "sulfation",
        "Combustibl": "fuel",
        "Agua": "water",
        "Refr": "coolant",
        "Hollin": "soot",
        "Visc a 40°": "viscosity_40c",
        "Visc a 100°": "viscosity_100c",
        "TAN": "tan",
        "TBN": "tbn",
        "PQ": "pq",
        ">4": "particles_gt_4",
        ">6": "particles_gt_6",
        ">14": "particles_gt_14",
        "Código ISO": "iso_code",
        "uri": "uri",
        "file_date": "file_date",
    }
    df = read_raw_oil_analysis.clone()
    df = (
        df.rename(column_mapping)
        .select(list(column_mapping.values()))
        .unique(subset=["sample_id"])
        .with_columns(
            equipment_name="TK" + pl.col("equipment_name").str.extract(r"(\d{3})$", 1),
            component_code=pl.col("component").str.extract(r"([A-Za-z]+)\d"),
            position_code=pl.col("component").str.extract(r"[A-Za-z]+(\d)").cast(pl.Int8),
            is_microfiltered=pl.col("component").str.contains("MICR").fill_null(False),
        )
        .with_columns(
            [
                pl.coalesce(
                    pl.col(col_name).str.strptime(pl.Date, "%Y-%m-%d %H:%M:%S", strict=False),
                    pl.col(col_name).str.strptime(pl.Date, "%d-%m-%Y", strict=False),
                ).alias(col_name)
                for col_name in ["sample_date", "received_date", "analysis_date"]
            ]
        )
        .drop_nulls(subset=["sample_date"])
        .drop(["component", "uri", "file_date"])
    )
    return df


@dg.asset
def read_oil_analysis():
    dl = DataLake()
    uri = "abfs://bhp-analytics-data/MAINTENANCE/OIL_ANALYSIS/oil_analysis.parquet"
    if dl.uri_exists(uri):
        return dl.read_tibble(uri=uri)
    else:
        return pl.DataFrame()


@dg.asset
def spawn_oil_analysis(
    context: dg.AssetExecutionContext, mutate_oil_analysis: pl.DataFrame, read_oil_analysis: pl.DataFrame
):
    """
    Performs an upsert operation between incoming oil analysis data and consolidated data.
    Writes the result to Data Lake and local SharePoint folder.

    Args:
        context: Dagster execution context
        mutate_oil_analysis: Incoming DataFrame with new or updated oil analysis records
        read_oil_analysis: Consolidated DataFrame with existing oil analysis records

    Returns:
        dict: Information about the data export operations
    """
    # Define key columns for the upsert operation
    key_columns = ["sample_id"]

    context.log.info(f"Performing upsert operation with {mutate_oil_analysis.height} incoming records")
    context.log.info(f"Consolidated dataset has {read_oil_analysis.height} existing records")

    if read_oil_analysis.height != 0:
        df = upsert_tibbles(
            incoming_df=mutate_oil_analysis,
            consolidated_df=read_oil_analysis,
            key_columns=key_columns,
        )
    else:
        context.log.info("Consolidated dataset is empty. Skipping upsert operation.")
        df = mutate_oil_analysis.clone()
    # Perform upsert operation
    df = df.sort("sample_date")

    result = {}

    # Upload to Data Lake as Parquet
    datalake = DataLake()
    datalake_path = datalake.upload_tibble(
        uri="abfs://bhp-analytics-data/MAINTENANCE/OIL_ANALYSIS/oil_analysis.parquet",
        df=df,
        format="parquet",
    )
    result["datalake"] = {"path": datalake_path, "format": "parquet"}
    context.log.info(f"Uploaded to Data Lake: {datalake_path}")

    # Save to SharePoint locally as Excel
    sharepoint_result = MSGraph().upload_tibble(
        site_id="KCHCLSP00022",
        filepath="/01. ÁREAS KCH/1.6 CONFIABILIDAD/CAEX/ANTECEDENTES/MAINTENANCE/OIL_ANALYSIS/oil_analysis.xlsx",
        df=df,
        format="excel",
    )
    result["sharepoint"] = {"file_url": sharepoint_result.web_url, "format": "excel"}

    # Add record count
    result["row_count"] = df.height

    return result
