import os

import dagster as dg
import polars as pl

from kdags.resources.dplyr import upsert_tibbles
from kdags.resources.tidyr import DataLake, MSGraph


class ReadRawOilAnalysisConfig(dg.Config):
    only_recent: bool = False
    days_lookback: int = 30


# Define the path for the final consolidated data
OIL_ANALYSIS_ANALYTICS_PATH = "abfs://bhp-analytics-data/MAINTENANCE/OIL_ANALYSIS/oil_analysis.parquet"


@dg.asset
def raw_oil_analysis(
    context: dg.AssetExecutionContext,
    config: ReadRawOilAnalysisConfig,
) -> pl.DataFrame:
    """
    Reads raw oil analysis Excel files from Azure Data Lake raw zone.
    Filters based on recency configuration.
    """
    dl = DataLake()
    abfs_path = "abfs://bhp-raw-data/LUBE_ANALYST/SCAAE"
    # List all files in the specified path
    files_df = dl.list_partitioned_paths(
        "abfs://bhp-raw-data/LUBE_ANALYST/SCAAE",
        only_recent=config.only_recent,
        days_lookback=config.days_lookback,
    )
    context.log.info(f"Found {len(files_df)} files in {abfs_path}")

    filepaths = files_df["abfs_path"].to_list()

    # Read all selected files
    all_dfs = []

    for filepath in filepaths:
        # Get file metadata
        filename = os.path.basename(filepath)
        date_str = filename[:8] if len(filename) >= 8 and filename[:8].isdigit() else None
        df = dl.read_tibble(az_path=filepath, include_az_path=True, use_polars=True, infer_schema_length=0)

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
        return pl.DataFrame(schema={"abfs_path": pl.Utf8, "file_date": pl.Date})

    # Concatenate all DataFrames
    result_df = pl.concat(all_dfs)
    context.log.info(f"Total rows: {result_df.shape[0]}")

    return result_df


@dg.asset
def mutate_oil_analysis(raw_oil_analysis):
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
        "abfs_path": "abfs_path",
        "file_date": "file_date",
    }
    df = raw_oil_analysis.clone()
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
        .drop(["component", "abfs_path", "file_date"])
    )
    return df


@dg.asset
def oil_analysis(context: dg.AssetExecutionContext, mutate_oil_analysis):
    new_df = mutate_oil_analysis.clone()
    context.log.info(f"Received {new_df.height} new/updated records for upsert.")

    existing_df = DataLake().read_tibble(OIL_ANALYSIS_ANALYTICS_PATH)
    key_columns = ["sample_id"]

    context.log.info(f"Performing upsert operation with {new_df.height} incoming records")
    context.log.info(f"Consolidated dataset has {existing_df.height} existing records")

    if existing_df.height != 0:
        df = upsert_tibbles(
            new_df=new_df,
            existing_df=existing_df,
            key_columns=key_columns,
        )
    else:
        context.log.info("Consolidated dataset is empty. Skipping upsert operation.")
        df = mutate_oil_analysis.clone()
    # Perform upsert operation
    df = df.sort("sample_date")

    context.log.info(f"Writing {df.height} records to {OIL_ANALYSIS_ANALYTICS_PATH}")
    DataLake().upload_tibble(df=df, az_path=OIL_ANALYSIS_ANALYTICS_PATH, format="parquet")
    context.log.info("Write successful.")
    context.add_output_metadata(
        {"abfs_path": OIL_ANALYSIS_ANALYTICS_PATH, "rows_written": df.height, "status": "completed"}
    )
    return oil_analysis


@dg.asset
def publish_sharepoint_oil_analysis(context: dg.AssetExecutionContext, oil_analysis: pl.DataFrame):
    """
    Takes the final oil_analysis data (read by read_oil_analysis) and uploads it to SharePoint.
    """
    df = oil_analysis.clone()
    if df.is_empty():
        context.log.warning("Received empty DataFrame. Skipping SharePoint upload.")
        context.add_output_metadata({"status": "skipped_empty_input", "sharepoint_url": None})
        return None

    context.log.info(f"Preparing to upload {df.height} records to SharePoint.")
    msgraph = MSGraph()  # Direct instantiation

    sharepoint_result = msgraph.upload_tibble(
        site_id="KCHCLSP00022",
        file_path="/01. ÁREAS KCH/1.6 CONFIABILIDAD/CAEX/ANTECEDENTES/MAINTENANCE/OIL_ANALYSIS/oil_analysis.xlsx",
        df=df,
        format="excel",
    )
    url = sharepoint_result.web_url
    context.log.info(f"Successfully uploaded oil analysis data to SharePoint: {url}")
    context.add_output_metadata(
        {"sharepoint_url": url, "format": "excel", "row_count": df.height, "status": "completed"}
    )
    return {"sharepoint_url": url}


@dg.asset(
    description="Reads the consolidated oil analysis data from the ADLS analytics layer.",
)
def read_oil_analysis(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake()
    if dl.az_path_exists(OIL_ANALYSIS_ANALYTICS_PATH):
        df = dl.read_tibble(az_path=OIL_ANALYSIS_ANALYTICS_PATH)
        context.log.info(f"Read {df.height} records from {OIL_ANALYSIS_ANALYTICS_PATH}.")
        return df
    else:
        context.log.warning(f"Data file not found at {OIL_ANALYSIS_ANALYTICS_PATH}. Returning empty DataFrame.")
        return pl.DataFrame()
