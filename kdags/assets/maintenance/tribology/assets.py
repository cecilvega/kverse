import os

import dagster as dg
import polars as pl

from kdags.resources.dplyr import upsert_tibbles
from kdags.resources.tidyr import DataLake, MSGraph
from kdags.config import DATA_CATALOG


class ReadRawOilAnalysisConfig(dg.Config):
    only_recent: bool = True
    days_lookback: int = 30


@dg.asset(group_name="maintenance")
def raw_oil_analysis(
    context: dg.AssetExecutionContext,
    config: ReadRawOilAnalysisConfig,
) -> pl.DataFrame:
    """
    Reads raw oil analysis Excel files from Azure Data Lake raw zone.
    Filters based on recency configuration.
    """
    dl = DataLake()
    az_path = "az://bhp-raw-data/LUBE_ANALYST/SCAAE"
    # List all files in the specified path
    files_df = dl.list_partitioned_paths(
        "az://bhp-raw-data/LUBE_ANALYST/SCAAE",
        only_recent=config.only_recent,
        days_lookback=config.days_lookback,
    )
    context.log.info(f"Found {len(files_df)} files in {az_path}")

    filepaths = files_df["az_path"].to_list()

    # Read all selected files
    all_dfs = []

    for filepath in filepaths:
        # Get file metadata
        filename = os.path.basename(filepath)
        date_str = filename[:8] if len(filename) >= 8 and filename[:8].isdigit() else None
        df = dl.read_tibble(az_path=filepath, include_az_path=True, infer_schema_length=0)

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
        return pl.DataFrame(schema={"az_path": pl.Utf8, "file_date": pl.Date})

    # Concatenate all DataFrames
    result_df = pl.concat(all_dfs)
    context.log.info(f"Total rows: {result_df.shape[0]}")

    return result_df


def process_oil_analysis(raw_oil_analysis):
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
        "az_path": "az_path",
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
        .drop(["component", "az_path", "file_date"])
    )
    return df


@dg.asset(group_name="maintenance")
def mutate_oil_analysis(context: dg.AssetExecutionContext, raw_oil_analysis: pl.DataFrame):
    process_df = process_oil_analysis(raw_oil_analysis)
    oil_analysis_analytics_path = DATA_CATALOG["oil_analysis"]["analytics_path"]
    new_df = process_df.clone()
    context.log.info(f"Received {new_df.height} new/updated records for upsert.")

    existing_df = DataLake().read_tibble(oil_analysis_analytics_path)
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
        df = process_df.clone()
    # Perform upsert operation
    df = df.sort("sample_date")

    context.log.info(f"Writing {df.height} records to {oil_analysis_analytics_path}")
    DataLake().upload_tibble(tibble=df, az_path=oil_analysis_analytics_path)
    context.log.info("Write successful.")
    context.add_output_metadata(
        {"az_path": oil_analysis_analytics_path, "rows_written": df.height, "status": "completed"}
    )
    return df


@dg.asset(
    group_name="readr",
    description="Reads the consolidated oil analysis data from the ADLS analytics layer.",
)
def oil_analysis(context: dg.AssetExecutionContext) -> pl.DataFrame:

    dl = DataLake(context)

    df = dl.read_tibble(az_path=DATA_CATALOG["oil_analysis"]["analytics_path"])

    return df
