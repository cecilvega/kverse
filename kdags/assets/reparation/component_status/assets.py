import re
from datetime import datetime

import dagster as dg
import polars as pl
from datetime import date
from kdags.resources.tidyr import DataLake, MSGraph
from .constants import *

COMPONENT_STATUS_ANALYTICS_PATH = "az://bhp-analytics-data/REPARATION/COMPONENT_STATUS/component_status.parquet"


@dg.asset(description="Reads raw component status parquet files based on the latest partitions.")  # Changed to dg.asset
# Add context back for logging
def raw_component_status(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """
    Reads the latest partitioned raw component status data.
    Instantiates DataLake directly. Minimal logging.
    """
    datalake = DataLake()  # Direct instantiation
    base_raw_path = "az://bhp-raw-data/RESO/COMPONENT_STATUS"

    files_df = datalake.list_paths(base_raw_path)
    if files_df.is_empty():
        context.log.warning(f"No files found in {base_raw_path}")
        return pl.DataFrame()

    parsed_data = []
    for path in files_df["az_path"].to_list():
        component_year_match = re.search(r"component_status_(\d{4})\.parquet", path)
        component_year = int(component_year_match.group(1)) if component_year_match else None
        partition_match = re.search(r"y=(\d{4})/m=(\d{2})/d=(\d{2})", path)
        partition_date = datetime(*map(int, partition_match.groups())) if partition_match else None
        if component_year and partition_date:
            parsed_data.append({"az_path": path, "component_year": component_year, "partition_date": partition_date})

    if not parsed_data:
        context.log.warning(f"No valid partitioned files found under {base_raw_path}")
        return pl.DataFrame()

    paths_df = pl.DataFrame(parsed_data)
    latest_paths_df = (
        paths_df.group_by("component_year")
        .agg(pl.col("partition_date").max())
        .join(paths_df, on=["component_year", "partition_date"])
    )
    paths_to_read = latest_paths_df["az_path"].to_list()

    tibbles = [datalake.read_tibble(az_path=p) for p in paths_to_read]
    if not tibbles:
        return pl.DataFrame()

    df = pl.concat(tibbles, how="vertical_relaxed")
    context.log.info(f"Read {len(paths_to_read)} raw files, total rows: {df.height}")
    return df


@dg.asset(description="Selects relevant columns, renames, and performs type conversions.")  # Changed to dg.asset
# Use direct parameter passing for dependency
# Add context back for logging
def process_component_status(context: dg.AssetExecutionContext, raw_component_status: pl.DataFrame) -> pl.DataFrame:

    df = raw_component_status.clone()  # Clone input

    available_raw_cols = [col for col in SELECTED_RAW_COLUMNS if col in df.columns]

    if len(available_raw_cols) < len(SELECTED_RAW_COLUMNS):
        context.log.warning("Not all expected raw columns were present in the input DataFrame.")

    # 1. Load initial data and add a unique ID BEFORE filtering
    df = df.with_row_count("original_row_id")  # Add index

    mapping_subset = {k: v for k, v in COLUMN_MAPPING.items()}

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
    df = (
        df.select(["original_row_id"] + available_raw_cols)  # Keep row id
        .rename(mapping_subset)
        .with_columns(
            [
                pl.col(c)
                .str.split_exact("-", 1)
                .struct.field("field_0")
                .str.split_exact(".", 1)
                .struct.field("field_0")
                .str.to_integer(strict=False)  # Keep strict=False
                .alias(c)
                for c in INT_CONVERSION_COLUMNS
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
                for c in DATE_CONVERSION_COLUMNS
            ]
        )
        .with_columns(pl.col("sap_equipment_name").str.strip_suffix(".0").cast(pl.Int64, strict=False).fill_null(-1))
        .with_columns(
            [
                pl.col(c)
                .str.strip_chars()
                .str.replace("\t", "")
                .str.replace_all("#", "")
                .str.split(" ")
                .list.first()
                .alias(c)
                for c in ["component_serial"]
            ]
        )
        .with_columns(
            [pl.col(col).replace(0, -1) for col in ["sap_equipment_name", "customer_work_order", "service_order"]]
        )
    )

    df = df.with_columns(update_date=pl.lit(date.today()))
    df = df.with_columns(
        component_status=pl.when(pl.col("reso_closing_date").is_not_null())
        .then(pl.lit("delivered"))
        .when(pl.col("load_final_report_in_review").is_not_null())
        .then(pl.lit("repaired"))
        .when(pl.col("approval_date").is_not_null())
        .then(pl.lit("assembly"))
        .when(pl.col("latest_quotation_publication").is_not_null())
        .then(pl.lit("awaiting_approval"))
        .when(pl.col("load_preliminary_report_in_review").is_not_null())
        .then(pl.lit("preparing_quotation"))
        .when(pl.col("opening_date").is_not_null())
        .then(pl.lit("disassembly"))
        .when(pl.col("reception_date").is_not_null())
        .then(pl.lit("received"))
        .otherwise(pl.lit(None))  # Assign Null if no condition is met
    )

    context.log.info(f"Mutation complete. Output rows: {df.height}")
    return df


@dg.asset(  # Changed to dg.asset
    description="Writes the final component status data to the ADLS analytics layer.",
)
# Use direct parameter passing for dependency, update name here
# Add context back for logging / metadata
def mutate_component_status(context: dg.AssetExecutionContext, process_component_status: pl.DataFrame) -> pl.DataFrame:
    """
    Takes the processed data from mutate_component_status and writes it to the final
    analytics path, overwriting any existing file. Instantiates DataLake directly. Minimal logging.
    """
    df = process_component_status.clone()  # Use the input parameter name

    datalake = DataLake()  # Direct instantiation
    context.log.info(f"Writing {df.height} records to {COMPONENT_STATUS_ANALYTICS_PATH}")

    datalake.upload_tibble(tibble=df, az_path=COMPONENT_STATUS_ANALYTICS_PATH, format="parquet")
    context.add_output_metadata(
        {  # Add metadata on success
            "az_path": COMPONENT_STATUS_ANALYTICS_PATH,
            "rows_written": df.height,
        }
    )
    return df


@dg.asset(
    description="Reads the consolidated oil analysis data from the ADLS analytics layer.",
)
def component_status(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake()
    if dl.az_path_exists(COMPONENT_STATUS_ANALYTICS_PATH):
        df = dl.read_tibble(az_path=COMPONENT_STATUS_ANALYTICS_PATH)
        context.log.info(f"Read {df.height} records from {COMPONENT_STATUS_ANALYTICS_PATH}.")
        return df
    else:
        context.log.warning(f"Data file not found at {COMPONENT_STATUS_ANALYTICS_PATH}. Returning empty DataFrame.")
        return pl.DataFrame()


@dg.asset
def publish_sp_component_status(context: dg.AssetExecutionContext, mutate_component_status: pl.DataFrame):
    df = mutate_component_status.clone()
    msgraph = MSGraph()
    upload_results = []
    sp_paths = [
        "sp://KCHCLGR00058/___/REPARACION/mega_estatus_componentes.xlsx",
        "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/REPARACION/mega_estatus_componentes.xlsx",
    ]
    for sp_path in sp_paths:
        context.log.info(f"Publishing to {sp_path}")
        upload_results.append(msgraph.upload_tibble(tibble=df, sp_path=sp_path))

    return upload_results
