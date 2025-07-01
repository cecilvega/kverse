from io import BytesIO

import dagster as dg
import pandas as pd
import polars as pl
from kdags.resources.tidyr import DataLake, MSGraph
from kdags.config import DATA_CATALOG


def clean_work_order_history(df: pl.DataFrame) -> pl.DataFrame:
    string_cols = [col for col, dtype in zip(df.columns, df.dtypes) if dtype == pl.Utf8]

    df = df.with_columns(
        [
            pl.col(col)
            .str.replace_all(r"NBSP", " ")
            .str.replace_all(r"\u00A0", " ")
            .str.replace_all(r"\s+", " ")
            .str.strip_chars()
            .alias(col)
            for col in string_cols
        ]
    )
    COLUMNS_MAP = {
        "Order": "work_order",
        "Sort Field": "equipment_name",
        "Order Description": "description",
        "Priority": "priority",
        "Basic Start Date": "start_date",
        "Basic End Date": "end_date",
        "System Status": "fiori_status",
        "Revision": "revision_week",
        "Entered By": "entered_by",
        "Functional Location": "functional_location",
        "Notification": "notification",
        "Order Status": "order_status",
        "Order Type": "order_type",
        "Plant of Main Work Center": "site_name",
    }
    df = df.rename(
        COLUMNS_MAP,
    ).select(list(COLUMNS_MAP.values()))
    df = (
        df.with_columns(
            [
                # Extract all components at once using capture groups
                pl.col("revision_week")
                .str.extract_groups(r"WK (\d+): ([\d.]+) - ([\d.]+) \((Y\d+W\d+)\)")
                .struct.rename_fields(
                    [
                        "week_number",
                        "revision_start_date",
                        "revision_end_date",
                        "revision_year_week",
                    ]
                )
            ]
        )
        .unnest("revision_week")
        .with_columns(
            # Convert start_date from DD.MM.YY to proper date (assuming 20YY format)
            revision_start_date=pl.col("revision_start_date")
            .str.strptime(pl.Date, "%d.%m.%y", strict=False)
            .alias("start_date_parsed"),
            revision_end_date=pl.col("revision_end_date")
            .str.strptime(pl.Date, "%d.%m.%y", strict=False)
            .alias("end_date_parsed"),
            site_name=pl.when(pl.col("site_name").str.to_lowercase().str.contains("escondida mine"))
            .then(pl.lit("MEL"))
            .when(pl.col("site_name").str.to_lowercase().str.contains("spence"))
            .then(pl.lit("SPENCE"))
            .otherwise(pl.col("site_name")),
            functional_location=pl.col("functional_location").str.extract(r"^([^(]+)", 1).str.strip_chars(),
            order_type=pl.col("order_type").str.replace(r"Work Order \(([^)]+)\) \(([PM]\w+)\)", r"$2 - $1"),
        )
    ).drop(["week_number", "revision_start_date", "revision_end_date"])
    return df


@dg.asset(compute_kind="raw")
def raw_work_order_history(context: dg.AssetExecutionContext):
    dl = DataLake(context)
    df = pd.read_excel(BytesIO(dl.read_bytes(DATA_CATALOG["work_order_history"]["raw_path"])))
    df = pl.from_pandas(df)
    return df


@dg.asset(compute_kind="manifest")
def work_order_history_manifest(context: dg.AssetExecutionContext):
    dl = DataLake(context=context)
    # Prepare manifest with all files and their status
    df = dl.prepare_manifest(
        DATA_CATALOG["work_order_history"]["raw_path"],
        DATA_CATALOG["work_order_history"]["manifest_path"],
    )
    # df = dl.read_tibble(az_path=DATA_CATALOG["work_order_history"]["manifest_path"])
    return df


@dg.asset(compute_kind="mutate")
def mutate_work_order_history(
    context: dg.AssetExecutionContext,
    work_order_history: pl.DataFrame,
    work_order_history_manifest: pl.DataFrame,
):
    """Process work order history with upsert logic"""

    dl = DataLake(context)

    # Define paths

    manifest_path = DATA_CATALOG["work_order_history"]["manifest_path"]
    analytics_path = DATA_CATALOG["work_order_history"]["analytics_path"]

    # Define deduplication strategy
    dedup_keys = ["work_order"]  # Adjust based on your actual column names

    # Upsert with deduplication
    deduplicated_df, updated_manifest = dl.upsert_tibble(
        manifest_df=work_order_history_manifest,
        analytics_df=work_order_history,
        dedup_keys=dedup_keys,
        date_column="partition_date",
        add_partition_date=True,
        cleaning_fn=clean_work_order_history,
    )

    # Apply work_order specific cleaning AFTER upsert
    if deduplicated_df.height > 0:
        # Save results
        dl.upload_tibble(deduplicated_df, analytics_path)
        dl.upload_tibble(updated_manifest, manifest_path)

        context.log.info(f"Saved {deduplicated_df.height} deduplicated work orders")
    return deduplicated_df, updated_manifest
