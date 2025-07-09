from io import BytesIO

import dagster as dg
import pandas as pd
import polars as pl
from kdags.resources.tidyr import DataLake, MSGraph, MasterData


from kdags.config import DATA_CATALOG


def clean_notifications(df: pl.DataFrame) -> pl.DataFrame:
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

    df = df.filter(pl.col("Sort Field").is_in(MasterData.equipments()["equipment_name"].unique()))
    return df


@dg.asset(compute_kind="raw")
def raw_notifications(context: dg.AssetExecutionContext):
    dl = DataLake(context)
    df = pd.read_excel(BytesIO(dl.read_bytes(DATA_CATALOG["notifications"]["raw_path"])))
    df = pl.from_pandas(df)
    return df


@dg.asset(compute_kind="manifest")
def notifications_manifest(context: dg.AssetExecutionContext):
    dl = DataLake(context=context)
    # Prepare manifest with all files and their status
    df = dl.prepare_manifest(
        DATA_CATALOG["notifications"]["raw_path"],
        DATA_CATALOG["notifications"]["manifest_path"],
    )

    return df


@dg.asset(compute_kind="mutate")
def mutate_notifications(
    context: dg.AssetExecutionContext,
    notifications: pl.DataFrame,
    notifications_manifest: pl.DataFrame,
):
    """Process work order history with upsert logic"""

    dl = DataLake(context)

    # Define paths

    manifest_path = DATA_CATALOG["notifications"]["manifest_path"]
    analytics_path = DATA_CATALOG["notifications"]["analytics_path"]

    # Define deduplication strategy
    dedup_keys = ["Notification"]  # Adjust based on your actual column names

    # Upsert with deduplication
    deduplicated_df, updated_manifest = dl.upsert_tibble(
        manifest_df=notifications_manifest,
        analytics_df=notifications,
        dedup_keys=dedup_keys,
        date_column="partition_date",
        add_partition_date=True,
        cleaning_fn=clean_notifications,
    )

    COLUMNS_MAP = {
        "Notification": "notification",
        "Notification Description": "notification_description",
        "Notification Date": "notification_date",
        "Created By": "created_by",
        "Changed By": "changed_by",
        "Changed On": "changed_on",
        "Notification Completion Date": "notification_completion_date",
        "Notification Status": "notification_status",
        "Technical Object": "technical_object",
        "Sort Field": "equipment_name",
        "Priority": "priority",
        "Order Status": "order_status",
        "Plant of Main Work Center": "site_name",
    }
    deduplicated_df = deduplicated_df.rename(
        COLUMNS_MAP,
    ).select(list(COLUMNS_MAP.values()))
    deduplicated_df = deduplicated_df.with_columns(
        site_name=pl.when(pl.col("site_name").str.to_lowercase().str.contains("escondida mine"))
        .then(pl.lit("MEL"))
        .when(pl.col("site_name").str.to_lowercase().str.contains("spence"))
        .then(pl.lit("SPENCE"))
        .otherwise(pl.col("site_name")),
        technical_object=pl.col("technical_object").str.extract(r"^([^(]+)", 1).str.strip_chars(),
        notification_status=pl.col("notification_status").str.replace(r"\s*\(\d+\)$", ""),
    )

    # Apply work_order specific cleaning AFTER upsert
    if deduplicated_df.height > 0:
        # Save results
        dl.upload_tibble(deduplicated_df, analytics_path)
        dl.upload_tibble(updated_manifest, manifest_path)

        context.log.info(f"Saved {deduplicated_df.height} deduplicated work orders")
    return deduplicated_df, updated_manifest
