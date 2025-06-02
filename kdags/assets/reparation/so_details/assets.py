import re
from datetime import datetime

import dagster as dg
import polars as pl
from datetime import date
from kdags.resources.tidyr import DataLake, MSGraph

import polars as pl
import dagster as dg
from kdags.resources.tidyr import MSGraph, DataLake
from kdags.config import DATA_CATALOG

QUOTATIONS_ANALYTICS_PATH = "az://bhp-analytics-data/RELIABILITY/QUOTATIONS/quotations.parquet"


@dg.asset
def mutate_quotations(context: dg.AssetExecutionContext):
    dl = DataLake(context)
    soq_df = dl.read_tibble(DATA_CATALOG["so_quotations"]["raw_path"])

    q_df = dl.list_paths("az://bhp-raw-data/RESO/QUOTATIONS/").with_columns(
        # Extract service_order (parent folder - the number before the filename)
        service_order=pl.col("az_path").str.extract(r"/(\d+)/[^/]+$").cast(pl.Int64),
        # Extract version (VX pattern from filename)
        version=pl.col("az_path").str.extract(r"_V(\d+)_", group_index=1).cast(pl.Int64),
    )

    df = (
        soq_df.rename({"date_time_str": "quotation_updated_dt", "amount": "repair_cost"})
        .with_columns(
            repair_cost=pl.col("repair_cost")
            .str.extract(r"\$\s*([\d,\.]+)", group_index=1)  # Extract the numeric part
            .str.replace(",", "")  # Remove commas if they exist
            .cast(pl.Float64, strict=False),
            quotation_updated_dt=pl.col("quotation_updated_dt").str.to_datetime("%d %B %Y %H:%M:%S"),
        )
        .join(q_df, on=["service_order", "version"], how="left", validate="1:1")
    )

    dl.upload_tibble(tibble=df, az_path=DATA_CATALOG["quotations"]["analytics_path"])
    return df


@dg.asset
def quotations(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake(context=context)
    df = dl.read_tibble(az_path=DATA_CATALOG["so_quotations"]["analytics_path"])
    return df


@dg.asset
def publish_sp_quotations(context: dg.AssetExecutionContext, mutate_quotations: pl.DataFrame):
    df = mutate_quotations.clone()
    msgraph = MSGraph(context)
    sp_results = []
    # sp_results.extend(msgraph.upload_tibble("sp://KCHCLGR00058/___/CONFIABILIDAD/presupuestos.xlsx", df))
    sp_results.extend(
        msgraph.upload_tibble(
            df,
            "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/REPARACION/presupuestos.xlsx",
        )
    )
    ##askdhaslkjdasknd
    return sp_results
