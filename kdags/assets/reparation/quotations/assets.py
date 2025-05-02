import re
from datetime import datetime

import dagster as dg
import polars as pl
from datetime import date
from kdags.resources.tidyr import DataLake, MSGraph

import polars as pl
import dagster as dg
from kdags.resources.tidyr import MSGraph, DataLake

QUOTATIONS_ANALYTICS_PATH = "az://bhp-analytics-data/RELIABILITY/QUOTATIONS/quotations.parquet"


@dg.asset
def mutate_quotations(changeouts_so):
    dl = DataLake()
    quotations_df = dl.read_tibble("az://bhp-raw-data//RESO/QUOTATIONS/quotations.parquet")

    cso_df = changeouts_so.clone()
    df = cso_df.join(quotations_df, on=["service_order"], how="left").with_columns(
        amount=pl.col("amount")
        .str.extract(r"\$\s*([\d,\.]+)", group_index=1)  # Extract the numeric part
        .str.replace(",", "")  # Remove commas if they exist
        .cast(pl.Float64, strict=False)  # Convert to float to handle both integer and decimal values
    )

    dl.upload_tibble(tibble=df, az_path=QUOTATIONS_ANALYTICS_PATH, format="parquet")
    return df


@dg.asset
def quotations(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake()
    if dl.az_path_exists(QUOTATIONS_ANALYTICS_PATH):
        df = dl.read_tibble(az_path=QUOTATIONS_ANALYTICS_PATH)
        df = df.rename({"amount": "repair_cost"})
        context.log.info(f"Read {df.height} records from {QUOTATIONS_ANALYTICS_PATH}.")
        return df
    else:
        context.log.warning(f"Data file not found at {QUOTATIONS_ANALYTICS_PATH}. Returning empty DataFrame.")
        return pl.DataFrame()


@dg.asset
def publish_sp_quotations(context: dg.AssetExecutionContext, mutate_quotations: pl.DataFrame):
    df = mutate_quotations.clone()
    msgraph = MSGraph()
    sp_results = []
    # sp_results.extend(msgraph.upload_tibble("sp://KCHCLGR00058/___/CONFIABILIDAD/presupuestos.xlsx", df))
    sp_results.extend(
        msgraph.upload_tibble(
            df,
            "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/REPARACION/presupuestos.xlsx",
        )
    )
    return sp_results
