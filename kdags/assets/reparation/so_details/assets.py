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
def mutate_quotations(component_reparations):
    dl = DataLake()
    quotations_df = dl.read_tibble("az://bhp-raw-data/RESO/SERVICE_ORDER_DETAILS/quotations.parquet")

    cr_df = component_reparations.clone()
    df = (
        cr_df.join(quotations_df, on=["service_order"], how="left")
        .rename({"date_time_str": "quotation_updated_dt", "amount": "repair_cost"})
        .with_columns(
            repair_cost=pl.col("repair_cost")
            .str.extract(r"\$\s*([\d,\.]+)", group_index=1)  # Extract the numeric part
            .str.replace(",", "")  # Remove commas if they exist
            .cast(pl.Float64, strict=False)  # Convert to float to handle both integer and decimal values
        )
    )

    df = df.with_columns(quotation_updated_dt=pl.col("quotation_updated_dt").str.to_datetime("%d %B %Y %H:%M:%S"))

    dl.upload_tibble(tibble=df, az_path=QUOTATIONS_ANALYTICS_PATH)
    return df


@dg.asset
def quotations(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake(context=context)
    df = dl.read_tibble(az_path=QUOTATIONS_ANALYTICS_PATH)
    return df


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
    ##askdhaslkjdasknd
    return sp_results
