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


@dg.asset
def raw_so_quotations(context: dg.AssetExecutionContext):
    dl = DataLake(context)
    # try:
    df = dl.read_tibble(DATA_CATALOG["so_quotations"]["raw_path"])
    # except Exception as e:
    #     df = pl.DataFrame(schema=QUOTATION_SCHEMA)

    return df


@dg.asset
def mutate_so_quotations(context: dg.AssetExecutionContext, raw_so_quotations: pl.DataFrame):
    dl = DataLake(context)
    # Get all paths and filter for QUOTATIONS
    q_df = (
        dl.list_paths("az://bhp-raw-data/RESO/DOCUMENTS")
        .filter(pl.col("az_path").str.contains("/QUOTATIONS/"))
        .with_columns(
            # Extract filename from the full path
            filename=pl.col("az_path").str.extract(r"/([^/]+)$", group_index=1),
            # Extract service_order - it's the folder before QUOTATIONS
            # Pattern: /service_order/QUOTATIONS/filename
            service_order=pl.col("az_path").str.extract(r"/(\d+)/QUOTATIONS/", group_index=1).cast(pl.Int64),
            # Extract version from filename - looking for _V{number}_
            version=pl.col("az_path").str.extract(r"_V(\d+)_", group_index=1).cast(pl.Int64),
        )
        .with_columns(ppto_number=pl.col("filename").str.extract(r"PPTO_(\d+)_V", group_index=1).cast(pl.Int64))
    )
    # Filter to keep only PPTO_1 when there are duplicates
    q_df = (
        q_df.with_columns(
            # Count how many files exist for each service_order + version combination
            count_per_group=pl.col("filename")
            .count()
            .over(["service_order", "version"])
        )
        .filter(
            # If there's only one file for the service_order + version, keep it
            # If there are multiple files, only keep PPTO_1
            (pl.col("count_per_group") == 1)
            | ((pl.col("count_per_group") > 1) & (pl.col("ppto_number") == 1))
        )
        .drop("count_per_group")  # Remove the temporary column
    )

    df = (
        raw_so_quotations.rename({"date_time_str": "quotation_updated_dt", "amount": "repair_cost"})
        .with_columns(
            repair_cost=pl.col("repair_cost")
            .str.extract(r"\$\s*([\d,\.]+)", group_index=1)  # Extract the numeric part
            .str.replace(",", "")  # Remove commas if they exist
            .cast(pl.Float64, strict=False),
            quotation_updated_dt=pl.col("quotation_updated_dt").str.to_datetime("%d %B %Y %H:%M:%S"),
        )
        .join(q_df, on=["service_order", "version"], how="left", validate="1:1")
    )

    dl.upload_tibble(tibble=df, az_path=DATA_CATALOG["so_quotations"]["analytics_path"])
    return df


@dg.asset
def so_quotations(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake(context=context)
    df = dl.read_tibble(az_path=DATA_CATALOG["so_quotations"]["analytics_path"])
    return df


# @dg.asset
# def publish_sp_quotations(context: dg.AssetExecutionContext, mutate_quotations: pl.DataFrame):
#     df = mutate_quotations.clone()
#     msgraph = MSGraph(context)
#     sp_results = []
#     # sp_results.extend(msgraph.upload_tibble("sp://KCHCLGR00058/___/CONFIABILIDAD/presupuestos.xlsx", df))
#     sp_results.extend(
#         msgraph.upload_tibble(
#             df,
#             "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/REPARACION/presupuestos.xlsx",
#         )
#     )
#     ##askdhaslkjdasknd
#     return sp_results
