from io import BytesIO

import dagster as dg
import pandas as pd
import polars as pl
from kdags.resources.tidyr import DataLake, MSGraph
from kdags.config import DATA_CATALOG


@dg.asset(compute_kind="readr")
def komtrax_smr(context: dg.AssetExecutionContext):
    dl = DataLake(context)
    df = dl.read_tibble(DATA_CATALOG["komtrax_smr"]["analytics_path"])
    return df


@dg.asset
def read_events(context: dg.AssetExecutionContext):
    df = pl.read_parquet(r"C:\Users\andmn\PycharmProjects\events.parquet")
    # dl = DataLake(context)
    # return dl.read_tibble("az://bhp-analytics-data/OPERATION/GE/events.parquet")
    return df


@dg.asset
def read_fault(context: dg.AssetExecutionContext):
    df = pl.read_parquet(r"C:\Users\andmn\PycharmProjects\fault.parquet")
    # dl = DataLake(context)
    # return dl.read_tibble("az://bhp-analytics-data/OPERATION/GE/events.parquet")
    return df


@dg.asset
def read_fault_codes(context: dg.AssetExecutionContext):
    df = pl.read_parquet(r"C:\Users\andmn\PycharmProjects\fault_codes.parquet")
    # dl = DataLake(context)
    # return dl.read_tibble("az://bhp-analytics-data/OPERATION/GE/events.parquet")
    return df


@dg.asset
def read_haul():
    dl = DataLake()
    uri = "az://bhp-analytics-data/OPERATION/PLM3/haul.parquet"
    if dl.az_path_exists(uri):
        return dl.read_tibble(az_path=uri)
    else:
        return pl.DataFrame()
