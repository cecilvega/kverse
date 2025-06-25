from io import BytesIO

import dagster as dg
import pandas as pd
import polars as pl
from kdags.resources.tidyr import DataLake, MSGraph
from kdags.config import DATA_CATALOG


@dg.asset(group_name="maintenance", compute_kind="readr")
def work_order_history(context: dg.AssetExecutionContext):
    dl = DataLake(context)
    df = dl.read_tibble(DATA_CATALOG["work_order_history"]["analytics_path"])
    return df


@dg.asset(
    group_name="maintenance",
    compute_kind="readr",
    description="Reads the consolidated oil analysis data from the ADLS analytics layer.",
)
def oil_analysis(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake(context)
    df = dl.read_tibble(az_path=DATA_CATALOG["oil_analysis"]["analytics_path"])
    return df
