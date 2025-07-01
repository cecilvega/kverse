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
