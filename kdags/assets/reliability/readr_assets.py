import dagster as dg
import polars as pl

from kdags.config import DATA_CATALOG
from kdags.resources.tidyr import DataLake


@dg.asset(
    group_name="reliability",
    compute_kind="readr",
)
def component_fleet(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake(context=context)
    df = dl.read_tibble(az_path=DATA_CATALOG["component_fleet"]["analytics_path"])
    return df


@dg.asset
def ep(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake(context=context)
    df = dl.read_tibble(DATA_CATALOG["ep"]["analytics_path"])
    return df
