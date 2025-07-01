import dagster as dg
import polars as pl

from kdags.config import DATA_CATALOG
from kdags.resources.tidyr import DataLake


@dg.asset(
    compute_kind="readr",
)
def component_fleet(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake(context=context)
    df = dl.read_tibble(az_path=DATA_CATALOG["component_fleet"]["analytics_path"])
    return df


@dg.asset(
    compute_kind="readr",
)
def ep(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake(context=context)
    df = dl.read_tibble(DATA_CATALOG["ep"]["analytics_path"])
    return df


@dg.asset(
    compute_kind="readr",
    description="Reads the consolidated oil analysis data from the ADLS analytics layer.",
)
def icc(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake(context)
    df = dl.read_tibble(az_path=DATA_CATALOG["icc"]["analytics_path"])
    return df
