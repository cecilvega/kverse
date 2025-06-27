import dagster as dg
import polars as pl

from kdags.config import DATA_CATALOG
from kdags.resources.tidyr import DataLake


@dg.asset(
    group_name="reparation",
    compute_kind="readr",
    description="Reads the consolidated oil analysis data from the ADLS analytics layer.",
)
def component_reparations(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake(context=context)
    df = dl.read_tibble(az_path=DATA_CATALOG["component_reparations"]["analytics_path"])
    return df


@dg.asset(group_name="reparation", compute_kind="readr")
def so_quotations(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake(context=context)
    df = dl.read_tibble(az_path=DATA_CATALOG["so_quotations"]["analytics_path"])
    return df


@dg.asset(group_name="reparation", compute_kind="readr")
def so_documents(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake(context=context)
    df = dl.read_tibble(az_path=DATA_CATALOG["so_documents"]["analytics_path"])
    return df


@dg.asset(group_name="reparation", compute_kind="readr")
def quotations(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake(context=context)
    df = dl.read_tibble(az_path=DATA_CATALOG["quotations"]["analytics_path"])
    return df
