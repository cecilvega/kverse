import dagster as dg
import polars as pl

from kdags.config import DATA_CATALOG
from kdags.resources.tidyr import DataLake


@dg.asset(
    group_name="components",
    compute_kind="readr",
    description="Reads the consolidated oil analysis data from the ADLS analytics layer.",
    # metadata={"dagster/column_schema": COMPONENT_CHANGEOUTS_SCHEMA},
)
def component_changeouts(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake(context=context)
    df = dl.read_tibble(az_path=DATA_CATALOG["component_changeouts"]["analytics_path"])
    return df


@dg.asset(
    group_name="components",
    compute_kind="readr",
    description="Reads the consolidated oil analysis data from the ADLS analytics layer.",
)
def component_history(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake(context=context)
    df = dl.read_tibble(az_path=DATA_CATALOG["component_history"]["analytics_path"])
    return df
