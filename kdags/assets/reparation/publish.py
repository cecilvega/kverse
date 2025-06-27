import dagster as dg
import polars as pl
from kdags.resources.tidyr import DataLake, MSGraph
from kdags.config import DATA_CATALOG


import dagster as dg
import polars as pl
from kdags.resources.tidyr import DataLake, MSGraph, MasterData
from kdags.config import DATA_CATALOG, TIDY_NAMES, tidy_tibble


@dg.asset(group_name="reparation", compute_kind="publish")
def publish_component_reparations(context: dg.AssetExecutionContext, component_reparations: pl.DataFrame):
    msgraph = MSGraph(context)

    df = component_reparations.clone()
    df = df.rename(TIDY_NAMES, strict=False).pipe(tidy_tibble, context)
    msgraph.upload_tibble(
        tibble=df,
        sp_path=DATA_CATALOG["component_reparation"]["publish_path"],
    )
    return df
