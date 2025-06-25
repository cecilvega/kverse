import dagster as dg
import polars as pl
from kdags.resources.tidyr import DataLake, MSGraph
from kdags.config import DATA_CATALOG, TIDY_NAMES, tidy_tibble


@dg.asset(group_name="components", compute_kind="publish")
def publish_component_history(context: dg.AssetExecutionContext, component_history: pl.DataFrame):
    msgraph = MSGraph(context)
    df = component_history.rename(TIDY_NAMES, strict=False).pipe(tidy_tibble, context)
    msgraph.upload_tibble(
        tibble=df,
        sp_path=DATA_CATALOG["component_history"]["publish_path"],
    )
    return df
