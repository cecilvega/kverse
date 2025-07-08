import dagster as dg
import polars as pl
from kdags.resources.tidyr import DataLake, MSGraph
from kdags.config import DATA_CATALOG


@dg.asset(compute_kind="publish")
def publish_notifications(context: dg.AssetExecutionContext, notifications: pl.DataFrame):
    msgraph = MSGraph(context)
    df = notifications

    msgraph.upload_tibble(
        tibble=df,
        sp_path=DATA_CATALOG["work_order_history"]["publish_path"],
    )
    return df


@dg.asset(compute_kind="publish")
def publish_oil_analysis(context: dg.AssetExecutionContext, oil_analysis: pl.DataFrame):
    msgraph = MSGraph(context)
    df = oil_analysis

    msgraph.upload_tibble(tibble=df, sp_path=DATA_CATALOG["oil_analysis"]["publish_path"])
    return df
