import dagster as dg
import polars as pl
from kdags.resources.tidyr import DataLake, MSGraph
from kdags.config import DATA_CATALOG

# # Only materialize once daily at a specific time
# policy = dg.AutoMaterializePolicy.eager().with_rules(
#     dg.AutoMaterializeRule.materialize_on_cron("0 20 * * *", timezone="US/Central")
# )


@dg.asset(
    compute_kind="publish",
    # auto_materialize_policy=policy,
)
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
