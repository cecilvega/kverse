import dagster as dg
import polars as pl
from kdags.resources.tidyr import DataLake, MSGraph
from kdags.config import DATA_CATALOG


@dg.asset(group_name="docs")
def publish_work_order_history(context: dg.AssetExecutionContext, mutate_work_order_history: pl.DataFrame):
    msgraph = MSGraph(context)

    # datalake.upload_tibble(tibble=tidy_ep_df, az_path="az://bhp-analytics-data/RELIABILITY/EP/tidy_ep.parquet")
    msgraph.upload_tibble(
        tibble=mutate_work_order_history,
        sp_path=DATA_CATALOG["work_order_history"]["publish_path"],
    )


@dg.asset(group_name="docs")
def publish_oil_analysis(context: dg.AssetExecutionContext, mutate_oil_analysis: pl.DataFrame):
    df = mutate_oil_analysis.clone()
    msgraph = MSGraph()
    upload_results = []
    sp_paths = [
        "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/MANTENIMIENTO/analisis_aceite.xlsx",
        DATA_CATALOG["oil_analysis"]["publish_path"],
    ]
    for sp_path in sp_paths:
        context.log.info(f"Publishing to {sp_path}")
        upload_results.append(msgraph.upload_tibble(tibble=df, sp_path=sp_path))
    return upload_results
