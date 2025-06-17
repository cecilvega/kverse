import dagster as dg
from kdags.resources.tidyr import DataLake, MSGraph, MasterData
from kdags.config import DATA_CATALOG
import polars as pl
from datetime import datetime


@dg.asset(group_name="reliability")
def reference_warranties(context: dg.AssetExecutionContext):
    msgraph = MSGraph(context)
    df = msgraph.read_tibble(DATA_CATALOG["warranties"]["reference_path"])
    return df


@dg.asset(group_name="reliability")
def reference_psg_requests(context: dg.AssetExecutionContext):
    msgraph = MSGraph(context)
    df = msgraph.read_tibble(DATA_CATALOG["psg_requests"]["reference_path"])
    return df


# def merge_so_report(df, so_report):
#     df = df.join(
#         so_report.unique("service_order").select(["service_order", "service_order_status"]),
#         how="left",
#         on=["service_order"],
#     )
#     return df


@dg.asset(group_name="reliability")
def mutate_warranties(
    context: dg.AssetExecutionContext,
    component_changeouts: pl.DataFrame,
    component_history: pl.DataFrame,
    reference_warranties: pl.DataFrame,
    reference_psg_requests: pl.DataFrame,
    so_report: pl.DataFrame,
):
    msgraph = MSGraph(context=context)
    datalake = DataLake(context=context)
    components_df = MasterData.components()
    components_df = components_df.filter(pl.col("subcomponent_main") == True).select(
        ["component_name", "subcomponent_name", "warranty_hours"]
    )

    df = (
        component_history.join(components_df, on=["component_name", "subcomponent_name"], how="left")
        .filter((pl.col("warranty_hours").is_not_null()) & (pl.col("changeout_date") >= datetime(2025, 1, 1)))
        .select(
            [
                "equipment_name",
                "component_name",
                "subcomponent_name",
                "position_name",
                "changeout_date",
                "component_hours",
                "warranty_hours",
                "component_serial",
                "service_order",
            ]
        )
        .with_columns(component_hours=pl.col("component_hours").cast(pl.Float64))
        .with_columns(
            warranty_type=pl.when(pl.col("component_hours") <= pl.col("warranty_hours"))
            .then(pl.lit("normal"))
            .when(
                (pl.col("component_hours") > pl.col("warranty_hours"))
                & (pl.col("component_hours") <= pl.col("warranty_hours") + 3500)
            )
            .then(pl.lit("political"))
            .otherwise(None)
        )
        .filter(pl.col("warranty_type").is_not_null())
    )
    merge_columns = [
        "equipment_name",
        "component_name",
        "position_name",
        "changeout_date",
    ]
    df = df.join(reference_warranties, on=merge_columns, how="left", coalesce=True, validate="1:1")
    df = df.join(reference_psg_requests, how="left", on=[*merge_columns, "sf_case"], validate="1:1")
    df = df.sort(["changeout_date", "equipment_name"], descending=True)
    assert df.filter(pl.col("warranty_type").is_null()).height == 0
    datalake.upload_tibble(tibble=df, az_path=DATA_CATALOG["warranties"]["analytics_path"])

    return df


@dg.asset(
    description="Reads the consolidated oil analysis data from the ADLS analytics layer.",
    group_name="readr",
)
def warranties(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake(context=context)
    df = dl.read_tibble(az_path=DATA_CATALOG["warranties"]["analytics_path"])
    return df


@dg.asset
def publish_sp_warranties(context: dg.AssetExecutionContext, mutate_warranties: pl.DataFrame):
    msgraph = MSGraph(context=context)
    df = mutate_warranties.clone()

    upload_results = []
    # sp_paths = [
    #     # "sp://KCHCLGR00058/___/CONFIABILIDAD/GARANTIAS/garantias.xlsx",
    #     "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/CONFIABILIDAD/GARANTIAS/garantias.xlsx",
    # ]
    # for sp_path in sp_paths:
    #     upload_results.append(msgraph.upload_tibble(tibble=df, sp_path=sp_path))
    return upload_results
