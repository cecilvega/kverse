import dagster as dg
from kdags.resources.tidyr import DataLake, MSGraph, MasterData
import polars as pl
from datetime import datetime


WARRANTIES_ANALYTIS_PATH = "az://bhp-analytics-data/RELIABILITY/WARRANTIES/warranties.parquet"


def merge_component_reparations(df: pl.DataFrame, component_reparations: pl.DataFrame):
    df = df.join(
        component_reparations.select(
            [
                "equipment_name",
                "component_name",
                "position_name",
                "changeout_date",
                "sap_equipment_name",
                "service_order",
            ]
        ),
        how="left",
        on=["equipment_name", "component_name", "position_name", "changeout_date", "sap_equipment_name"],
    )
    return df


def merge_so_report(df, so_report):
    df = df.join(
        so_report.unique("service_order").select(["service_order", "service_order_status"]),
        how="left",
        on=["service_order"],
    )
    return df


@dg.asset
def mutate_warranties(
    context: dg.AssetExecutionContext,
    component_changeouts: pl.DataFrame,
    component_reparations: pl.DataFrame,
    so_report: pl.DataFrame,
):
    msgraph = MSGraph(context=context)
    datalake = DataLake(context=context)
    warranty_df = pl.read_excel(
        r"C:\Users\andmn\OneDrive - Komatsu Ltd\DRS MEL - JEFE_CONFIABILIDAD\INPUTS\warranty_input.xlsx"
    )
    # warranty_df = msgraph.read_tibble(
    #     "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/INPUTS/warranty_input.xlsx"
    # )
    components_df = MasterData.components()
    components_df = components_df.filter(pl.col("subcomponent_main") == True).select(
        ["component_name", "subcomponent_name", "warranty_hours"]
    )
    df = component_changeouts.clone()
    df = (
        df.join(components_df, on=["component_name", "subcomponent_name"], how="left")
        .filter((pl.col("warranty_hours").is_not_null()) & (pl.col("changeout_date") >= datetime(2024, 9, 26)))
        .select(
            [
                "equipment_name",
                "component_name",
                "subcomponent_name",
                "position_name",
                "changeout_date",
                "component_hours",
                "warranty_hours",
                "sap_equipment_name",
                "component_serial",
            ]
        )
        .with_columns(component_hours=pl.col("component_hours").cast(pl.Float64))
        .with_columns(
            warranty_type=pl.when(pl.col("component_hours") <= pl.col("warranty_hours"))
            .then(pl.lit("normal"))
            .when((pl.col("component_hours") > pl.col("warranty_hours")) & (pl.col("component_hours") <= 6500))
            .then(pl.lit("political"))
            .otherwise(None)
        )
        .filter(pl.col("warranty_type").is_not_null())
        .sort(["changeout_date", "equipment_name"])
    )
    df = df.join(
        warranty_df,
        on=[
            "equipment_name",
            "component_name",
            "position_name",
            "sap_equipment_name",
            "changeout_date",
        ],
        how="full",
        coalesce=True,
    )
    df = merge_component_reparations(df, component_reparations)
    df = merge_so_report(df, so_report).sort("changeout_date")
    datalake.upload_tibble(tibble=df, az_path=WARRANTIES_ANALYTIS_PATH)
    return df


@dg.asset(
    description="Reads the consolidated oil analysis data from the ADLS analytics layer.",
    # metadata={"dagster/column_schema": COMPONENT_CHANGEOUTS_SCHEMA},
)
def warranties(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake(context=context)
    df = dl.read_tibble(az_path=WARRANTIES_ANALYTIS_PATH)
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
