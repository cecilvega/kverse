import dagster as dg
from kdags.resources.tidyr import DataLake, MSGraph, MasterData
import polars as pl
from datetime import datetime


WARRANTIES_ANALYTIS_PATH = "az://bhp-analytics-data/RELIABILITY/WARRANTIES/warranties.parquet"


@dg.asset
def mutate_warranties(component_changeouts: pl.DataFrame):
    msgraph = MSGraph()
    datalake = DataLake()
    warranty_df = msgraph.read_tibble(
        "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/CONFIABILIDAD/GARANTIAS/warranty_input.xlsx"
    )
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
            "subcomponent_name",
            "position_name",
            "sap_equipment_name",
            "changeout_date",
        ],
        how="full",
        coalesce=True,
    )
    datalake.upload_tibble(tibble=df, az_path=WARRANTIES_ANALYTIS_PATH)
    return df


@dg.asset(
    description="Reads the consolidated oil analysis data from the ADLS analytics layer.",
    # metadata={"dagster/column_schema": COMPONENT_CHANGEOUTS_SCHEMA},
)
def warranties(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake()
    if dl.az_path_exists(WARRANTIES_ANALYTIS_PATH):
        df = dl.read_tibble(az_path=WARRANTIES_ANALYTIS_PATH)
        context.log.info(f"Read {df.height} records from {WARRANTIES_ANALYTIS_PATH}.")
        return df
    else:
        context.log.warning(f"Data file not found at {WARRANTIES_ANALYTIS_PATH}. Returning empty DataFrame.")
        return pl.DataFrame()


@dg.asset
def publish_sp_warranties(context: dg.AssetExecutionContext, mutate_warranties: pl.DataFrame):
    df = mutate_warranties.clone()
    msgraph = MSGraph()
    upload_results = []
    sp_paths = [
        "sp://KCHCLGR00058/___/CONFIABILIDAD/GARANTIAS/garantias.xlsx",
        "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/CONFIABILIDAD/GARANTIAS/garantias.xlsx",
    ]
    for sp_path in sp_paths:
        context.log.info(f"Publishing to {sp_path}")
        upload_results.append(msgraph.upload_tibble(tibble=df, sp_path=sp_path))
    return upload_results
