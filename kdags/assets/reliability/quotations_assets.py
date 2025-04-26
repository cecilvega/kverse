import polars as pl
import dagster as dg
from kdags.resources.tidyr import MSGraph, DataLake

QUOTATIONS_ANALYTICS_PATH = "az://bhp-analytics-data/RELIABILITY/QUOTATIONS/quotations.parquet"


@dg.asset
def quotations(read_component_reparations):
    dl = DataLake()
    quotations_df = dl.read_tibble("az://bhp-raw-data//RESO/QUOTATIONS/quotations.parquet")

    cr_df = read_component_reparations.clone()
    df = cr_df.select(
        [
            "equipment_name",
            "component_name",
            "subcomponent_name",
            "position_name",
            "component_serial",
            "sap_equipment_name",
            "component_hours",
            "changeout_date",
            "reception_date",
            "service_order",
            "customer_work_order",
        ]
    ).join(quotations_df, on=["service_order"], how="inner")

    dl.upload_tibble(df=df, az_path=QUOTATIONS_ANALYTICS_PATH, format="parquet")
    return df


@dg.asset
def publish_sp_quotations(context: dg.AssetExecutionContext, quotations: pl.DataFrame):
    df = quotations.clone()
    msgraph = MSGraph()
    sp_results = []
    sp_results.extend(msgraph.upload_tibble("sp://KCHCLGR00058/___/CONFIABILIDAD/presupuestos.xlsx", df))
    sp_results.extend(
        msgraph.upload_tibble(
            "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/CONFIABILIDAD/presupuestos.xlsx",
            df,
        )
    )
    return sp_results
