import polars as pl
import dagster as dg
from kdags.resources.tidyr import MSGraph, DataLake

COMPONENT_REPARATIONS_ANALYTICS_PATH = (
    "az://bhp-analytics-data/RELIABILITY/COMPONENT_REPARATIONS/component_reparations.parquet"
)


@dg.asset
def mutate_component_reparations(component_changeouts, mutate_changeouts_so, mutate_so_report, mutate_quotations):
    dl = DataLake()
    cso_df = mutate_changeouts_so.clone()
    so_df = mutate_so_report.clone()
    cc_df = component_changeouts.clone()
    quotations_df = mutate_quotations.clone()
    df = cc_df.select(
        [
            "equipment_name",
            "component_name",
            "subcomponent_name",
            "position_name",
            "changeout_date",
            "component_serial",
            "sap_equipment_name",
            "customer_work_order",
            "component_hours",
            "component_usage",
        ]
    )
    # Add linked component_changeouts with service_orders
    df = df.join(
        cso_df.select(
            [
                "equipment_name",
                "component_name",
                "subcomponent_name",
                "position_name",
                "changeout_date",
                "service_order",
            ]
        ),
        on=[
            "equipment_name",
            "component_name",
            "subcomponent_name",
            "position_name",
            "changeout_date",
        ],
        how="left",
    )
    # Adding information specific to the service order
    df = df.join(
        so_df.sort(["service_order", "reception_date"])
        .unique(subset=["service_order"], keep="last", maintain_order=True)
        .select(
            [
                "service_order",
                "component_status",
                "reception_date",
                "latest_quotation_publication",
                "load_preliminary_report_date",
                "load_final_report_date",
                "quotation_status",
            ]
        ),
        on=["service_order"],
        how="left",
    )
    # Adding quotation info
    df = df.join(quotations_df.select(["service_order", "amount"]), on=["service_order"], how="left")

    dl.upload_tibble(tibble=df, az_path=COMPONENT_REPARATIONS_ANALYTICS_PATH, format="parquet")
    return df


@dg.asset
def publish_sp_component_reparations(context: dg.AssetExecutionContext, mutate_component_reparations: pl.DataFrame):
    df = mutate_component_reparations.clone()
    msgraph = MSGraph()
    upload_results = []
    sp_paths = [
        # "sp://KCHCLGR00058/___/CONFIABILIDAD/reparacion_componentes.xlsx",
        "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/CONFIABILIDAD/reparacion_componentes.xlsx",
    ]
    for sp_path in sp_paths:
        context.log.info(f"Publishing to {sp_path}")
        upload_results.append(msgraph.upload_tibble(tibble=df, sp_path=sp_path))
    return upload_results


@dg.asset(
    description="Reads the consolidated oil analysis data from the ADLS analytics layer.",
)
def component_reparations(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake()
    if dl.az_path_exists(COMPONENT_REPARATIONS_ANALYTICS_PATH):
        df = dl.read_tibble(az_path=COMPONENT_REPARATIONS_ANALYTICS_PATH)
        context.log.info(f"Read {df.height} records from {COMPONENT_REPARATIONS_ANALYTICS_PATH}.")
        return df
    else:
        context.log.warning(
            f"Data file not found at {COMPONENT_REPARATIONS_ANALYTICS_PATH}. Returning empty DataFrame."
        )
        return pl.DataFrame()
