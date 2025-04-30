import dagster as dg
import polars as pl

from kdags.resources.tidyr import MSGraph, DataLake, MasterData

COMPONENT_TRACKING_ANALYTICS_PATH = (
    "az://bhp-analytics-data/RELIABILITY/COMPONENT_REPARATIONS/component_tracking.parquet"
)
COMPONENT_HISTORY_ANALYTICS_PATH = "az://bhp-analytics-data/RELIABILITY/COMPONENT_REPARATIONS/component_history.parquet"


@dg.asset
def mutate_component_history(component_reparations: pl.DataFrame, so_report, component_changeouts):
    dl = DataLake()
    cr_df = component_reparations.clone()
    so_df = so_report.clone()
    cc_df = component_changeouts.clone()
    components_df = MasterData.components()
    components_df = components_df.filter(pl.col("subcomponent_main").is_not_null()).select(
        ["component_name", "subcomponent_name"]
    )

    df = cr_df.join(components_df, how="inner", on=["component_name", "subcomponent_name"])
    df = df.join(
        cc_df.filter(pl.col("equipment_model").str.contains("960E")).select(
            [
                "equipment_name",
                "component_name",
                "subcomponent_name",
                "position_name",
                "changeout_date",
                "installed_component_serial",
            ]
        ),
        on=["equipment_name", "component_name", "subcomponent_name", "position_name", "changeout_date"],
        how="inner",
    )
    df = (
        df.join(
            so_df.sort(["service_order", "reception_date"])
            .unique(subset=["service_order"], keep="last", maintain_order=True)
            .select(
                [
                    "service_order",
                    "opening_date",
                    "reso_closing_date",
                    "update_date",
                    "approval_date",
                ]
            ),
            how="left",
            on=["service_order"],
        )
        .with_columns(reso_closing_date=pl.col("reso_closing_date").fill_null(pl.col("load_final_report_date")))
        .sort("changeout_date")
    )

    # df = df.filter(pl.col("changeout_date") >= datetime(2024, 9, 26)).sort("changeout_date")
    # List to hold duration calculation expressions
    duration_expressions = []

    # --- Days in received ---
    duration_expressions.append(
        pl.when(pl.col("reception_date").is_not_null())  # Must have started
        .then(
            pl.when(pl.col("opening_date").is_not_null())  # Check if next step started
            .then(pl.col("opening_date") - pl.col("reception_date"))  # Duration = Next Start - Start
            .otherwise(pl.col("update_date") - pl.col("reception_date"))  # Duration = Update Date - Start
        )
        .otherwise(None)  # Not received yet
        .dt.total_days()  # Get duration as integer days
        .alias("days_in_received")
    )

    # --- Days in disassembly ---
    duration_expressions.append(
        pl.when(pl.col("opening_date").is_not_null())  # Must have started
        .then(
            pl.when(pl.col("load_preliminary_report_date").is_not_null())  # Check if next step started
            .then(pl.col("load_preliminary_report_date") - pl.col("opening_date"))
            .otherwise(pl.col("update_date") - pl.col("opening_date"))
        )
        .otherwise(None)  # Not in disassembly yet
        .dt.total_days()
        .alias("days_in_disassembly")
    )

    # --- Days in preparing_quotation ---
    duration_expressions.append(
        pl.when(pl.col("load_preliminary_report_date").is_not_null())  # Must have started
        .then(
            pl.when(pl.col("latest_quotation_publication").is_not_null())  # Check if next step started
            .then(pl.col("latest_quotation_publication") - pl.col("load_preliminary_report_date"))
            .otherwise(pl.col("update_date") - pl.col("load_preliminary_report_date"))
        )
        .otherwise(None)
        .dt.total_days()
        .alias("days_in_preparing_quotation")
    )

    # --- Days in awaiting_approval ---
    duration_expressions.append(
        pl.when(pl.col("latest_quotation_publication").is_not_null())  # Must have started
        .then(
            pl.when(pl.col("approval_date").is_not_null())  # Check if next step started
            .then(pl.col("approval_date") - pl.col("latest_quotation_publication"))
            .otherwise(pl.col("update_date") - pl.col("latest_quotation_publication"))
        )
        .otherwise(None)
        .dt.total_days()
        .alias("days_in_awaiting_approval")
    )

    # --- Days in assembly ---
    duration_expressions.append(
        pl.when(pl.col("approval_date").is_not_null())  # Must have started
        .then(
            pl.when(pl.col("load_final_report_date").is_not_null())  # Check if next step started
            .then(pl.col("load_final_report_date") - pl.col("approval_date"))
            .otherwise(pl.col("update_date") - pl.col("approval_date"))
        )
        .otherwise(None)
        .dt.total_days()
        .alias("days_in_assembly")
    )

    # --- Days in repaired ---
    duration_expressions.append(
        pl.when(pl.col("load_final_report_date").is_not_null())  # Must have started
        .then(
            pl.when(pl.col("reso_closing_date").is_not_null())  # Check if next step started (Delivery)
            .then(pl.col("reso_closing_date") - pl.col("load_final_report_date"))
            .otherwise(pl.col("update_date") - pl.col("load_final_report_date"))
        )
        .otherwise(None)
        .dt.total_days()
        .alias("days_in_repaired")
    )

    # --- Time Since delivered ---
    duration_expressions.append(
        pl.when(pl.col("reso_closing_date").is_not_null())
        .then(pl.col("update_date") - pl.col("reso_closing_date"))
        .otherwise(None)
        .dt.total_days()
        .alias("days_since_delivered")
    )
    df = df.with_columns(duration_expressions).with_columns(
        total_days=(pl.col("reception_date") - pl.col("changeout_date")).dt.total_days()
    )

    dl.upload_tibble(tibble=df, az_path=COMPONENT_HISTORY_ANALYTICS_PATH)
    return df


@dg.asset
def mutate_component_tracking(mutate_component_history):
    dl = DataLake()
    msgraph = MSGraph()
    retired_components_df = msgraph.read_tibble(
        "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/REPARACION/retired_components.xlsx"
    ).drop(["last_changeout_date", "last_service_order"])
    df = mutate_component_history.clone()

    df = df.clone().filter(
        pl.col("subcomponent_name").is_in(
            [
                "transmision",
                "cilindro_direccion",
                "suspension_delantera",
                "suspension_trasera",
            ]
        )
    )

    # Tomar los componentes montados
    latest_installed_components_df = (
        df.sort(
            [
                "equipment_name",
                "component_name",
                "subcomponent_name",
                "position_name",
                "changeout_date",
            ],
            descending=False,
        )
        .unique(
            subset=[
                "equipment_name",
                "component_name",
                "subcomponent_name",
                "position_name",
            ],
            keep="last",
            maintain_order=True,
        )
        .select(
            [
                "installed_component_serial",
            ]
        )
        .rename({"installed_component_serial": "component_serial"})
        .with_columns(mounted=pl.lit(True))
    )

    # Tomar el último estado por cada serie de cada componente
    latest_cc_df = (
        df.sort(
            [
                "component_serial",
                "changeout_date",
            ],
            descending=False,
        )
        .unique(
            subset=["component_serial"],
            keep="last",
            maintain_order=True,
        )
        .select(
            [
                "equipment_name",
                "component_name",
                "subcomponent_name",
                "position_name",
                "component_serial",
                "changeout_date",
                "service_order",
            ]
        )
    )

    cs_df = pl.concat(
        [
            df.select(["component_serial", "component_name", "subcomponent_name"]),
            df.select(["installed_component_serial", "component_name", "subcomponent_name"]).rename(
                {"installed_component_serial": "component_serial"}
            ),
        ]
    ).unique()
    cs_df = cs_df.join(
        latest_installed_components_df,
        on="component_serial",
        how="left",
    )
    cs_df = cs_df.join(
        latest_cc_df,
        on=["component_serial", "component_name", "subcomponent_name"],
        how="left",
    )

    # Agregar indicador de series de componentes que están fuera de servicio
    cs_df = (
        cs_df.join(
            retired_components_df,
            how="left",
            on=["component_name", "subcomponent_name", "component_serial"],
        )
        .filter(pl.col("retired_status").is_null())
        .drop(["retired_status"])
        .with_columns(pl.col("mounted").fill_null(pl.lit(False)))
    )

    # Agregar información de la órden de servicio
    cs_df = cs_df.join(
        df.select(
            [
                "equipment_name",
                "component_name",
                "subcomponent_name",
                "position_name",
                "changeout_date",
                # "service_order",
                # "reception_date",
                "component_status",
                "quotation_status",
                "reception_date",
                # "lastest_quotation_date",
                "load_preliminary_report_date",
                "load_final_report_date",
                "total_days",
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
    dl.upload_tibble(tibble=df, az_path=COMPONENT_TRACKING_ANALYTICS_PATH)
    return cs_df


@dg.asset
def publish_sp_component_history(
    context: dg.AssetExecutionContext, mutate_component_history: pl.DataFrame, mutate_component_tracking: pl.DataFrame
):
    component_history_df = mutate_component_history.clone()
    component_tracking_df = mutate_component_tracking.clone()
    msgraph = MSGraph()
    upload_results = []

    sp_paths = [
        "sp://KCHCLGR00058/___/CONFIABILIDAD/reparacion_componentes.xlsx",
        "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/CONFIABILIDAD/historial_componentes.xlsx",
    ]
    for sp_path in sp_paths:
        context.log.info(f"Publishing to {sp_path}")
        upload_results.append(msgraph.upload_tibble(tibble=component_history_df, sp_path=sp_path))

    sp_paths = [
        "sp://KCHCLGR00058/___/CONFIABILIDAD/reparacion_componentes.xlsx",
        "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/CONFIABILIDAD/seguimiento_componentes.xlsx",
    ]
    for sp_path in sp_paths:
        context.log.info(f"Publishing to {sp_path}")
        upload_results.append(msgraph.upload_tibble(tibble=component_tracking_df, sp_path=sp_path))

    return upload_results
