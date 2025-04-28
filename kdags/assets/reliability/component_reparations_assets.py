import polars as pl
import dagster as dg
from kdags.resources.tidyr import MSGraph, DataLake

COMPONENT_REPARATIONS_ANALYTICS_PATH = (
    "az://bhp-analytics-data/RELIABILITY/COMPONENT_REPARATIONS/component_reparations.parquet"
)


@dg.asset
def mutate_component_reparations(component_changeouts, linked_component_history, component_status):
    dl = DataLake()
    pr_df = linked_component_history.clone()
    cs_df = component_status.clone()
    cc_df = component_changeouts.clone()
    df = cc_df.drop(["cc_index"]).join(pr_df, on=["customer_work_order", "sap_equipment_name"], how="left")
    df = df.join(
        cs_df.select(
            [
                "service_order",
                "reception_date",
                "update_date",
                "component_status",
                "opening_date",
                "load_preliminary_report_in_review",
                "latest_quotation_publication",
                "approval_date",
                "load_final_report_in_review",
                "reso_closing_date",
            ]
        ),
        on=[
            "service_order",
            "reception_date",
        ],
        how="left",
    )

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
            pl.when(pl.col("load_preliminary_report_in_review").is_not_null())  # Check if next step started
            .then(pl.col("load_preliminary_report_in_review") - pl.col("opening_date"))
            .otherwise(pl.col("update_date") - pl.col("opening_date"))
        )
        .otherwise(None)  # Not in disassembly yet
        .dt.total_days()
        .alias("days_in_disassembly")
    )

    # --- Days in preparing_quotation ---
    duration_expressions.append(
        pl.when(pl.col("load_preliminary_report_in_review").is_not_null())  # Must have started
        .then(
            pl.when(pl.col("latest_quotation_publication").is_not_null())  # Check if next step started
            .then(pl.col("latest_quotation_publication") - pl.col("load_preliminary_report_in_review"))
            .otherwise(pl.col("update_date") - pl.col("load_preliminary_report_in_review"))
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
            pl.when(pl.col("load_final_report_in_review").is_not_null())  # Check if next step started
            .then(pl.col("load_final_report_in_review") - pl.col("approval_date"))
            .otherwise(pl.col("update_date") - pl.col("approval_date"))
        )
        .otherwise(None)
        .dt.total_days()
        .alias("days_in_assembly")
    )

    # --- Days in repaired ---
    duration_expressions.append(
        pl.when(pl.col("load_final_report_in_review").is_not_null())  # Must have started
        .then(
            pl.when(pl.col("reso_closing_date").is_not_null())  # Check if next step started (Delivery)
            .then(pl.col("reso_closing_date") - pl.col("load_final_report_in_review"))
            .otherwise(pl.col("update_date") - pl.col("load_final_report_in_review"))
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
    df = df.with_columns(duration_expressions)

    dl.upload_tibble(tibble=df, az_path=COMPONENT_REPARATIONS_ANALYTICS_PATH, format="parquet")
    return df


@dg.asset
def publish_sp_component_reparations(context: dg.AssetExecutionContext, mutate_component_reparations: pl.DataFrame):
    df = mutate_component_reparations.clone()
    msgraph = MSGraph()
    upload_results = []
    sp_paths = [
        "sp://KCHCLGR00058/___/CONFIABILIDAD/reparacion_componentes.xlsx",
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
