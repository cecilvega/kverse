import dagster as dg
import polars as pl
from kdags.resources.tidyr import DataLake, MSGraph
from kdags.config import DATA_CATALOG, TIDY_NAMES, tidy_tibble


@dg.asset()
def publish_ep(context: dg.AssetExecutionContext, ep: pl.DataFrame):
    msgraph = MSGraph(context)
    df = ep.clone()

    df = df.with_columns(
        changeout_date=pl.col("changeout_date").dt.to_string("%Y-%m-%d"),
    )

    df = df.select(
        [
            "equipment_name",
            "component_name",
            "position_name",
            "changeout_date",
            "component_hours",
            "component_usage",
            "ep_status",
            "raised_by",
            "ep_date",
            "dificultad_tecnica",
            "repair_cost",
            "count_without_repair_cost",
            "service_order",
            "mean_repair_cost",
            "prorrata_sale",
            "economical_impact",
            "days_since_quotation",
            "payment_status_analysis",
            "priority_score",
        ]
    ).rename(
        {
            "equipment_name": "Equipo",
            "component_name": "Componente",
            "position_name": "Posición",
            "changeout_date": "Fecha Cambio",
            "component_usage": "%Uso",
            # "ep_status": "Estado EP",
            "raised_by": "Levantado Por",
            "ep_date": "Fecha EP",
            # "payment_status_analysis": "Clasificación EP",
            "service_order": "OS KRCC",
            # "days_since_quotation": "Días desde Último Presupuesto",
            "repair_cost": "Costo Reparación",
            "prorrata_sale": "Costo Prorrata",
            "mean_repair_cost": "Costo Reparación Medio",
        },
        strict=False,
    )
    tidy_ep_df = (
        df.filter(
            ((pl.col("ep_status") != "skipped") & (pl.col("ep_status") != "planificado_bajo_costo"))
            | (pl.col("ep_status").is_null())
        )
        .filter(pl.col("Costo Reparación").is_not_null())
        .filter(pl.col("Fecha EP").is_null())
    )

    msgraph.upload_tibble(
        tibble=df,
        sp_path=DATA_CATALOG["ep"]["publish_path"],
    )

    msgraph.upload_tibble(
        tibble=tidy_ep_df,
        sp_path=DATA_CATALOG["tidy_ep"]["publish_path"],
    )

    return df


@dg.asset(compute_kind="publish")
def publish_icc(context: dg.AssetExecutionContext, icc: pl.DataFrame):
    msgraph = MSGraph(context)

    df = icc.rename(TIDY_NAMES, strict=False).pipe(tidy_tibble, context)
    msgraph.upload_tibble(
        tibble=df,
        sp_path=DATA_CATALOG["icc"]["publish_path"],
    )
    return df
