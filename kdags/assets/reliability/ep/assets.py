from datetime import date

import dagster as dg
import polars as pl

from kdags.config import DATA_CATALOG
from kdags.resources.tidyr import *
from .prorrata import calculate_prorrata_sale


@dg.asset
def pcp_repair_costs(context: dg.AssetExecutionContext):
    dl = DataLake(context)
    df = dl.read_tibble(DATA_CATALOG["pcp_repair_costs"]["raw_path"]).with_columns(
        mean_repair_cost=pl.col("mean_repair_cost").round(0).cast(pl.Int64)
    )
    return df


@dg.asset
def gfa_overhaul_rates(context: dg.AssetExecutionContext):
    dl = DataLake(context)
    return dl.read_tibble(DATA_CATALOG["gfa_overhaul_rates"]["raw_path"])


@dg.asset
def ep_reference(context: dg.AssetExecutionContext):
    msgraph = MSGraph(context)
    df = msgraph.read_tibble(DATA_CATALOG["ep"]["reference_path"]).drop(["component_icon"])
    return df


@dg.asset
def mutate_ep(
    context: dg.AssetExecutionContext,
    component_history: pl.DataFrame,
    pcp_repair_costs: pl.DataFrame,
    gfa_overhaul_rates: pl.DataFrame,
    ep_reference: pl.DataFrame,
    so_report,
    so_quotations,
):
    dl = DataLake(context)
    msgraph = MSGraph(context)

    merge_columns = ["equipment_name", "component_name", "position_name", "changeout_date"]
    # Analizar EP desde el 2025 en adelante + casos históricos
    df = component_history.join(
        MasterData.equipments().select(["equipment_name", "site_name", "equipment_model"]),
        how="left",
        on="equipment_name",
    ).filter(pl.col("site_name") == "MEL")
    # Guardar fecha publicación presupuesto
    main_component_history_df = (
        df.join(
            MasterData.components().filter("subcomponent_main").select(["component_name", "subcomponent_name"]),
            how="inner",
            on=["component_name", "subcomponent_name"],
        )
        .join(
            so_report.select(["service_order", "first_quotation_publication"]),
            how="left",
            on="service_order",
        )
        .select(
            [
                *merge_columns,
                "component_hours",
                "component_usage",
                "service_order",
                "first_quotation_publication",
            ]
        )
    )

    ### Agregar costos de reparación
    df = df.join(
        so_quotations.select(["service_order", "repair_cost", "quotation_updated_dt", "user", "remarks"]),
        how="left",
        on="service_order",
    )

    df = (
        df.group_by(
            [
                *merge_columns,
                "site_name",
                "equipment_model",
            ]
        )
        .agg(
            repair_cost=pl.sum("repair_cost"),
            count_without_repair_cost=pl.col("subcomponent_name").filter(pl.col("repair_cost").is_null()).len(),
            service_order=pl.col("service_order"),
        )
        .join(main_component_history_df, how="left", on=merge_columns)
    )

    # Verificar que no falten cases en el input de EP

    missing_df = (
        df.filter((pl.col("changeout_date").dt.year() >= 2025))
        .select(merge_columns)
        .join(
            ep_reference.with_columns(_merge=pl.lit(True)),
            how="anti",
            on=merge_columns,
            coalesce=True,
        )
    )
    if not missing_df.is_empty():
        context.log.critical(f"Missing entries: {missing_df.to_dicts()}")
        msgraph.upload_tibble(
            missing_df.with_columns(changeout_date=pl.col("changeout_date").dt.to_string("%Y-%m-%d")),
            "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/REFERENCE/STD_ERROR/missing_ep.xlsx",
        )

    # Agregar información horómetros
    df = ep_reference.join(
        df.select(
            [
                *merge_columns,
                "site_name",
                "equipment_model",
                "component_hours",
                "component_usage",
                "repair_cost",
                "count_without_repair_cost",
                "first_quotation_publication",
                "service_order",
            ]
        ),
        how="left",
        on=merge_columns,
    )

    ### Agregar costos medios de reparación
    # Summary de costos de reparación por componentes mayores
    merge_columns = ["site_name", "equipment_model", "component_name"]
    component_mean_repair_cost = (
        pcp_repair_costs.join(
            MasterData.components().select(["subcomponent_tag", "component_name", "subcomponent_name"]),
            how="left",
            on=["subcomponent_tag"],
        )
        .group_by(merge_columns)
        .agg(mean_repair_cost=pl.col("mean_repair_cost").sum())
    )

    df = df.join(
        component_mean_repair_cost,
        how="left",
        on=merge_columns,
    )
    ####

    ### Agregar costo prorrata
    # Summary tarifa GFA por componentes mayores
    merge_columns = ["site_name", "equipment_model", "component_name"]
    component_gfa_overhaul_rates = gfa_overhaul_rates.group_by(
        ["site_name", "equipment_model", "component_name", "mtbo"]
    ).agg(gfa_overhaul_rate=pl.col("gfa_overhaul_rate").sum())
    df = df.join(component_gfa_overhaul_rates, how="left", on=merge_columns)
    df = df.pipe(calculate_prorrata_sale).drop(["gfa_overhaul_rate", "mtbo"])

    ###
    df = df.drop(["site_name", "equipment_model"])

    # Análisis final
    df = df.with_columns(
        economical_impact=pl.col("prorrata_sale") - pl.col("repair_cost"),
        days_since_quotation=(pl.lit(date.today()) - pl.col("first_quotation_publication")).dt.total_days(),
    )
    # Decision variables for payment status analysis
    HIGH_USAGE_THRESHOLD = 1.05  # >105% usage
    MEDIUM_USAGE_THRESHOLD = 0.85  # Between 85% and 105%
    HIGH_IMPACT_THRESHOLD = -10000  # < -10000 is High Impact
    MEDIUM_IMPACT_THRESHOLD = -3000  # < -3000 (but >= -10000) is Medium Impact
    STANDARD_COST_TOLERANCE = 10000  # Within this range of mean is Standard Cost

    # Priority formula parameters
    MAX_DAYS_CONSIDERED = 365  # Cap at 1 year for normalization

    df = df.with_columns(
        # Payment status analysis
        payment_status_analysis=pl.when(pl.col("count_without_repair_cost") != 0)
        .then(pl.lit("Missing Budget"))
        .otherwise(
            pl.when(pl.col("component_usage") > HIGH_USAGE_THRESHOLD)
            .then(pl.lit(">105% Usage"))
            .when(pl.col("component_usage") >= MEDIUM_USAGE_THRESHOLD)
            .then(pl.lit("85-105% Usage"))
            .otherwise(pl.lit("<85% Usage"))
            + pl.lit(" ")
            + pl.when(pl.col("economical_impact") < HIGH_IMPACT_THRESHOLD)
            .then(pl.lit("High Impact"))
            .when(pl.col("economical_impact") < MEDIUM_IMPACT_THRESHOLD)
            .then(pl.lit("Medium Impact"))
            .otherwise(pl.lit("Low Impact"))
            + pl.when((pl.col("repair_cost") - pl.col("mean_repair_cost")).abs() <= STANDARD_COST_TOLERANCE)
            .then(pl.lit(" - Mean Repair Cost"))
            .otherwise(pl.lit(""))
        ),
        # Priority formula (0 to 1)
        priority_score=pl.when(pl.col("count_without_repair_cost") != 0)
        .then(pl.lit(1.0))  # Missing budget always max priority
        .otherwise(
            # Usage factor (0.5 weight - most important)
            (
                pl.when(pl.col("component_usage") > HIGH_USAGE_THRESHOLD)
                .then(pl.lit(1.0))  # >105% gets full usage score
                .when(pl.col("component_usage") >= MEDIUM_USAGE_THRESHOLD)
                .then(pl.lit(0.7))  # 85-105% gets medium score
                .otherwise(pl.lit(0.3))  # <85% gets low score
            )
            * 0.5
            +
            # Impact factor (0.2 weight - medium/low impact easier to resolve)
            (
                pl.when(pl.col("economical_impact") < HIGH_IMPACT_THRESHOLD)
                .then(pl.lit(0.6))  # High impact = lower priority (harder cases)
                .when(pl.col("economical_impact") < MEDIUM_IMPACT_THRESHOLD)
                .then(pl.lit(0.8))  # Medium impact = higher priority (easier)
                .otherwise(pl.lit(1.0))  # Low impact = highest priority (easiest)
            )
            * 0.2
            +
            # Days factor (0.3 weight - time urgency)
            (pl.col("days_since_quotation").clip(0, MAX_DAYS_CONSIDERED) / MAX_DAYS_CONSIDERED) * 0.3
        ),
    )

    dl.upload_tibble(tibble=df, az_path=DATA_CATALOG["ep"]["analytics_path"])
    return df


@dg.asset
def ep(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake(context=context)
    df = dl.read_tibble(DATA_CATALOG["ep"]["analytics_path"])
    return df


@dg.asset
def publish_sp_ep(context: dg.AssetExecutionContext, mutate_ep: pl.DataFrame):
    msgraph = MSGraph(context)
    df = mutate_ep.clone()

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
        }
    )
    tidy_ep_df = (
        df.filter(
            ((pl.col("ep_status") != "skipped") & (pl.col("ep_status") != "planificado_bajo_costo"))
            | (pl.col("ep_status").is_null())
        )
        .filter(pl.col("Costo Reparación").is_not_null())
        .filter(pl.col("Fecha EP").is_null())
    )

    # msgraph = MSGraph()
    upload_results = []
    upload_results.append(
        # datalake.upload_tibble(tibble=tidy_ep_df, az_path="az://bhp-analytics-data/RELIABILITY/EP/tidy_ep.parquet")
        msgraph.upload_tibble(
            tibble=df,
            sp_path="sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/CONFIABILIDAD/ep.xlsx",
        )
    )
    upload_results.append(
        msgraph.upload_tibble(
            tibble=tidy_ep_df,
            sp_path="sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/CONFIABILIDAD/tidy_ep.xlsx",
        )
    )

    return [1, 2]
