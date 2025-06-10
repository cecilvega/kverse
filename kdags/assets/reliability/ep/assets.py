import dagster as dg
import polars as pl
from kdags.resources.tidyr import *
from datetime import datetime
from kdags.config import DATA_CATALOG


def merge_ccr(component_changeouts: pl.DataFrame, component_reparations: pl.DataFrame):
    components_df = (
        MasterData.components().filter(pl.col("subcomponent_main")).select(["component_name", "subcomponent_name"])
    )
    cr_df = (
        component_reparations.clone()
        .filter((pl.col("changeout_date").dt.year() >= 2024))
        .join(components_df, how="inner", on=["component_name", "subcomponent_name"])
    )
    cc_df = (
        component_changeouts.clone()
        .filter((pl.col("changeout_date").dt.year() >= 2024))
        .join(components_df, how="inner", on=["component_name", "subcomponent_name"])
    )
    merge_columns = [
        "equipment_name",
        "component_name",
        "subcomponent_name",
        "position_name",
        "changeout_date",
    ]
    df = cr_df.join(
        cc_df.select(merge_columns).with_columns(_merge=pl.lit("both")),
        how="full",
        on=merge_columns,
        coalesce=True,
    )
    assert df.filter(pl.col("_merge").is_null()).height == 0
    df = df.select([*merge_columns, "service_order"]).drop(["subcomponent_name"])
    return df


def merge_ep_input(context, ep_changeouts: pl.DataFrame, ep_input: pl.DataFrame):
    merge_columns = ["equipment_name", "component_name", "position_name", "changeout_date"]

    # Agregar sólo ciertos casos antiguos
    ep_df_old = ep_input.filter(pl.col("changeout_date").dt.year() == 2024).join(
        ep_changeouts.filter(pl.col("changeout_date").dt.year() == 2024), how="left", on=merge_columns
    )
    # Agregar todos los casos posteriores al 2025 y verificar
    ep_df = ep_changeouts.filter(pl.col("changeout_date").dt.year() >= 2025).join(
        ep_input.filter(pl.col("changeout_date").dt.year() >= 2025).with_columns(_merge=pl.lit("both")),
        how="full",
        on=merge_columns,
        coalesce=True,
    )
    check_no_missing_changeouts = ep_df.filter(pl.col("_merge").is_null()).height == 0
    if not check_no_missing_changeouts:
        context.log.critical(
            f"Missing changeouts for {ep_df.filter(pl.col('_merge').is_null()).select(merge_columns).to_dicts()}"
        )

    df = pl.concat([ep_df_old, ep_df.filter(pl.col("_merge").is_not_null()).drop("_merge")], how="diagonal").sort(
        "changeout_date"
    )
    return df


def merge_prorrata(df, raw_prorrata):

    prorrata_df = raw_prorrata.clone()
    df = df.with_columns(
        prorrata_date=pl.when(pl.col("changeout_date").dt.day() >= 15)
        .then(
            # If day is 15th or later, prorrata_date is the 1st of the NEXT month
            pl.col("changeout_date")
            .dt.month_start()  # Get the first day of the current month
            .dt.offset_by("1mo")  # Add one month to get the first day of the next month
        )
        .otherwise(
            # If day is before 15th (1st to 14th), prorrata_date is the 1st of the CURRENT month
            pl.col("changeout_date").dt.month_start()
        )
    )
    prorrata_merge_columns = [
        "equipment_name",
        "component_name",
        "position_name",
        "prorrata_date",
    ]

    df = df.join(
        prorrata_df.select([*prorrata_merge_columns, "prorrata_cost", "prorrata_item"]),
        how="left",
        on=prorrata_merge_columns,
    )
    return df


def merge_so_report(df: pl.DataFrame, so_report: pl.DataFrame):
    df = df.join(
        so_report.unique("service_order").select(
            ["service_order", "latest_quotation_publication", "service_order_status"]
        ),
        how="left",
        on=["service_order"],
    )
    return df


def merge_mean_repair_cost(df):
    datalake = DataLake()
    # msgraph = MSGraph()
    # input_repair_cost_df = msgraph.read_tibble(
    #     "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/INPUTS/repair_cost_input.xlsx"
    # )
    input_repair_cost_df = datalake.read_tibble("az://bhp-raw-data/INPUTS/repair_cost_input.xlsx")
    df = df.join(
        input_repair_cost_df.filter(pl.col("equipment_model") == "960E").select(
            ["component_name", "subcomponent_name", "mean_repair_cost"]
        ),
        how="left",
        on=["component_name", "subcomponent_name"],
    )
    return df


@dg.asset
def ep_input(context: dg.AssetExecutionContext):
    msgraph = MSGraph(context)
    df = msgraph.read_tibble(DATA_CATALOG["ep"]["reference_path"]).drop(["component_icon"])
    return df


def merge_sucomponents(ep_df, component_reparations: pl.DataFrame):

    merge_columns = ["equipment_name", "component_name", "position_name", "changeout_date"]
    df = (
        ep_df.drop(["service_order"])
        .join(
            component_reparations.select(
                [
                    *merge_columns,
                    "subcomponent_name",
                    "component_usage",
                    "service_order",
                    "customer_work_order",
                ]
            ),
            on=merge_columns,
            how="left",
            coalesce=True,
        )
        .sort(
            [
                "changeout_date",
                "equipment_name",
                "component_name",
                "subcomponent_name",
                "position_name",
            ]
        )
    )
    return df


def merge_quotations(df, quotations: pl.DataFrame):
    df = df.join(
        quotations.select(["service_order", "repair_cost", "quotation_updated_dt", "user", "remarks"]),
        how="left",
        on="service_order",
    )
    return df


@dg.asset
def mutate_ep(
    context: dg.AssetExecutionContext,
    component_changeouts: pl.DataFrame,
    component_reparations: pl.DataFrame,
    ep_input: pl.DataFrame,
    raw_prorrata: pl.DataFrame,
    so_report,
    quotations,
):
    dl = DataLake(context)

    df = merge_ccr(component_changeouts, component_reparations)

    df = merge_ep_input(context, df, ep_input)
    initial_height = df.height
    df = merge_prorrata(df, raw_prorrata)
    assert df.height == initial_height
    df = merge_sucomponents(df, component_reparations)

    df = merge_so_report(df, so_report)

    df = merge_quotations(df, quotations)

    df = merge_mean_repair_cost(df)
    df = df.with_columns(component_usage=pl.col("component_usage").cast(pl.Float64).round(decimals=2))

    df = df.with_columns(
        economical_impact=pl.col("component_usage") * pl.col("mean_repair_cost") - pl.col("repair_cost"),
    )
    df = (
        df.with_columns(
            USO=pl.col("component_usage") > 0.9,
            IMPACT=pl.col("economical_impact") < -5000,
        )
        .with_columns(
            ep_category=pl.when(pl.col("USO"))
            .then(
                pl.when(pl.col("IMPACT"))
                .then(pl.lit("PLANIFICADO_SOBRECOSTO"))
                .otherwise(pl.lit("PLANIFICADO_COSTO_MEDIO"))
            )
            .otherwise(
                pl.when(pl.col("IMPACT"))
                .then(pl.lit("IMPREVISTO_SOBRECOSTO"))
                .otherwise(pl.lit("IMPREVISTO_COSTO_MEDIO"))
            )
        )
        .with_columns(
            ep_category=pl.when(pl.col("repair_cost").is_null()).then(pl.lit(None)).otherwise(pl.col("ep_category"))
        )
        .drop(["USO", "IMPACT"])
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
        prorrata_date=pl.col("prorrata_date").dt.to_string("%Y-%m-%d"),
        latest_quotation_publication=pl.col("latest_quotation_publication").dt.to_string("%Y-%m-%d"),
        quotation_updated_dt=pl.col("quotation_updated_dt").dt.to_string("%Y-%m-%d"),
    )

    tidy_ep_df = (
        df.filter(
            ((pl.col("ep_status") != "skipped") & (pl.col("ep_status") != "planificado_bajo_costo"))
            | (pl.col("ep_status").is_null())
        )
        .filter(pl.col("ep_date").is_null())
        .select(
            [
                "equipment_name",
                "component_name",
                "subcomponent_name",
                "position_name",
                "changeout_date",
                "prorrata_date",
                "component_usage",
                "ep_status",
                "raised_by",
                "ep_date",
                "ep_category",
                "latest_quotation_publication",
                "repair_cost",
                "prorrata_cost",
                "mean_repair_cost",
                "customer_work_order",
                "service_order",
                "quotation_updated_dt",
                "remarks",
            ]
        )
        .rename(
            {
                "equipment_name": "Equipo",
                "component_name": "Componente",
                "subcomponent_name": "Subcomponente",
                "position_name": "Posición",
                "changeout_date": "Fecha Cambio",
                "prorrata_date": "Fecha Prorrata",
                "component_usage": "%Uso",
                "ep_status": "Estado EP",
                "raised_by": "Levantado Por",
                "ep_date": "Fecha EP",
                "ep_category": "Clasificación EP",
                "customer_work_order": "OT Cliente",
                "service_order": "OS KRCC",
                "latest_quotation_publication": "Ultima Publicacion Presupuesto",
                "quotation_updated_dt": "Fecha Aprobación Presupuesto",
                "repair_cost": "Costo Reparación",
                "prorrata_cost": "Costo Prorrata",
                "mean_repair_cost": "Costo Reparación Medio",
                "remarks": "Observaciones",
            }
        )
    )

    # msgraph = MSGraph()
    upload_results = []
    upload_results.append(
        msgraph.upload_tibble(
            tibble=df,
            sp_path="sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/CONFIABILIDAD/ep.xlsx",
        )
    )
    upload_results.append(
        # datalake.upload_tibble(tibble=tidy_ep_df, az_path="az://bhp-analytics-data/RELIABILITY/EP/tidy_ep.parquet")
        msgraph.upload_tibble(
            tibble=tidy_ep_df,
            sp_path="sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/CONFIABILIDAD/tidy_ep.xlsx",
        )
    )
    return upload_results
