import dagster as dg
import polars as pl
from kdags.resources.tidyr import *
from datetime import datetime

EP_ANALYTICS_PATH = "az://bhp-analytics-data/RELIABILITY/EP/ep.parquet"


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
    assert ep_df.filter(pl.col("_merge").is_null()).height == 0, (
        ep_df.filter(pl.col("_merge").is_null()).select(merge_columns).to_dicts()
    )
    df = pl.concat([ep_df_old, ep_df.drop("_merge")], how="diagonal").sort("changeout_date")
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
    msgraph = MSGraph()
    input_repair_cost_df = msgraph.read_tibble(
        "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/INPUTS/repair_cost_input.xlsx"
    )
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
    msgraph = MSGraph()
    df = msgraph.read_tibble(
        "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/INPUTS/ep_input.xlsx"
    ).drop(["component_icon"])
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

    dl.upload_tibble(tibble=df, az_path=EP_ANALYTICS_PATH)
    return df


def mutate_ep_deprecated(component_changeouts: pl.DataFrame, so_report, component_reparations, quotations):
    # Definiciones
    msgraph = MSGraph()
    # Inputs
    components_df = MasterData.components()
    cc_df = component_changeouts.clone()
    so_df = so_report.clone()
    cr_df = component_reparations.clone()
    quotations_df = quotations.clone()
    input_repair_cost_df = msgraph.read_tibble(
        "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/INPUTS/repair_cost_input.xlsx"
    )
    ep_input_df = msgraph.read_tibble(
        "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/CONFIABILIDAD/EP/ep_input.xlsx"
    )

    # Proceso
    # Lista que representa las columna necesarias para definir un cambio de componente
    changeout_columns = [
        "equipment_name",
        "component_name",
        "subcomponent_name",
        "position_name",
        "changeout_date",
    ]

    # selecionamos las columnas del df cc (planilla Haydee) con select
    df = cc_df.select(
        [
            # Esta es una operación del cecil para sumar una lista con otros elementos (notar el *)
            *changeout_columns,
            "equipment_model",
            "component_usage",
        ]
    ).with_columns(component_usage=pl.col("component_usage").cast(pl.Float64))

    # Agregar si es candidato a EP o No
    df = df.join(
        components_df.select(["component_name", "subcomponent_name", "subcomponent_ep"]),
        on=["component_name", "subcomponent_name"],
        how="left",
    ).with_columns(subcomponent_ep=pl.col("subcomponent_ep").fill_null(pl.lit(False)))

    # Aplicamos el filtro por fecha, esto es para hacerlo más fácil de revisar a partir de la fecha que empezamos a controlar los cambios de componentes
    df = df.filter(
        (pl.col("changeout_date") >= datetime(2024, 9, 26))
        | (
            pl.col("cc_index").is_in(
                [
                    2618,
                    2619,
                    2620,
                    2621,
                    2647,
                    2648,
                    2649,
                    2650,
                    2496,
                    2497,
                    2498,
                    2675,
                    2730,
                ]
            )
        )
    )

    ### ETAPA2: AGREGAR OS140 a planilla haydee

    # realizamos la union de dos df siendo cc_df (la izquierda) y changeouts_so (la derecha) con join, si queremos seleccionar varias columnas tiene que ser de manera listado es decir ["nombre columnas", "nombre columna"].changeouts_so es un df virtual, que solamente sirve para realizar la conexion con la os 140. service_order
    df = df.join(
        cr_df.select(
            [
                "service_order",
                *changeout_columns,
            ]
        ),
        how="left",
        on=changeout_columns,
    )
    df = df.join(
        so_df.select(
            [
                "service_order",
                "approval_date",
                "first_quotation_publication",
                "service_order_status",
                "component_status",
            ]
        ),
        how="left",
        on="service_order",
    )
    ###

    ### AGREGAR DATOS DE PRESUPUESTO
    # aca agregamos el amount que es la columna que queres extraer de quotations y server_order es la conexion con el df
    df = df.join(
        quotations_df.select(["service_order", "repair_cost"]),
        how="left",
        on="service_order",
    )
    ###

    ### AGREGAR INPUT COSTO MEDIO DE REPARACION
    df = df.join(
        input_repair_cost_df,
        how="left",
        on=["equipment_model", "component_name", "subcomponent_name"],
    ).drop(["site_name", "equipment_model"])
    ###

    #### Calculando costos
    df = df.with_columns(
        repair_cost=pl.col("repair_cost").cast(pl.Int64),
        mean_repair_cost=pl.col("mean_repair_cost").cast(pl.Int64),
    ).with_columns(
        economical_impact=(pl.col("repair_cost") - pl.col("component_usage") * pl.col("mean_repair_cost")).cast(
            pl.Int64
        )
    )

    # Columnas calculadas extras
    df = df.with_columns(
        USO=pl.col("component_usage") > 0.9,
        IMPACT=pl.col("economical_impact") - 5000 > 0,
    )
    df = (
        df.with_columns(
            category=pl.when(pl.col("USO"))
            .then(
                pl.when(pl.col("IMPACT"))
                .then(pl.lit("PLANIFICADO_ALTO_COSTO"))
                .otherwise(pl.lit("PLANIFICADO_BAJO_COSTO"))
            )
            .otherwise(
                pl.when(pl.col("IMPACT"))
                .then(pl.lit("IMPREVISTO_ALTO_COSTO"))
                .otherwise(pl.lit("IMPREVISTO_BAJO_COSTO"))
            )
        )
        .with_columns(
            aplica_ep=pl.when(~pl.col("category").is_in(["PLANIFICADO_BAJO_COSTO"]) & (pl.col("subcomponent_ep")))
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
        )
        .with_columns(
            prioridad_inicial=pl.when(pl.col("category") == "PLANIFICADO_ALTO_COSTO")
            .then(pl.lit(1, dtype=pl.Int8))
            .when(pl.col("category") == "IMPREVISTO_ALTO_COSTO")
            .then(pl.lit(2, dtype=pl.Int8))
            .when(pl.col("category") == "IMPREVISTO_BAJO_COSTO")  # PUEDE SER QUE EL IMPACTO SEA POSIITIVO
            .then(pl.lit(3, dtype=pl.Int8))
            .otherwise(pl.lit(4, dtype=pl.Int8))  # Assign null for unspecified categories
        )
    )

    ### Agregar información de input manual del EP en sharepoint
    df = df.join(
        ep_input_df.select(
            [
                *changeout_columns,
                "pieza_afectada",
                "failure_effect",
                "failure_mechanism",
                "failure_mode",
                "root_cause",
                "failure_analysis",
                "factibilidad_cobro",
                "dificultad_tecnica",
            ]
        ),
        how="left",
        on=changeout_columns,
    )
    df = df.with_columns(component_usage=(pl.col("component_usage") * 100).cast(pl.Int64))
    return df


@dg.asset
def ep(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake(context=context)
    df = dl.read_tibble(EP_ANALYTICS_PATH)

    return df


@dg.asset
def publish_sp_ep(mutate_ep: pl.DataFrame):
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

    msgraph = MSGraph()
    upload_results = []
    upload_results.append(
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
    return upload_results
