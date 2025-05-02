import dagster as dg
import polars as pl
from kdags.resources.tidyr import *
from datetime import datetime


@dg.asset
def mutate_ep(component_changeouts: pl.DataFrame, so_report, changeouts_so, quotations):
    # Definiciones
    msgraph = MSGraph()
    # Inputs
    components_df = MasterData.components()
    cc_df = component_changeouts.clone()
    so_df = so_report.clone()
    cso_df = changeouts_so.clone()
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
    )

    # Aplicamos el filtro por fecha, esto es para hacerlo más fácil de revisar a partir de la fecha que empezamos a controlar los cambios de componentes
    df = df.filter(pl.col("changeout_date") >= datetime.datetime(2024, 9, 26))

    ### ETAPA2: AGREGAR OS140 a planilla haydee

    # realizamos la union de dos df siendo cc_df (la izquierda) y changeouts_so (la derecha) con join, si queremos seleccionar varias columnas tiene que ser de manera listado es decir ["nombre columnas", "nombre columna"].changeouts_so es un df virtual, que solamente sirve para realizar la conexion con la os 140. service_order
    df = df.join(
        cso_df.select(
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
        economical_impact=(pl.col("repair_cost") - (pl.col("component_usage") * pl.col("mean_repair_cost"))).cast(
            pl.Int64
        )
    )

    # Columnas calculadas extras
    df = df.with_columns(
        USO=pl.col("component_usage") > 0.9,
        IMPACT=pl.col("economical_impact") - 3000 > 0,
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
            aplica_ep=pl.when(pl.col("category").is_in(["PLANIFICADO_BAJO_COSTO"]))
            .then(pl.lit(False))
            .otherwise(pl.lit(True))
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
    return df


@dg.asset
def ep(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake()
    df = dl.read_tibble("az://bhp-analytics-data/RELIABILITY/EP/ep.parquet")
    context.log.info(f"Read {df.height} records from ep.parquet.")
    return df


@dg.asset
def publish_sp_ep(mutate_ep: pl.DataFrame):
    df = mutate_ep.clone()

    # TODO: Cambiarle los nombres

    msgraph = MSGraph()
    upload_result = msgraph.upload_tibble(
        tibble=df, sp_path="sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/CONFIABILIDAD/ep.xlsx"
    )
    return upload_result
