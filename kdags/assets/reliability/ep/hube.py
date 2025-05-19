import dagster as dg
import polars as pl
from kdags.resources.tidyr import *
from datetime import datetime
import pandas as pd


def read_hube(context: dg.AssetExecutionContext):
    msgraph = MSGraph(context=context)
    # content = ms
    hube_df = pd.read_excel(
        r"hube.xlsx",
        sheet_name="Prorrata Histórica",
        skiprows=3,
        usecols=[
            "ep_status",
            "N° Equipo",
            "Componente",
            "Posición",
            # "Motivo",
            "Mes Prorrata",
        ],
        dtype=str,
    ).query("ep_status.notnull()")
    hube_df = pl.from_pandas(hube_df)
    map_list = [
        {
            "Componente": "Blower parrillas",
            "component_name": "blower_parrilla",
            "subcomponent_name": "blower_parrilla",
        },
        {
            "Componente": "Alternador principal",
            "component_name": "modulo_potencia",
            "subcomponent_name": "alternador_principal",
        },
        {
            "Componente": "Suspension Trasera",
            "component_name": "suspension_trasera",
            "subcomponent_name": "suspension_trasera",
        },
        {
            "Componente": "Suspensión Trasera",
            "component_name": "suspension_trasera",
            "subcomponent_name": "suspension_trasera",
        },
        {
            "Componente": "Motor de tracción",
            "component_name": "motor_traccion",
            "subcomponent_name": "transmision",
        },
        {
            "Componente": "Conjunto Suspensión Delantera",
            "component_name": "conjunto_maza_suspension",
            "subcomponent_name": "suspension_delantera",
        },
        {
            "Componente": "Cilindro de levante",
            "component_name": "cilindro_levante",
            "subcomponent_name": "cilindro_levante",
        },
        {
            "Componente": "Cilindro de Dirección",
            "component_name": "cilindro_direccion",
            "subcomponent_name": "cilindro_direccion",
        },
        {
            "Componente": "Cilindro de Levante",
            "component_name": "cilindro_levante",
            "subcomponent_name": "cilindro_levante",
        },
        {
            "Componente": "Blower Parrillas",
            "component_name": "blower_parrilla",
            "subcomponent_name": "blower_parrilla",
        },
    ]

    mapping_df = pl.DataFrame(map_list)

    hube_df = (
        (
            hube_df.join(mapping_df, on=["Componente"], how="left")
            .filter(pl.col("component_name").is_not_null())
            .with_columns(pl.col("Mes Prorrata").str.strptime(pl.Date, "%Y-%m-%d %H:%M:%S"))
            .filter(pl.col("Mes Prorrata").dt.year() >= 2024)
            .with_columns(
                pl.col("Posición").replace({"IZQUIERDO": "izquierdo", "DERECHO": "derecho", "UNICA": "unico"})
            )
        )
        .rename(
            {
                "Mes Prorrata": "prorrata_date",
                "N° Equipo": "equipment_name",
                "Posición": "position_name",
            }
        )
        .drop(["Componente"])
    )
    hube_df


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
