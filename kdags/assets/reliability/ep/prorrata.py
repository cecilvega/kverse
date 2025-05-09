import dagster as dg
import polars as pl
from kdags.resources.tidyr import *
from datetime import datetime
from io import BytesIO
import pandas as pd

PRORRATA_COMPONENTS_MAPPING = [
    {
        "Componentes": "Blower parrillas",
        "component_name": "blower_parrilla",
        "subcomponent_name": "blower_parrilla",
    },
    {
        "Componentes": "Alternador principal",
        "component_name": "modulo_potencia",
        "subcomponent_name": "alternador_principal",
    },
    {
        "Componentes": "Suspension Trasera",
        "component_name": "suspension_trasera",
        "subcomponent_name": "suspension_trasera",
    },
    {
        "Componentes": "Suspensión Trasera",
        "component_name": "suspension_trasera",
        "subcomponent_name": "suspension_trasera",
    },
    {
        "Componentes": "Motor de tracción",
        "component_name": "motor_traccion",
        "subcomponent_name": "transmision",
    },
    {
        "Componentes": "Conjunto Suspensión Delantera",
        "component_name": "conjunto_maza_suspension",
        "subcomponent_name": "suspension_delantera",
    },
    {
        "Componentes": "Cilindro de levante",
        "component_name": "cilindro_levante",
        "subcomponent_name": "cilindro_levante",
    },
    {
        "Componentes": "Cilindro de Dirección",
        "component_name": "cilindro_direccion",
        "subcomponent_name": "cilindro_direccion",
    },
    {
        "Componentes": "Cilindro de Levante",
        "component_name": "cilindro_levante",
        "subcomponent_name": "cilindro_levante",
    },
    {
        "Componentes": "Blower Parrillas",
        "component_name": "blower_parrilla",
        "subcomponent_name": "blower_parrilla",
    },
]


@dg.asset
def raw_prorrata(context):
    msgraph = MSGraph(context=context)
    file_content = msgraph.read_bytes(
        sp_path="sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/INPUTS/prorrata_input.xlsx"
    )

    df = pd.read_excel(
        BytesIO(file_content),
        sheet_name="Prorrata Componentes Histórica",
        skiprows=16,
        usecols=[
            "N° Equipo",
            "Componentes",
            "Posición",
            "Motivo",
            "Mes Prorrata",
            "Ítem",
            "Costo Prorrata Final (USD)",
        ],
        dtype=str,
    )
    df = pl.from_pandas(df)
    mapping_df = pl.DataFrame(PRORRATA_COMPONENTS_MAPPING)

    df = (
        (
            df.join(mapping_df, on=["Componentes"], how="left")
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
                "Costo Prorrata Final (USD)": "prorrata_cost",
                "Ítem": "prorrata_item",
            }
        )
        .drop(["Componentes"])
        .with_columns(prorrata_cost=pl.col("prorrata_cost").cast(pl.Float64).round(decimals=0))
    )
    assert (
        df.filter(
            pl.struct(["equipment_name", "component_name", "position_name", "prorrata_date"]).is_duplicated()
        ).height
        == 0
    )
    return df
