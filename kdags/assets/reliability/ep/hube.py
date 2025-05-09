import dagster as dg
import polars as pl
from kdags.resources.tidyr import *
from datetime import datetime
import pandas as pd


@dg.asset
def read_hube(context: dg.AssetExecutionContext):
    msgraph = MSGraph(context=context)
    content = ms
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
