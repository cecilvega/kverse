import os
from io import BytesIO

import pandas as pd
import unicodedata
import re
import openpyxl
from office365.graph_client import GraphClient
from kdags.resources.msgraph.auth import acquire_token_func

openpyxl.reader.excel.warnings.simplefilter(action="ignore")


def master_components():
    df = pd.DataFrame(
        [
            {"component_code": "mp", "component": "modulo_potencia", "subcomponent": ""},
            {"component_code": "mt", "component": "motor_traccion", "subcomponent": ""},
            {"component_code": "bp", "component": "blower_parrilla", "subcomponent": ""},
            {"component_code": "cl", "component": "cilindro_levante", "subcomponent": ""},
            {"component_code": "st", "component": "suspension_trasera", "subcomponent": ""},
            {"component_code": "sd", "component": "suspension_delantera", "subcomponent": ""},
            {"component_code": "cd", "component": "cilindro_direccion", "subcomponent": ""},
            {"component_code": "ap", "component": "modulo_potencia", "subcomponent": "alternador_principal"},
        ]
    )
    return df


def clean_string(s):
    # Remove accents
    s = str(s)
    if s is not None:

        s = s.lower()
        s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

        # Replace whitespaces with underscore
        s = re.sub(r"\s+", "_", s)

        # Remove all non-alphanumeric characters except underscore
        s = re.sub(r"[^\w]+", "", s)
        s = {"cms": "conjunto_masa_suspension_delantera"}.get(s, s)
    return s


def read_cc():

    graph_client = GraphClient(acquire_token_func)
    site = graph_client.sites.get_by_url("https://globalkomatsu.sharepoint.com/sites/KCHCLSP00022")

    file = (
        site.drive.root.get_by_path(
            "/01. ÁREAS KCH/1.3 PLANIFICACION/01. Gestión pool de componentes/01. Control Cambio Componentes/PLANILLA DE CONTROL CAMBIO DE COMPONENTES.xlsx"
        )
        .get()
        .execute_query()
    )
    content = file.get_content().execute_query().value

    df = pd.read_excel(BytesIO(content), usecols="A:AG").dropna(subset=["FECHA DE CAMBIO"])
    columns_map = {
        "EQUIPO": "equipo",
        "COMPONENTE": "component",
        "SUB COMPONENTE": "subcomponent",
        "POSICION": "position",
        "N/S RETIRADO": "component_serial",
        "W": "changeout_week",
        "FECHA DE CAMBIO": "changeout_date",
        "HORA CC": "component_hours",
        "TBO": "tbo_hours",
        "TIPO CAMBIO POOL": "pool_changeout_type",
    }

    # df = pd.merge(df, master_components(), validate="1:1")

    df = df.rename(columns=columns_map).assign(  # [list(columns_map.keys())]
        equipo=lambda x: x["equipo"].str.extract(r"(\d+)"),
    )
    df = df.dropna(subset=["component"])
    df = df.assign(
        component_serial=df["component_serial"].str.strip().str.replace("\t", ""),
    )

    clean_columns = ["component", "subcomponent"]

    df[clean_columns] = df[clean_columns].apply(lambda x: x.apply(clean_string))

    df = df.assign(
        component=df["component"].map(
            lambda x: {"conjunto_masa_suspension_delantera": "suspension_delantera", "blower": "blower_parrilla"}.get(
                x, x
            )
        )
    )
    df = df.loc[df["component"].isin(master_components()["component"].unique())].reset_index(drop=True)

    df = df.assign(
        changeout_week=lambda x: x["changeout_date"]
        .dt.year.astype(str)
        .str.cat(x["changeout_week"].astype(int).astype(str), sep="-W")
    )
    df = df.loc[df["MODÉLO"].str.contains("960")].reset_index(drop=True)

    return df
