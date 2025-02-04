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
        s = s.rstrip("_")
    return s


def read_cc():

    # graph_client = GraphClient(acquire_token_func)
    # MEL
    # site = graph_client.sites.get_by_url("https://globalkomatsu.sharepoint.com/sites/KCHCLSP00022")
    # file = (
    #     site.drive.root.get_by_path(
    #         "/01. ÁREAS KCH/1.3 PLANIFICACION/01. Gestión pool de componentes/01. Control Cambio Componentes/PLANILLA DE CONTROL CAMBIO DE COMPONENTES.xlsx"
    #     )
    #     .get()
    #     .execute_query()
    # )
    # content = file.get_content().execute_query().value
    # mel_df = pd.read_excel(BytesIO(content), usecols="A:AG", dtype={"MODÉLO": str}).dropna(subset=["FECHA DE CAMBIO"])

    # SPENCE
    # site = graph_client.sites.get_by_url("https://globalkomatsu.sharepoint.com/sites/KCHCLSP00060")
    # file = (
    #     site.drive.root.get_by_path(
    #         "/1.-%20Gesti%C3%B3n%20de%20Componentes/2.-%20Spence/1.-%20Planilla%20Control%20cambio%20de%20componentes/Planilla%20control%20cambio%20componentes/NUEVA%20PLANILLA%20DE%20CONTROL%20CAMBIO%20DE%20COMPONENTES%20SPENCE.xlsx"
    #     )
    #     .get()
    #     .execute_query()
    # )
    # content = file.get_content().execute_query().value
    # spence_df = pd.read_excel(BytesIO(content), usecols="A:AG", dtype={"MODÉLO": str}).dropna(
    #     subset=["FECHA DE CAMBIO"]
    # )
    columns = [
        "EQUIPO",
        "COMPONENTE",
        "SUB COMPONENTE",
        "MODÉLO",
        "POSICION",
        "FECHA DE CAMBIO",
        "TIPO DE CAMBIO",
        "HORA EQ",
        "HORA CC",
        "TBO",
        "USO",
        "DESCRIPCIÓN DE FALLA",
        "N/S RETIRADO",
        "N/S INSTALADO",
        "OS  181",
        "%Vida Util",
        "Costo Real x Hrs",
        "Costo N/S RETIRADO",
        "Decisión N/S RETIRADO",
        "Resolución Interna",
        "Estatus RCA",
        "subcomponente afectado",
        "Descripción",
        "% Vida util",
        "Presupuesto x Hr",
        "Costo Real x Hrs",
        "Delta",
        "Costo proyectado",
        "Real Perdidas",
        "Valor venta",
        "Costo Medio MEL",
        "Perdida",
        "Gestión",
    ]
    mel_df = (
        pd.read_excel(
            "/home/cecilvega/Downloads/PILI/W4 PLANILLA DE CONTROL CAMBIO DE COMPONENTES.xlsx",
            sheet_name="Planilla Cambio Componente  960",
            dtype={"MODÉLO": str},
        )
        .filter(columns)
        .assign(site_name="MEL")
        .rename_axis("cc_index")
        .reset_index()
    )
    print(mel_df.columns)
    spence_df = (
        pd.read_excel(
            "/home/cecilvega/Downloads/PILI/W4 NUEVA PLANILLA DE CONTROL CAMBIO DE COMPONENTES SPENCE.xlsx",
            sheet_name="Planilla Cambio Componente  980",
            dtype={"MODÉLO": str},
        )
        .filter(columns)
        .assign(site_name="SPENCE")
        .rename_axis("cc_index")
        .reset_index()
    )

    compatibilidad_pili = pd.read_excel("/home/cecilvega/Downloads/PILI/COMPATIBILIDAD_PILI.xlsx")

    df = pd.concat([mel_df, spence_df], ignore_index=True)

    clean_columns = ["COMPONENTE", "SUB COMPONENTE"]

    df[clean_columns] = df[clean_columns].apply(lambda x: x.apply(clean_string))

    df = pd.merge(df, compatibilidad_pili, on=["COMPONENTE", "SUB COMPONENTE"], how="left").drop(
        columns=["COMPONENTE", "SUB COMPONENTE"]
    )

    columns_map = {
        "EQUIPO": "equipment_name",
        # "COMPONENTE": "component_name",
        # "SUB COMPONENTE": "subcomponent_name",
        "POSICION": "position_name",
        "N/S RETIRADO": "component_serial",
        "W": "changeout_week",
        "FECHA DE CAMBIO": "changeout_date",
        "HORA CC": "component_hours",
        "TBO": "tbo_hours",
        "TIPO CAMBIO POOL": "pool_changeout_type",
        "OS  181": "os_180",
        "MODÉLO": "equipment_model",
    }

    # df = pd.merge(df, master_components(), validate="1:1")

    df = df.rename(columns=columns_map).assign(  # [list(columns_map.keys())]
        equipment_name=lambda x: x["equipment_name"].astype(str).str.extract(r"(\d+)"),
    )
    df = df.dropna(subset=["equipment_name", "changeout_date"])
    df = df.assign(
        component_serial=df["component_serial"].str.strip().str.replace("\t", ""),
        position_name=df["position_name"].replace({"RH": "DERECHO", "LH": "IZQUIERDO"}),
    )

    df["equipment_name"] = (
        df["equipment_model"].map({"980E-5": "CEX", "960E-2": "TK", "960E-1": "TK", "930E-4": "TK"}).fillna("")
        + df["equipment_name"]
    )

    # df = df.loc[df["component_name"].isin(master_components()["component"].unique())].reset_index(drop=True)

    # df = df.assign(
    # changeout_week=lambda x: x["changeout_date"]
    # .dt.year.astype(str)
    # .str.cat(x["changeout_week"].astype(int).astype(str), sep="-W"),
    # )
    # df = df.loc[df["MODÉLO"].str.contains("960")].reset_index(drop=True)
    df = df.loc[df["changeout_date"].dt.year >= 2024].reset_index(drop=True)

    df = pd.merge(
        df,
        pd.read_csv("/home/cecilvega/Downloads/PILI/FILTER.csv"),
        how="inner",
        on=["component_name", "subcomponent_name"],
    ).reset_index(drop=True)

    return df
