import re
import unicodedata
from io import BytesIO

import openpyxl
import pandas as pd
import numpy as np
from kdags.assets.planification.cc.compatibility_data import compatibility_data
from kdags.resources.msgraph import MSGraph
import dagster as dg

# import polars as pl
openpyxl.reader.excel.warnings.simplefilter(action="ignore")


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


@dg.asset
def read_raw_cc():

    msgraph = MSGraph()

    file_content = msgraph.get_sharepoint_file_content(
        site_id="KCHCLSP00022",
        file_path="/01. ÁREAS KCH/1.3 PLANIFICACION/01. Gestión pool de componentes/01. Control Cambio Componentes/PLANILLA DE CONTROL CAMBIO DE COMPONENTES.xlsx",
    )

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
    ]
    df = (
        pd.read_excel(
            BytesIO(file_content),
            sheet_name="Planilla Cambio Componente  960",
            dtype={"MODÉLO": str},
        )
        .filter(columns)
        .assign(site_name="MEL")
        .rename_axis("cc_index")
        .reset_index()
    )

    clean_columns = ["COMPONENTE", "SUB COMPONENTE"]

    df[clean_columns] = df[clean_columns].apply(lambda x: x.apply(clean_string))

    df = pd.merge(
        df,
        pd.DataFrame(compatibility_data),
        on=["COMPONENTE", "SUB COMPONENTE"],
        how="left",
    )
    df = df.assign(
        component_name=np.where(df["component_name"].isnull(), df["COMPONENTE"], df["component_name"]),
        subcomponent_name=np.where(
            df["subcomponent_name"].isnull(),
            df["SUB COMPONENTE"],
            df["subcomponent_name"],
        ),
    )

    df = df.drop(columns=["COMPONENTE", "SUB COMPONENTE"])

    columns_map = {
        "EQUIPO": "equipment_name",
        "POSICION": "position_name",
        "N/S RETIRADO": "component_serial",
        "W": "changeout_week",
        "FECHA DE CAMBIO": "changeout_date",
        "HORA CC": "component_hours",
        "TBO": "tbo_hours",
        "TIPO CAMBIO POOL": "pool_changeout_type",
        "OS  181": "customer_work_order",
        "MODÉLO": "equipment_model",
    }

    df = df.rename(columns=columns_map).assign(
        equipment_name=lambda x: x["equipment_name"].astype(str).str.extract(r"(\d+)"),
    )
    df = df.dropna(subset=["equipment_name", "changeout_date"])
    df = df.assign(
        component_serial=df["component_serial"].str.strip().str.replace("\t", ""),
        position_name=df["position_name"].replace({"RH": "DERECHO", "LH": "IZQUIERDO"}),
        customer_work_order=pd.to_numeric(
            df["customer_work_order"].astype(str).str.extract(r"(\d+)", expand=False).fillna(-1).astype(int),
            errors="coerce",
        ),
    )

    df["equipment_name"] = (
        df["equipment_model"].map({"980E-5": "CEX", "960E-2": "TK", "960E-1": "TK", "930E-4": "TK"}).fillna("")
        + df["equipment_name"]
    )

    return df
