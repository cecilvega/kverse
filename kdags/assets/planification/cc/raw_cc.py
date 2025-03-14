import re
from io import BytesIO

import dagster as dg
import openpyxl
import pandas as pd
import unicodedata

from kdags.resources import MSGraph

# import polars as pl
openpyxl.reader.excel.warnings.simplefilter(action="ignore")

compatibility_mapping = {
    # (COMPONENTE, SUB COMPONENTE): (component_name, subcomponent_name)
    ("alternador_principal", "alternador_principal"): (
        "modulo_potencia",
        "alternador_principal",
    ),
    ("blower_parrilla", "blower_parrilla"): ("blower", None),
    ("blower", "blower"): ("blower", None),
    ("cilindro_direccion", "cilindro_direccion"): ("cilindro_direccion", None),
    ("cilindro_levante", "cilindro_levante"): ("cilindro_levante", None),
    ("suspension_trasera", "suspension_trasera"): ("suspension_trasera", None),
    ("cms", "suspension"): ("conjunto_masa_suspension", "suspension_delantera"),
    ("cms", "suspension_delantera"): (
        "conjunto_masa_suspension",
        "suspension_delantera",
    ),
    ("cms", "masa"): ("conjunto_masa_suspension", "masa"),
    ("cms", "freno_servicio"): ("conjunto_masa_suspension", "freno_servicio_delantero"),
    ("cms", "freno_servicio_delanteros"): (
        "conjunto_masa_suspension",
        "freno_servicio_delantero",
    ),
    ("mdp", "radiador"): ("modulo_potencia", "radiador"),
    ("mdp", "subframe"): ("modulo_potencia", "subframe"),
    ("mdp", "motor_"): ("modulo_potencia", "motor"),
    ("modulo_potencia", "motor"): ("modulo_potencia", "motor"),
    ("mdp", "alternador_principal"): ("modulo_potencia", "alternador_principal"),
    ("motor_traccion", "motor_traccion"): ("motor_traccion", "transmision"),
    ("motor_traccion", "freno_estacionamiento"): (
        "motor_traccion",
        "freno_estacionamiento",
    ),
    ("motor_traccion", "freno_servicio"): ("motor_traccion", "freno_servicio_trasero"),
    ("motor_traccion", "motor_electrico"): ("motor_traccion", "motor_electrico"),
}


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


def apply_component_mapping(row):
    key = (row["COMPONENTE"], row["SUB COMPONENTE"])
    if key in compatibility_mapping:
        return pd.Series(compatibility_mapping[key])
    else:
        # If no mapping found, keep original values
        return pd.Series([row["COMPONENTE"], row["SUB COMPONENTE"]])


@dg.asset
def read_raw_cc():

    msgraph = MSGraph()

    file_content = msgraph.read_bytes(
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

    df[["component_name", "subcomponent_name"]] = df.apply(apply_component_mapping, axis=1)

    df = df.drop(columns=["COMPONENTE", "SUB COMPONENTE"])

    columns_map = {
        "EQUIPO": "equipment_name",
        "POSICION": "position_name",
        "N/S RETIRADO": "removed_component_serial",
        "N/S INSTALADO": "installed_component_serial",
        "W": "changeout_week",
        "FECHA DE CAMBIO": "changeout_date",
        "HORA CC": "component_hours",
        "TBO": "tbo",
        "TIPO CAMBIO POOL": "pool_changeout_type",
        "OS  181": "customer_work_order",
        "MODÉLO": "equipment_model",
        "DESCRIPCIÓN DE FALLA": "failure_description",
        "HORA EQ": "equipment_hours",
    }

    df = df.rename(columns=columns_map).assign(
        equipment_name=lambda x: x["equipment_name"].astype(str).str.extract(r"(\d+)"),
    )
    df = df.dropna(subset=["equipment_name", "changeout_date"])
    df = df.assign(
        removed_component_serial=df["removed_component_serial"].str.strip().str.replace("\t", ""),
        installed_component_serial=df["installed_component_serial"].str.strip().str.replace("\t", ""),
        position_name=df["position_name"].replace({"RH": "derecho", "LH": "izquierdo"}).str.lower(),
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
