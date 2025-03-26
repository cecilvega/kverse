import re
from io import BytesIO

import dagster as dg
import openpyxl
import pandas as pd
import unicodedata
import os
from pathlib import Path
import polars as pl

# from kdags.resources.tidyr import MSGraph

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


def apply_component_mapping(row_dict):
    key = (row_dict["COMPONENTE"], row_dict["SUB COMPONENTE"])
    if key in compatibility_mapping:
        return {"component_name": compatibility_mapping[key][0], "subcomponent_name": compatibility_mapping[key][1]}
    else:
        # If no mapping found, keep original values
        return {"component_name": row_dict["COMPONENTE"], "subcomponent_name": row_dict["SUB COMPONENTE"]}


@dg.asset
def read_cc():
    file_bytes = open(
        Path(os.environ["ONEDRIVE_LOCAL_PATH"])
        / "01. Control Cambio Componentes/PLANILLA DE CONTROL CAMBIO DE COMPONENTES.xlsx",
        "rb",
    ).read()

    columns = [
        "EQUIPO",
        "COMPONENTE",
        "SUB COMPONENTE",
        "MODÉLO",
        "POSICION",
        "FECHA DE CAMBIO",
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
        pl.read_excel(
            BytesIO(file_bytes),
            sheet_name="Planilla Cambio Componente  960",
            infer_schema_length=0,
        )
        .select(columns)  # equivalent to filter(columns)
        .with_columns(pl.lit("MEL").alias("site_name"))
        .with_row_index("cc_index")  # equivalent to rename_axis + reset_index
    )

    clean_columns = ["COMPONENTE", "SUB COMPONENTE"]

    # Clean columns
    for col in clean_columns:
        df = df.with_columns(pl.col(col).map_elements(clean_string).alias(col))

    # Convert the mapping dictionary to a DataFrame
    mapping_data = [(k[0], k[1], v[0], v[1]) for k, v in compatibility_mapping.items()]
    mapping_df = pl.DataFrame(
        mapping_data, schema=["COMPONENTE", "SUB COMPONENTE", "component_name", "subcomponent_name"]
    )

    # Join with the mapping DataFrame
    df = df.join(mapping_df, on=["COMPONENTE", "SUB COMPONENTE"], how="left")

    # Fill missing values with original values
    df = df.with_columns(
        [
            pl.col("component_name").fill_null(pl.col("COMPONENTE")),
            pl.col("subcomponent_name").fill_null(pl.col("SUB COMPONENTE")),
        ]
    )

    # Drop columns
    df = df.drop("COMPONENTE", "SUB COMPONENTE")

    # Rename columns
    columns_map = {
        "EQUIPO": "equipment_name",
        "POSICION": "position_name",
        "N/S RETIRADO": "removed_component_serial",
        "N/S INSTALADO": "installed_component_serial",
        "FECHA DE CAMBIO": "changeout_date",
        "HORA CC": "component_hours",
        "TBO": "tbo",
        "OS  181": "customer_work_order",
        "MODÉLO": "equipment_model",
        "DESCRIPCIÓN DE FALLA": "failure_description",
        "HORA EQ": "equipment_hours",
        "USO": "component_usage",
    }

    df = df.rename(columns_map)

    # Extract equipment_name digits
    df = df.with_columns(pl.col("equipment_name").cast(pl.Utf8).str.extract(r"(\d+)").alias("equipment_name"))

    # Drop rows with null values in specific columns
    df = df.filter(pl.col("equipment_name").is_not_null() & pl.col("changeout_date").is_not_null())

    # Process various columns
    df = df.with_columns(
        [
            pl.col("removed_component_serial").str.strip_chars().str.replace("\t", ""),
            pl.col("installed_component_serial").str.strip_chars().str.replace("\t", ""),
            pl.col("position_name").replace({"RH": "derecho", "LH": "izquierdo"}).str.to_lowercase(),
            pl.col("customer_work_order")
            .cast(pl.Utf8)
            .str.extract(r"(\d+)")
            .fill_null("-1")
            .cast(pl.Int64, strict=False),
            pl.col("equipment_hours").cast(pl.Float64, strict=False).alias("equipment_hours"),
        ]
    )

    # Map equipment model to prefix and concatenate with equipment_name
    model_mapping = {"980E-5": "CEX", "960E-2": "TK", "960E-1": "TK", "930E-4": "TK"}
    df = df.with_columns(
        (pl.col("equipment_model").map_elements(lambda x: model_mapping.get(x, "")) + pl.col("equipment_name")).alias(
            "equipment_name"
        )
    )

    return df


#
# def apply_component_mapping(row):
#     key = (row["COMPONENTE"], row["SUB COMPONENTE"])
#     if key in compatibility_mapping:
#         return pd.Series(compatibility_mapping[key])
#     else:
#         # If no mapping found, keep original values
#         return pd.Series([row["COMPONENTE"], row["SUB COMPONENTE"]])
#
#
# @dg.asset
# def read_cc():
#
#     file_bytes = open(
#         Path(os.environ["ONEDRIVE_LOCAL_PATH"])
#         / "01. Control Cambio Componentes/PLANILLA DE CONTROL CAMBIO DE COMPONENTES.xlsx",
#         "rb",
#     ).read()
#
#     # msgraph = MSGraph()
#     #
#     # file_content = msgraph.read_bytes(
#     #     site_id="KCHCLSP00022",
#     #     filepath="/01. ÁREAS KCH/1.3 PLANIFICACION/01. Gestión pool de componentes/01. Control Cambio Componentes/PLANILLA DE CONTROL CAMBIO DE COMPONENTES.xlsx",
#     # )
#
#     columns = [
#         "EQUIPO",
#         "COMPONENTE",
#         "SUB COMPONENTE",
#         "MODÉLO",
#         "POSICION",
#         "FECHA DE CAMBIO",
#         "TIPO DE CAMBIO",
#         "HORA EQ",
#         "HORA CC",
#         "TBO",
#         "USO",
#         "DESCRIPCIÓN DE FALLA",
#         "N/S RETIRADO",
#         "N/S INSTALADO",
#         "OS  181",
#     ]
#
#     df = (
#         pd.read_excel(
#             BytesIO(file_bytes),
#             sheet_name="Planilla Cambio Componente  960",
#             dtype={"MODÉLO": str},
#         )
#         .filter(columns)
#         .assign(site_name="MEL")
#         .rename_axis("cc_index")
#         .reset_index()
#     )
#
#     clean_columns = ["COMPONENTE", "SUB COMPONENTE"]
#
#     df[clean_columns] = df[clean_columns].apply(lambda x: x.apply(clean_string))
#
# df[["component_name", "subcomponent_name"]] = df.apply(apply_component_mapping, axis=1)
#
#     df = df.drop(columns=["COMPONENTE", "SUB COMPONENTE"])
#
#     columns_map = {
#         "EQUIPO": "equipment_name",
#         "POSICION": "position_name",
#         "N/S RETIRADO": "removed_component_serial",
#         "N/S INSTALADO": "installed_component_serial",
#         "W": "changeout_week",
#         "FECHA DE CAMBIO": "changeout_date",
#         "HORA CC": "component_hours",
#         "TBO": "tbo",
#         "TIPO CAMBIO POOL": "pool_changeout_type",
#         "OS  181": "customer_work_order",
#         "MODÉLO": "equipment_model",
#         "DESCRIPCIÓN DE FALLA": "failure_description",
#         "HORA EQ": "equipment_hours",
#     }
#
#     df = df.rename(columns=columns_map).assign(
#         equipment_name=lambda x: x["equipment_name"].astype(str).str.extract(r"(\d+)"),
#     )
#     df = df.dropna(subset=["equipment_name", "changeout_date"])
#     df = df.assign(
#         removed_component_serial=df["removed_component_serial"].str.strip().str.replace("\t", ""),
#         installed_component_serial=df["installed_component_serial"].str.strip().str.replace("\t", ""),
#         position_name=df["position_name"].replace({"RH": "derecho", "LH": "izquierdo"}).str.lower(),
#         customer_work_order=pd.to_numeric(
#             df["customer_work_order"].astype(str).str.extract(r"(\d+)", expand=False).fillna(-1).astype(int),
#             errors="coerce",
#         ),
#     )
#
#     df["equipment_name"] = (
#         df["equipment_model"].map({"980E-5": "CEX", "960E-2": "TK", "960E-1": "TK", "930E-4": "TK"}).fillna("")
#         + df["equipment_name"]
#     )
#
#     return df
