import re
from io import BytesIO

import dagster as dg
import openpyxl
import pandas as pd
import unicodedata
import os
from pathlib import Path
import polars as pl
from .constants import COMPATIBILITY_MAPPING
from kdags.resources.tidyr import MSGraph, DataLake


COMPONENT_CHANGEOUTS_ANALYTIS_PATH = (
    "abfs://bhp-analytics-data/PLANNING/COMPONENT_CHANGEOUTS/component_changeouts.parquet"
)


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
def raw_cc():
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
        "HORA EQ",
        "HORA CC",
        "TBO",
        "USO",
        "DESCRIPCIÓN DE FALLA",
        "N/S RETIRADO",
        "N/S INSTALADO",
        "OS  181",
    ]
    df = pl.read_excel(
        BytesIO(file_content), sheet_name="Planilla Cambio Componente  960", infer_schema_length=0, columns=columns
    )
    return df


@dg.asset
def components_changeouts(context: dg.AssetExecutionContext, raw_cc: pl.DataFrame):
    df = (
        raw_cc.clone().with_columns(pl.lit("MEL").alias("site_name")).with_row_index("cc_index")
    )  # equivalent to rename_axis + reset_index

    clean_columns = ["COMPONENTE", "SUB COMPONENTE"]

    # Clean columns
    for col in clean_columns:
        df = df.with_columns(pl.col(col).map_elements(clean_string).alias(col))

    # 1. Create a mapping DataFrame
    map_list = [
        {
            "COMPONENTE": k[0],
            "SUB COMPONENTE": k[1],
            "new_component_name": v[0],
            "new_subcomponent_name": v[1],
        }
        for k, v in COMPATIBILITY_MAPPING.items()
    ]
    mapping_df = pl.DataFrame(map_list)

    # 2. Perform a left join
    df = df.join(mapping_df, on=["COMPONENTE", "SUB COMPONENTE"], how="left")

    # 3. Coalesce the results: use the new name if found, otherwise keep the original
    df = df.with_columns(
        [
            pl.coalesce(pl.col("new_component_name"), pl.col("COMPONENTE")).alias("component_name"),
            pl.coalesce(pl.col("new_subcomponent_name"), pl.col("SUB COMPONENTE")).alias("subcomponent_name"),
        ]
    )

    # 4. Drop temporary and original columns
    df = df.drop(["COMPONENTE", "SUB COMPONENTE", "new_component_name", "new_subcomponent_name"])

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
    df = df.drop_nulls(subset=["equipment_name", "position_name", "changeout_date"])

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
            pl.col("changeout_date").str.to_date("%Y-%m-%d %H:%M:%S", strict=False).alias("changeout_date"),
        ]
    ).filter(pl.col("equipment_model") != "PC8000")

    # Map equipment model to prefix and concatenate with equipment_name
    model_mapping = {"980E-5": "CEX", "960E-2": "TK", "960E-1": "TK", "930E-4": "TK"}
    df = df.with_columns(
        (pl.col("equipment_model").map_elements(lambda x: model_mapping.get(x, "")) + pl.col("equipment_name")).alias(
            "equipment_name"
        )
    )
    datalake = DataLake()  # Direct instantiation
    context.log.info(f"Writing {df.height} records to {COMPONENT_CHANGEOUTS_ANALYTIS_PATH}")

    datalake.upload_tibble(df=df, abfs_path=COMPONENT_CHANGEOUTS_ANALYTIS_PATH, format="parquet")
    context.add_output_metadata(
        {  # Add metadata on success
            "abfs_path": COMPONENT_CHANGEOUTS_ANALYTIS_PATH,
            "rows_written": df.height,
        }
    )

    return df


@dg.asset(
    description="Reads the consolidated oil analysis data from the ADLS analytics layer.",
)
def read_component_changeouts(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake()
    if dl.abfs_path_exists(COMPONENT_CHANGEOUTS_ANALYTIS_PATH):
        df = dl.read_tibble(abfs_path=COMPONENT_CHANGEOUTS_ANALYTIS_PATH)
        context.log.info(f"Read {df.height} records from {COMPONENT_CHANGEOUTS_ANALYTIS_PATH}.")
        return df
    else:
        context.log.warning(f"Data file not found at {COMPONENT_CHANGEOUTS_ANALYTIS_PATH}. Returning empty DataFrame.")
        return pl.DataFrame()
