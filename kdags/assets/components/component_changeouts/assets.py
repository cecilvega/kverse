import re
from io import BytesIO

import dagster as dg
import pandas as pd
import polars as pl
import unicodedata

from kdags.resources.tidyr import MSGraph, DataLake
from .constants import *
from kdags.schemas.planning.component_changeouts import COMPONENT_CHANGEOUTS_SCHEMA
from kdags.config import *


@dg.asset(group_name="components")
def raw_component_changeouts_spence(context):
    msgraph = MSGraph(context=context)
    file_content = msgraph.read_bytes(
        sp_path=DATA_CATALOG["component_changeouts"]["reference_path"]["spence"],
    )

    columns = list(
        {
            k.replace("EQUIPO", "CEX").replace("OS  181", "OS").replace("Equipo SAP.1", "Equipo"): v
            for k, v in COLUMN_MAPPING.items()
            if k not in ["MODELO"]
        }.keys()
    )
    df = (
        pl.read_excel(
            BytesIO(file_content),
            sheet_name="Planilla Cambio Componente  980",
            infer_schema_length=0,
            columns=columns,
        )
        .with_columns([pl.lit("980E-5").alias("MODELO")])
        .rename({"CEX": "EQUIPO", "OS": "OS  181", "Equipo": "Equipo SAP.1"})
    )

    return df


@dg.asset(group_name="components")
def raw_component_changeouts_mel(context):
    msgraph = MSGraph(context=context)
    file_content = msgraph.read_bytes(
        sp_path=DATA_CATALOG["component_changeouts"]["reference_path"]["mel"],
    )
    columns = list(COLUMN_MAPPING.keys())
    df = pd.read_excel(
        BytesIO(file_content), sheet_name="Planilla Cambio Componente  960", dtype=str, usecols=columns, nrows=4000
    )
    df = pl.DataFrame(df)

    return df


@dg.asset(group_name="components")
def patched_component_changeouts(context):
    msgraph = MSGraph(context)
    df = (
        msgraph.read_tibble(DATA_CATALOG["patched_component_changeouts"]["reference_path"]).drop_nulls()
        # .with_columns(changeout_date=pl.col("changeout_date").str.to_date("%Y-%m-%d"))
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


def process_component_changeouts(df: pl.DataFrame, site_name: str):
    df = (
        df.with_columns(pl.lit(site_name).alias("site_name"))
        .with_row_index("cc_index")
        .drop_nulls(subset=["FECHA DE CAMBIO"])
    )

    df = df.with_columns(
        [
            pl.col(c).map_elements(clean_string, return_dtype=pl.String).alias(c)
            for c in ["COMPONENTE", "SUB COMPONENTE"]
        ]
    )

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
            pl.coalesce(pl.col("new_component_name"), pl.col("COMPONENTE")).alias("COMPONENTE"),
            pl.coalesce(pl.col("new_subcomponent_name"), pl.col("SUB COMPONENTE")).alias("SUB COMPONENTE"),
        ]
    ).drop(["new_component_name", "new_subcomponent_name"])

    df = df.rename(COLUMN_MAPPING)

    # Extract equipment_name digits
    df = df.with_columns(pl.col("equipment_name").cast(pl.Utf8).str.extract(r"(\d+)").alias("equipment_name"))

    # Process various columns
    df = (
        df.with_columns(
            [
                pl.col("component_serial").str.strip_chars().str.replace("\t", ""),
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
        )
        # .filter(pl.col("equipment_model") != "PC8000")
        .with_columns(
            equipment_model=pl.col("equipment_model").replace({"960E-1": "960E", "960E-2": "960E"})
        ).with_columns(
            [
                pl.col(c).str.strip_suffix(".0").cast(pl.Int64, strict=False).fill_null(-1).alias(c)
                for c in ["sap_equipment_name", "installed_sap_equipment_name"]
            ]
        )
    )

    # Map equipment model to prefix and concatenate with equipment_name
    model_mapping = {"980E-5": "CEX", "960E": "TK", "930E-4": "TK"}
    df = df.with_columns(
        (pl.col("equipment_model").replace(model_mapping) + pl.col("equipment_name").cast(pl.String)).alias(
            "equipment_name"
        )
    )
    return df


@dg.asset(
    group_name="components",
    metadata={
        "dagster/column_schema": dg.TableSchema(
            columns=[
                dg.TableColumn(
                    "name",
                    "string",
                    description="The name of the person",
                ),
                dg.TableColumn(
                    "age",
                    "int",
                    description="The age of the person",
                ),
            ]
        )
    },
)
def mutate_component_changeouts(
    context: dg.AssetExecutionContext,
    raw_component_changeouts_mel: pl.DataFrame,
    raw_component_changeouts_spence: pl.DataFrame,
    patched_component_changeouts: pl.DataFrame,
):
    mel_df = raw_component_changeouts_mel.clone().pipe(process_component_changeouts, site_name="MEL")
    spence_df = raw_component_changeouts_spence.clone().pipe(process_component_changeouts, site_name="SPENCE")
    df = pl.concat([mel_df, spence_df], how="diagonal")
    # Reparar serie componentes
    cs_columns = ["component_serial", "installed_component_serial"]
    df = df.with_columns([pl.col(c).str.strip_prefix("#").alias(c) for c in cs_columns])
    df = df.join(
        patched_component_changeouts,
        on=["equipment_name", "component_name", "subcomponent_name", "position_name", "changeout_date"],
        how="left",
        suffix="_patched",
    ).with_columns(
        component_usage=pl.col("component_usage").cast(pl.Float64).round(2),
        component_hours=pl.col("component_hours").cast(pl.Float64).round(0),
    )
    patched_columns = ["component_serial", "installed_component_serial"]
    df = df.with_columns(
        [
            pl.when(pl.col(f"{c}_patched").is_not_null()).then(pl.col(f"{c}_patched")).otherwise(pl.col(c)).alias(c)
            for c in patched_columns
        ]
    ).drop([f"{c}_patched" for c in patched_columns])

    datalake = DataLake(context=context)  # Direct instantiation
    datalake.upload_tibble(tibble=df, az_path=DATA_CATALOG["component_changeouts"]["analytics_path"])

    return df


@dg.asset(
    group_name="readr",
    description="Reads the consolidated oil analysis data from the ADLS analytics layer.",
    metadata={"dagster/column_schema": COMPONENT_CHANGEOUTS_SCHEMA},
)
def component_changeouts(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake(context=context)
    df = dl.read_tibble(az_path=DATA_CATALOG["component_changeouts"]["analytics_path"])
    return df
