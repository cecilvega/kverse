import re
from io import BytesIO

import dagster as dg
import pandas as pd
import polars as pl
import unicodedata

from kdags.resources.tidyr import MSGraph, DataLake
from .constants import *
from kdags.schemas.planning.component_changeouts import COMPONENT_CHANGEOUTS_SCHEMA

COMPONENT_CHANGEOUTS_ANALYTIS_PATH = (
    "az://bhp-analytics-data/PLANNING/COMPONENT_CHANGEOUTS/component_changeouts.parquet"
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

    # Drop rows with null values in specific columns
    # df = df.drop_nulls(subset=["equipment_name", "position_name", "changeout_date"])

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
        ).with_columns(pl.col("sap_equipment_name").str.strip_suffix(".0").cast(pl.Int64, strict=False).fill_null(-1))
    )

    # Map equipment model to prefix and concatenate with equipment_name
    model_mapping = {"980E-5": "CEX", "960E": "TK", "930E-4": "TK"}
    df = df.with_columns(
        (pl.col("equipment_model").replace(model_mapping) + pl.col("equipment_name").cast(pl.String)).alias(
            "equipment_name"
        )
    )
    return df


@dg.asset
def raw_component_changeouts(context):
    dl = DataLake(context=context)
    file_content = dl.read_bytes(az_path="az://bhp-raw-data/PLANNING/PLANILLA DE CONTROL CAMBIO DE COMPONENTES.xlsx")
    # msgraph = MSGraph(context=context)
    # file_content = msgraph.read_bytes(
    #     sp_path="sp://KCHCLSP00022/01. ÁREAS KCH/1.3 PLANIFICACION/01. Gestión pool de componentes/01. Control Cambio Componentes/PLANILLA DE CONTROL CAMBIO DE COMPONENTES.xlsx",
    # )
    columns = list(COLUMN_MAPPING.keys())
    df = pl.read_excel(
        BytesIO(file_content), sheet_name="Planilla Cambio Componente  960", infer_schema_length=0, columns=columns
    )
    # df = pd.read_excel(
    #     BytesIO(file_content), sheet_name="Planilla Cambio Componente  960", dtype=str, usecols=columns, nrows=4000
    # )
    # df = pl.DataFrame(df)

    return df


@dg.asset(
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
def mutate_component_changeouts(context: dg.AssetExecutionContext, raw_component_changeouts: pl.DataFrame):
    df = raw_component_changeouts.clone().pipe(process_component_changeouts, site_name="MEL")
    datalake = DataLake(context=context)  # Direct instantiation
    datalake.upload_tibble(tibble=df, az_path=COMPONENT_CHANGEOUTS_ANALYTIS_PATH)

    return df


@dg.asset(
    description="Reads the consolidated oil analysis data from the ADLS analytics layer.",
    metadata={"dagster/column_schema": COMPONENT_CHANGEOUTS_SCHEMA},
)
def component_changeouts(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake(context=context)
    df = dl.read_tibble(az_path=COMPONENT_CHANGEOUTS_ANALYTIS_PATH)
    return df
    # df = (
    #     df.filter((pl.col("component_hours").is_not_null()) & (pl.col("equipment_name") != "TK853"))
    #     .filter(pl.col("changeout_date").dt.year() >= 2019)
    #     # .filter(~(pl.col("subcomponent_name").is_in(["motor", "radiador", "subframe"])))
    #     .unique(
    #         subset=[
    #             "equipment_name",
    #             "component_name",
    #             "subcomponent_name",
    #             "position_name",
    #             "changeout_date",
    #             "sap_equipment_name",
    #             "customer_work_order",
    #         ]
    #     )
    # )

    #     context.log.info(f"Read {df.height} records from {COMPONENT_CHANGEOUTS_ANALYTIS_PATH}.")
    #     return df
    # else:
    #     context.log.warning(f"Data file not found at {COMPONENT_CHANGEOUTS_ANALYTIS_PATH}. Returning empty DataFrame.")
    #     return pl.DataFrame()
