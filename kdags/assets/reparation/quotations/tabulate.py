import re
from datetime import datetime

import dagster as dg
import polars as pl
from datetime import date
from kdags.resources.tidyr import DataLake, MSGraph

import polars as pl
import dagster as dg
from kdags.resources.tidyr import MSGraph, DataLake
from kdags.config import DATA_CATALOG
from .quotations_extractor import extract_tables_from_quotations


@dg.asset
def mutate_quotations(
    context: dg.AssetExecutionContext, component_reparations: pl.DataFrame, so_quotations: pl.DataFrame
) -> pl.DataFrame:
    so_df = (
        component_reparations.join(
            so_quotations
            # .select(["service_order", "component_serial", "az_path"])
            .unique(subset=["service_order", "component_serial"]),
            how="left",
            on=["service_order", "component_serial"],
        )
        .drop_nulls("file_size")
        .filter(pl.col("subcomponent_tag") == "5A30")
        .sort("reception_date")
        .tail(5)
        .select(["service_order", "component_serial", "az_path"])
        .to_dicts()
    )
    so_df = extract_tables_from_quotations(so_df)
    return so_df
