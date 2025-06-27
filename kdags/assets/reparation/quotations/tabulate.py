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
    dl = DataLake(context)
    so_records = (
        component_reparations.join(
            so_quotations
            # .select(["service_order", "component_serial", "az_path"])
            .unique(subset=["service_order", "component_serial"]),
            how="left",
            on=["service_order", "component_serial"],
        )
        .drop_nulls("file_size")
        .filter(pl.col("subcomponent_tag").is_in(["5A30", "0980"]))
        .sort(["subcomponent_tag", "reception_date"])
        # .group_by("subcomponent_tag", maintain_order=True)
        # .tail(5)
        .select(["service_order", "component_serial", "az_path"])
        .to_dicts()
    )

    tibbles = []
    for record in so_records:
        try:
            # Extract metadata
            service_order = record["service_order"]
            component_serial = record["component_serial"]
            az_path = record["az_path"]

            # Read PDF content
            content = dl.read_bytes(az_path)

            # Extract conclusions section
            conclusions = extract_tables_from_quotations(content)
            tibble = pl.DataFrame(conclusions).with_columns(
                service_order=pl.lit(service_order),
                component_serial=pl.lit(component_serial),
                az_path=pl.lit(az_path),
            )
            tibbles.append(tibble)
        except Exception as e:
            print(e)
    df = pl.concat(tibbles, how="diagonal")

    dl.upload_tibble(df, DATA_CATALOG["quotations"]["analytics_path"])
    return df
