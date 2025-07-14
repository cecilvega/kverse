import os
from pathlib import Path

import dagster as dg
import polars as pl

from kdags.resources.tidyr import DataLake, MSGraph
from .reader import read_csv_fault


@dg.asset
def read_raw_fault(context: dg.AssetExecutionContext, ddm_manifest) -> pl.DataFrame:
    # Fix This part
    event_files = ddm_manifest.filter(pl.col("data_type") == "FAULT")

    event_files = event_files.select(["filepath", "equipment_name"]).to_dicts()
    tibbles = []
    for v in event_files:
        content = Path(v["filepath"]).read_bytes()
        tibble = read_csv_fault(content)
        if not tibble.is_empty():
            tibbles.append(
                tibble.with_columns(filepath=pl.lit(v["filepath"]), filepath_equipment_name=pl.lit(v["equipment_name"]))
            )

    df = pl.concat(tibbles)

    return df


@dg.asset
def mutate_fault(context: dg.AssetExecutionContext, read_raw_fault):
    df = read_raw_fault.clone().unique(["parameter_code", "record_dt", "parameter_count"])

    # dl = DataLake()
    df.write_parquet(r"C:\Users\andmn\PycharmProjects\fault.parquet")
    # dl.upload_tibble(az_path="az://bhp-analytics-data/OPERATION/GE/events.parquet", tibble=df)
    return df
