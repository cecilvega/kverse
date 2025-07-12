from io import BytesIO

import dagster as dg
import pandas as pd
import polars as pl
from kdags.resources.tidyr import DataLake, MSGraph
from kdags.config import DATA_CATALOG


@dg.asset(compute_kind="readr")
def mutate_fault_codes(context: dg.AssetExecutionContext, mutate_fault, mutate_events):
    fault_df = (
        mutate_fault.rename({"filepath_equipment_name": "equipment_name"})
        .with_columns(recording_type=pl.lit("KPLUS"))
        .select(
            [
                "equipment_name",
                "parameter_code",
                "recording_type",
                "record_dt",
                "parameter_count",
                "parameter_name",
            ]
        )
    )

    events_df = (
        mutate_events.rename(
            {
                "filepath_equipment_name": "equipment_name",
            }
        )
        .with_columns(parameter_count=pl.lit(1))
        .select(
            [
                "equipment_name",
                "parameter_code",
                "recording_type",
                "record_dt",
                "parameter_count",
                "parameter_name",
            ]
        )
    )
    df = pl.concat([fault_df, events_df], how="diagonal_relaxed")
    df.write_parquet(r"C:\Users\andmn\PycharmProjects\fault_codes.parquet")
    df.write_csv(r"C:\Users\andmn\PycharmProjects\fault_codes.csv")
    return df
