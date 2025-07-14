import dagster as dg
import pandas as pd
from pathlib import Path
import os
from .utils import extract_equipment_name
from kdags.resources.tidyr import DataLake, MasterData
import re
import polars as pl
from datetime import datetime

from ...config import DATA_CATALOG

# Data patterns mapping - order matters for PLM patterns
DATA_PATTERNS = {
    # PLM (Order is important: more specific PLM4 comes first)
    "PLM4": {"source": "PLM", "pattern": r"haulcycle"},
    "PLM3": {"source": "PLM", "pattern": r"haul|kms_export"},
    # VHMS
    "FAULT": {"source": "VHMS", "pattern": r"fault0"},
    "TREND_DATA": {"source": "VHMS", "pattern": r"(?<![_\w])trend0"},
    "PAYLOAD3": {"source": "VHMS", "pattern": r"payload3"},
    # GE
    "EFFECTIVE_GRADE": {"source": "GE", "pattern": r"egdata"},
    "TORQUE_SWING": {"source": "GE", "pattern": r"trqswdata"},
    "DATAPACK": {"source": "GE", "pattern": r"dp_"},
    "TRIP_MONITOR": {"source": "GE", "pattern": r"tripdata"},
    "EVENTS": {"source": "GE", "pattern": r"(?<![_\w])events(?![_\w])"},
    "MINE_SERIAL": {"source": "GE", "pattern": r"serial_"},
    "PROFILE": {"source": "GE", "pattern": r"profile_"},
    "COUNTER": {"source": "GE", "pattern": r"counter_"},
}


def extract_filename_equipment_name(path):
    """Extract equipment name from filepath using TK pattern or dsc/tci pattern"""
    tk_match = re.search(r"TK(\d{3})", path)
    if tk_match:
        return "TK" + tk_match.group(1)

    dsc_match = re.search(r"(\d{3})_(?:dsc|tci)", path)
    if dsc_match:
        return "TK" + dsc_match.group(1)
    return None


def extract_filename_equipment_serial(path):
    """Extract serial number from folder path - only matches folders with pattern A##### (A + 5 digits)"""
    # Split the path to get individual components
    path_parts = path.split("/")

    # Check each part to see if it's a folder matching exactly A + 5 digits
    for part in path_parts:
        # Match exactly A followed by 5 digits (no more, no less)
        if re.match(r"^A\d{5}$", part):
            return part

    return None


def extract_date_pattern(filepath):
    """Extract year and week from folder structure y=YYYY/w=WW"""
    year_match = re.search(r"y=(\d{4})", filepath)
    week_match = re.search(r"w=(\d{2})", filepath)

    if year_match and week_match:
        # year = year_match.group(1)
        # TODO:
        year = 2025
        week = week_match.group(1)
        iso_string = f"{year}-W{week:02d}-1"  # Monday of that week

        return datetime.strptime(iso_string, "%Y-W%U-%w")
        # return f"y={year}/w={week}"
    return None


def extract_data_pattern(filepath):
    """Extract data pattern and source from filepath"""
    for pattern_name, info in DATA_PATTERNS.items():
        if re.search(info["pattern"], filepath, re.IGNORECASE):
            return {"data_type": pattern_name, "data_source": info["source"]}
    return {"data_type": "unknown", "data_source": "unknown"}


@dg.asset(compute_kind="mutate")
def mutate_ddm_manifest(context, ddm_manifest: pl.DataFrame):
    dl = DataLake(context)
    df = dl.list_paths("az://bhp-ingest-data")

    df = df.with_columns(filename=pl.col("az_path").str.split("/").list.get(-1)).with_columns(
        filesuffix=pl.col("filename").str.split(".").list.get(-1).str.to_lowercase(),
        filestem=pl.col("filename").str.split(".").list.get(0),
    )
    df = df.filter(pl.col("filesuffix").is_in(["csv", "zip"])).filter(pl.col("file_size") > 0)
    # Extract equipment name from filepath
    df = df.with_columns(
        filename_equipment_name=pl.col("az_path").map_elements(extract_filename_equipment_name, return_dtype=pl.Utf8)
    )

    # Extract serial number from filepath
    df = df.with_columns(
        filename_equipment_serial=pl.col("az_path").map_elements(
            extract_filename_equipment_serial, return_dtype=pl.Utf8
        )
    )

    # Apply date pattern extraction
    df = df.with_columns(partition_date=pl.col("az_path").map_elements(extract_date_pattern, return_dtype=pl.Utf8))

    # Apply data pattern extraction and expand dict to columns
    df = df.with_columns(
        data_info=pl.col("az_path").map_elements(
            extract_data_pattern,
            return_dtype=pl.Struct([pl.Field("data_type", pl.String), pl.Field("data_source", pl.String)]),
        )
    )

    # Extract data_type and data_source from the struct
    df = df.with_columns(
        data_type=pl.col("data_info").struct.field("data_type"),
        data_source=pl.col("data_info").struct.field("data_source"),
    ).drop("data_info")

    # Get equipment master data for serial mapping
    equipments_df = MasterData.equipments().select(["equipment_serial", "equipment_name"])

    # Join with equipment master data on serial
    df = df.join(
        equipments_df.rename({"equipment_name": "_equipment_name"}),
        left_on="filename_equipment_serial",
        right_on="equipment_serial",
        how="left",
    )

    # Create final equipment_name column using coalesce
    df = df.with_columns(
        equipment_name=pl.coalesce(
            [
                pl.col("filename_equipment_name"),
                pl.col("_equipment_name"),  # from master data join
            ]
        )
    )

    # Drop intermediate columns
    df = df.drop(["_equipment_name"])

    # Left join to preserve all files, with processing status from manifest
    if not ddm_manifest.is_empty():
        df = df.join(
            ddm_manifest.select(["az_path", "file_size", "processed_at"]), on=["az_path", "file_size"], how="left"
        )
    else:
        df = df.with_columns(pl.lit(None).alias("processed_at"))

    dl.upload_tibble(df, DATA_CATALOG["ddm"]["manifest_path"])

    return df


@dg.asset(compute_kind="readr")
def ddm_manifest(context: dg.AssetExecutionContext):
    dl = DataLake(context)
    df = dl.read_tibble(DATA_CATALOG["ddm"]["manifest_path"])
    if df.is_empty():
        df = pl.DataFrame({"az_path": [], "file_size": [], "last_modified": [], "processed_at": []})

    return df
