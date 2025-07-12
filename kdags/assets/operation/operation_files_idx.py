import dagster as dg
import pandas as pd
from pathlib import Path
import os
from .utils import extract_equipment_name
from kdags.resources.tidyr import DataLake, MasterData
import re
import polars as pl
from datetime import datetime

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
    path_parts = path.split(os.sep)

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


@dg.asset
def list_operation_files():
    """Process operation files and create indexed dataframe with equipment mapping"""

    # Get base path and list files
    # base_path = (
    #     Path(os.environ["ONEDRIVE_LOCAL_PATH"]).parent / "BHPDATA/DDMM/Datos MEL"
    # )
    base_path = r"C:\Users\andmn\Downloads\Datos MEL"

    files = [f for f in Path(base_path).rglob("*")]
    df = [f for f in files if "." in str(f).split("/")[-1]]
    df = [f for f in df if ((str(f).lower().endswith(".csv")) | (str(f).lower().endswith(".zip")))]
    df = [f for f in df if f.stat().st_size > 0]  # Remove 0-byte files

    # Create initial dataframe directly with polars
    df = pl.DataFrame({"filepath": [str(s) for s in df]})

    # Add suffix using polars expressions

    df = df.with_columns(suffix=pl.col("filepath").str.split(".").list.last().str.to_lowercase())

    # Extract equipment name from filepath
    df = df.with_columns(
        filename_equipment_name=pl.col("filepath").map_elements(extract_filename_equipment_name, return_dtype=pl.Utf8)
    )

    # Extract serial number from filepath
    df = df.with_columns(
        filename_equipment_serial=pl.col("filepath").map_elements(
            extract_filename_equipment_serial, return_dtype=pl.Utf8
        )
    )

    # Apply date pattern extraction
    df = df.with_columns(partition_date=pl.col("filepath").map_elements(extract_date_pattern, return_dtype=pl.Utf8))

    # Apply data pattern extraction and expand dict to columns
    df = df.with_columns(data_info=pl.col("filepath").map_elements(extract_data_pattern, return_dtype=pl.Struct))

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

    # # Upload to data lake
    # DataLake().upload_tibble(
    #     az_path="az://bhp-analytics-data/OPERATION/filtered_op_file_idx.parquet",
    #     tibble=df,
    # )
    df.write_parquet(r"C:\Users\andmn\PycharmProjects\operation_manifest.parquet")

    return df


@dg.asset
def operation_manifest():
    df = pl.read_parquet(r"C:\Users\andmn\PycharmProjects\operation_manifest.parquet")
    # dl = DataLake()
    # df = dl.read_tibble("az://bhp-analytics-data/OPERATION/filtered_op_file_idx.parquet")
    return df
