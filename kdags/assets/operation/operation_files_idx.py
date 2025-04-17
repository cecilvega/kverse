import dagster as dg
import pandas as pd
from pathlib import Path
import os
from .utils import extract_equipment_name
from kdags.resources.tidyr import DataLake
import re


@dg.asset
def list_op_file_idx():

    base_path = Path(os.environ["ONEDRIVE_LOCAL_PATH"]).parent / "BHPDATA/DDMM"

    files = [f for f in Path(base_path).rglob("*")]
    df = [f for f in files if "." in str(f).split("/")[-1]]
    df = [f for f in df if ((str(f).lower().endswith(".csv")) | (str(f).lower().endswith(".zip")))]
    df = pd.DataFrame({"filepath": [str(s) for s in df]})
    df = df.assign(suffix=df["filepath"].map(lambda x: str(x).split(".")[-1]))
    df = df.assign(equipment_name=lambda x: x["filepath"].map(lambda y: extract_equipment_name(y)))
    return df


def extract_patterns(filepath):
    # Date patterns
    date_patterns = {
        r"\d{4}-\d{2}-\d{2}": "yyyy-mm-dd",
        r"\d{2}\.\d{2}\.\d{2}": "dd.mm.yy",
        r"\d{2}-\d{2}-\d{2}": "dd-mm-yy",
    }

    # Data patterns mapping with their sources
    data_patterns = {
        "EFFECTIVE_GRADE": {"pattern": r"egdata", "source": "GE"},
        "TORQUE_SWING": {"pattern": r"trqswdata", "source": "GE"},
        "MINE": {"pattern": r"serial_", "source": "GE"},
        "PROFILE": {"pattern": r"profile_", "source": "GE"},
        "COUNTER": {"pattern": r"counter_", "source": "GE"},
        "TRIP_MONITOR": {"pattern": "rtripdata", "source": "GE"},
        "EVENTS": {"pattern": r"events", "source": "GE"},
        "DATAPACK": {"pattern": r"dp_", "source": "GE"},
        "PLM3": {"pattern": r"KMS_Export", "source": "PLM"},
        "PLM4": {"pattern": r"haulcycle", "source": "PLM"},
        "FAULT": {"pattern": r"fault0", "source": "VHMS"},
        "TREND_DATA": {"pattern": r"/trend0\.csv$", "source": "VHMS"},
    }

    # Extract date pattern
    date_value = None
    date_pattern = "no_date_pattern"
    for pattern, format_name in date_patterns.items():
        match = re.search(pattern, filepath.lower())
        if match:
            date_value = match.group(0)
            date_pattern = format_name
            break

    # Extract data pattern and source
    data_type = "unknown"
    data_source = "unknown"
    for pattern_name, info in data_patterns.items():
        if re.search(info["pattern"], filepath, re.IGNORECASE):
            data_type = pattern_name
            data_source = info["source"]
            break

    return {
        "extracted_date": date_value,
        "date_pattern": date_pattern,
        "data_type": data_type,
        "data_source": data_source,
    }


def process_files_list(df):
    # Create new columns
    df["extracted_date"] = None
    df["date_pattern"] = None
    df["data_type"] = None
    df["data_source"] = None

    # Process each row
    for idx, row in df.iterrows():
        patterns = extract_patterns(row["filepath"])
        df.at[idx, "extracted_date"] = patterns["extracted_date"]
        df.at[idx, "date_pattern"] = patterns["date_pattern"]
        df.at[idx, "data_type"] = patterns["data_type"]
        df.at[idx, "data_source"] = patterns["data_source"]

    return df


def convert_date_format(date_str, pattern):
    if pd.isna(date_str) or pd.isna(pattern):
        return None

    # Map our patterns to pandas datetime formats
    pattern_to_format = {
        "yyyy-mm-dd": "%Y-%m-%d",
        "dd.mm.yy": "%d.%m.%y",
        "dd-mm-yy": "%d-%m-%y",
        # "yyyymmdd": "%Y%m%d",
    }

    try:
        if pattern in pattern_to_format:
            return pd.to_datetime(date_str, format=pattern_to_format[pattern], errors="coerce")
        return None
    except ValueError:
        return None


@dg.asset
def spawn_op_file_idx(list_op_file_idx):
    df = list_op_file_idx.copy()
    df["extracted_date"] = None
    df["date_pattern"] = None
    df["data_type"] = None
    df["data_source"] = None

    # Process each row
    for idx, row in df.iterrows():
        patterns = extract_patterns(row["filepath"])
        df.at[idx, "extracted_date"] = patterns["extracted_date"]
        df.at[idx, "date_pattern"] = patterns["date_pattern"]
        df.at[idx, "data_type"] = patterns["data_type"]
        df.at[idx, "data_source"] = patterns["data_source"]

    df["partition_date"] = df.apply(lambda x: convert_date_format(x["extracted_date"], x["date_pattern"]), axis=1)

    DataLake().upload_tibble(
        az_path="abfs://bhp-analytics-data/OPERATION/op_file_idx.parquet",
        df=df,
        format="parquet",
    )

    return df


@dg.asset
def read_op_file_idx():
    dl = DataLake()
    df = dl.read_tibble("abfs://bhp-analytics-data/OPERATION/op_file_idx.parquet")
    return df
