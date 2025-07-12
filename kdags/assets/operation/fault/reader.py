from io import StringIO
from datetime import datetime
import polars as pl

# from kdags.assets.operation.utils import extract_header_info
import re


def convert_timestamp(timestamp_str):
    # Split the string and get the Unix timestamp
    timestamp = int(timestamp_str.split("|")[0])

    # Convert to datetime and format
    dt = datetime.fromtimestamp(timestamp)
    return dt


def read_csv_fault(data: bytes) -> pl.DataFrame:

    try:
        data = data.decode("utf-8")
    except UnicodeError:
        return pl.DataFrame()

    # header_info = extract_faut_header_info(data)
    data = data[data.find("[Data]") :][8:]
    if data.__len__() <= 10:
        return pl.DataFrame()

    df = pl.read_csv(
        StringIO(data),
        columns=[i for i in range(9)],
        separator=",",
        truncate_ragged_lines=True,
        infer_schema_length=0,
        has_header=False,
    ).drop("column_1")
    df.columns = [
        "parameter_code",
        "from_smr",
        "record_dt",
        "to_smr",
        "to_dt",
        "_flag",
        "parameter_count",
        "parameter_name",
    ]

    df = df.with_columns(
        record_dt=pl.col("record_dt").map_elements(
            convert_timestamp,
            return_dtype=pl.Datetime,
        ),
        parameter_count=pl.col("parameter_count").cast(pl.Int64),
    )
    df = df.drop(["from_smr", "to_smr", "_flag", "to_dt"]).with_columns(
        [
            pl.col(c).str.strip_chars().cast(pl.String, strict=False).alias(c)
            for c in ["parameter_code", "parameter_name"]
        ]
    )

    return df
