from io import StringIO

import polars as pl

# from kdags.assets.operation.utils import extract_header_info
import re


def extract_events_header_info(data: str) -> dict:
    """
    Extract header information from a GE Drive System Event Summary CSV.

    Args:
        data (str): Raw CSV data as a string

    Returns:
        dict: Dictionary containing the extracted header information
    """
    # Get the first few lines to process the header
    lines = data.split("\n")[:5]  # Get first 5 lines which should contain the header info

    header_info = {
        "report_title": lines[0].strip(),
        "oem": None,
        "truck_model": None,
        "truck_id": None,
        "date_created": None,
    }

    # Extract OEM
    oem_match = re.search(r"OEM:,(.+)", data)
    if oem_match:
        header_info["oem"] = oem_match.group(1).strip()

    # Extract Truck Model (removing the single quote if present)
    model_match = re.search(r"Truck Model:,(?:\')?(.+)", data)
    if model_match:
        header_info["truck_model"] = model_match.group(1).strip()

    # Extract Truck ID
    id_match = re.search(r"Truck ID:,(.+)", data)
    if id_match:
        header_info["truck_id"] = id_match.group(1).strip()

    # Extract Date Created (removing the Excel formula marker "=")
    date_match = re.search(r"Date Created:,=?\"?(.+?)\"?$", data, re.MULTILINE)
    if date_match:
        header_info["date_created"] = date_match.group(1).strip().replace('"', "")

    return header_info


def read_csv_events(data: bytes) -> pl.DataFrame:

    try:
        data = data.decode("utf-8")
    except UnicodeError:
        return pl.DataFrame()

    header_info = extract_events_header_info(data)
    data = data[data.find("Type") - 2 :]
    if data.__len__() <= 10:
        return pl.DataFrame()

    data = data.replace("Low speed, high torque timeout", "Low speed high torque timeout")

    # Detect delimiter by comparing comma vs semicolon counts in header
    first_line = data.split("\n")[0]
    comma_count = first_line.count(",")
    semicolon_count = first_line.count(";")
    delimiter = ";" if semicolon_count > comma_count else ","
    df = (
        pl.read_csv(
            StringIO(data),
            columns=[i for i in range(7)],
            separator=delimiter,
            truncate_ragged_lines=True,
            infer_schema_length=0,
        )
        .rename(lambda c: c.strip())
        .with_columns(
            [pl.col(c).str.strip_chars().cast(pl.Int64, strict=False).alias(c) for c in ["#", "Sub ID", "Event #"]]
        )
        .filter(pl.col("#").is_not_null())
    )
    header_info = {k: v for k, v in header_info.items() if k in ["truck_id", "date_created"]}
    for key, value in header_info.items():
        df = df.with_columns(pl.lit(value).alias(key))
    return df
