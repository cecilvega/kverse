import logging
from io import StringIO

import pandas as pd

from kdags.assets.operation.utils import extract_filename_info, extract_header_info


def read_csv_trqsw(data: bytes, az_path: str) -> pd.DataFrame:
    """
    Reads OHV style csv data
    :param data:
    :param az_path:
    :return:
    """

    data = data.decode("latin-1")
    # Step 1: Information from header and filename
    header_info = extract_header_info(data, ["equipment_model", "equipment_name"])

    filename_info = extract_filename_info(az_path, ["equipment_serial", "site_name"])

    # Step 2: Slice data and read
    data = data[data.rfind("Date") :]

    try:
        # Read just the header of the CSV
        header_df = pd.read_csv(StringIO(data), nrows=0)
        # Determine valid columns based on the header
        valid_columns = header_df.columns[header_df.columns.notnull()].tolist()
        df = pd.read_csv(StringIO(data), sep=",", index_col=False, dtype=str, usecols=valid_columns)
    except Exception as e:  # pd.errors.EmptyDataError as e:
        logging.warning(f"Error {e} in file: {az_path}")
        return pd.DataFrame()

    df.columns = df.columns.str.strip(" ")

    # Step 3: Remove null rows, clean columns and add header and filename information as columns
    columns_map = {
        "Delta": "Motor 1 Delta",
        "Min": "Motor 1 Min",
        "Max": "Motor 1 Max",
        "Trq": "Motor 1 Trq",
        "Delta.1": "Motor 2 Delta",
        "Min.1": "Motor 2 Min",
        "Max.1": "Motor 2 Max",
        "Trq.1": "Motor 2 Trq",
    }
    try:
        df = (
            df.loc[df["Date"].notnull()]
            .rename(columns={c: c.strip(" ") for c in df.columns})
            .rename(columns=columns_map)[
                [
                    "Date",
                    "Orange",
                    "Red",
                    "Cluster",
                    "Truckspeed",
                    "GVW",
                    "Incline",
                    *list(columns_map.values()),
                    "Orange Lim",
                    "Red Lim",
                ]
            ]
            .assign(
                **header_info,
                **filename_info,
            )
        )
    except KeyError as e:
        logging.warning(f"Missing column in file: {az_path}")
        return pd.DataFrame()
    df["Date"] = pd.to_datetime(df["Date"].replace({"'": ""}, regex=True), format="%Y-%m-%d %H:%M:%S.%f")

    return df
