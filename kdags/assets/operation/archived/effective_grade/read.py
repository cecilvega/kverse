import logging
from io import StringIO

import pandas as pd

from kdags.assets.operation.utils import extract_filename_info, extract_header_info


def read_csv_eg(data: bytes, az_path: str) -> pd.DataFrame:
    """
    Reads OHV style csv data
    :param data:
    :param az_path:
    :return:
    """
    try:
        data = data.decode("latin-1")

        # Step 1: Information from header and filename
        header_info = extract_header_info(data, ["equipment_model", "equipment_name"])

        filename_info = extract_filename_info(az_path, ["equipment_serial", "site_name"])

        # Step 2: Detect the begining of the data
        data = data[data.rfind("Date") :]

        try:
            df = pd.read_csv(StringIO(data), sep=",", index_col=False, dtype=str, usecols=list(range(0, 33)))

        except Exception as e:
            logging.warning(f"Error {e} reading file: {az_path}")
            return pd.DataFrame()
        # except pd.errors.EmptyDataError as e:
        #     logging.warning(f"Empty file: {az_path}")
        #     return pd.DataFrame()
        df.columns = df.columns.str.strip(" ")
        # Step 3: Remove null rows, clean columns and add header and filename information as columns
        df = df.dropna().rename(columns={c: c.strip(" ") for c in df.columns}).assign(**header_info, **filename_info)
        df["Date"] = pd.to_datetime(
            df["Date"].replace({"'": ""}, regex=True), format="%Y-%m-%d %H:%M:%S.%f", errors="coerce"
        )
        df = df.dropna(subset=["Date"]).reset_index(drop=True)
    except Exception as e:
        return pd.DataFrame()
    return df
