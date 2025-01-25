from kdags.assets.operations.utils import extract_header_info, extract_filename_info, parse_time_column
import pandas as pd
import logging
from io import StringIO


def read_csv_events(data: bytes, az_path: str) -> pd.DataFrame:
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

    # Step 2: Slice data and read, find Type first then slide 2 to the left to find #
    columns = [
        "#",
        "Type",
        "Event #",
        "Sub ID",
        "Name",
        "Sub ID Name",
        "Time",
        "Restriction",
        "Extra",
    ]
    data = data[data.find("Type") - 2 :]
    try:
        df = pd.read_csv(
            StringIO(data),
            sep=",",
            index_col=False,
            dtype=str,
            names=columns,
            skiprows=1,
            # usecols=list(range(0, 9)),
        )

        # Step 3: Remove null rows, clean columns and add header and filename information as columns
        df = (
            df.loc[df["Event #"].notnull()]
            .rename(columns={c: c.strip(" ") for c in df.columns})[
                [
                    "Type",
                    "Event #",
                    "Sub ID",
                    "Name",
                    "Sub ID Name",
                    "Time",
                    "Restriction",
                    "Extra",
                ]
            ]
            .assign(
                **header_info,
                **filename_info,
                Time=lambda x: parse_time_column(x["Time"]),
                # Time=pd.to_datetime(
                #     df["Time"].replace({'"': "", "=": ""}, regex=True),
                #     format="%Y-%m-%d %H:%M:%S.%f",
                # ),
            )
        )
    except Exception as e:
        logging.warning(f"Error {e} reading file: {az_path}")
        return pd.DataFrame()
    return df
