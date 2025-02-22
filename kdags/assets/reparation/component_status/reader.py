import polars as pl
from kdags.resources import DataLake
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from io import StringIO


def read_raw_component_status():
    dl = DataLake()
    files_df = dl.list_paths("kcc-raw-data", "BHP/RESO/COMPONENT_STATUS")

    dfs = [
        pd.read_html(StringIO(dl.read_bytes("kcc-raw-data", path).decode("utf-8")))[0]
        for path in files_df["file_path"]
        if int(path.split()[-1].split(".")[0]) >= 2022
    ]

    return pd.concat(dfs, ignore_index=True)


# def read_component_status():
#     dl = DataLake()
#
#     # List all files in the specified path
#     files_df = dl.list_paths("kcc-raw-data", "RESO/COMPONENT_STATUS")
#
#     all_data = []
#
#     # Process each file
#     for file_path in files_df["file_path"].to_list():
#         # Get file content as bytes
#         content = dl.read_bytes("kcc-raw-data", file_path)
#         decoded_content = content.decode("utf-8")
#         # Read HTML table from the Excel content
#         df = pd.read_html(StringIO(decoded_content))[0]
#         all_data.append(df)
#
#     final_df = pd.concat(all_data, ignore_index=True)
#     return final_df
