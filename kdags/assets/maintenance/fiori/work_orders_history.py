from io import BytesIO

import dagster as dg
import pandas as pd

from kdags.resources.tidyr import DataLake, MSGraph


@dg.asset
def raw_work_orders(context: dg.AssetExecutionContext):
    dl = DataLake(context)
    df = (
        pd.read_excel(BytesIO(dl.read_bytes(f"az://bhp-raw-data/FIORI/Work Orders.xlsx")))
        .rename(
            columns={
                "Order": "ot",
                "Sort Field": "equipment_name",
                "Order Description": "description",
                "Priority": "priority",
                "Basic Start Date": "start_date",
                "Basic End Date": "end_date",
            }
        )
        .drop(
            columns=[
                "User Status",
                "System Status",
                "Main Work Center",
            ]
        )
    )
    return df


# @dg.asset
# def spawn_work_order_history(read_raw_work_orders_history):
#
#     result = {}
#
#     sharepoint_result = MSGraph().upload_tibble_deprecated(
#         site_id="KCHCLSP00022",
#         filepath="/01. ÁREAS KCH/1.6 CONFIABILIDAD/CAEX/ANTECEDENTES/MAINTENANCE/WORK_ORDERS_HISTORY/work_orders_history.xlsx",
#         df=read_raw_work_orders_history,
#         format="excel",
#     )
#     result["sharepoint"] = {"file_url": sharepoint_result.web_url, "format": "excel"}
#
#     # 2. Upload to Data Lake as Parquet
#     datalake = DataLake()
#     datalake_path = datalake.upload_tibble(
#         az_path="abfs://bhp-analytics-data/MAINTENANCE/WORK_ORDERS_HISTORY/work_orders_history.parquet",
#         tibble=read_raw_work_orders_history,
#     )
#     result["datalake"] = {"path": datalake_path, "format": "parquet"}
#
#     # Add record count
#     result["count"] = len(read_raw_work_orders_history)
#
#     return result
#
#
# @dg.asset
# def work_order_history(context: dg.AssetExecutionContext):
#     dl = DataLake(context)
#     df = dl.read_tibble(f"abfs://bhp-analytics-data/MAINTENANCE/WORK_ORDERS_HISTORY/work_orders_history.parquet")
#     return df
