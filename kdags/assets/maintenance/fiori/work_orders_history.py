from io import BytesIO

import dagster as dg
import pandas as pd
import polars as pl
from kdags.resources.tidyr import DataLake, MSGraph
from kdags.config import DATA_CATALOG


@dg.asset
def raw_work_order_history(context: dg.AssetExecutionContext):
    dl = DataLake(context)
    df = pd.read_excel(BytesIO(dl.read_bytes(DATA_CATALOG["work_order_history"]["raw_path"])))
    df = pl.from_pandas(df)
    return df


@dg.asset
def mutate_work_order_history(context: dg.AssetExecutionContext, raw_work_order_history):
    df = (
        raw_work_order_history.drop(
            [
                "User Status",
                "System Status",
                "Main Work Center",
            ]
        )
        .rename(
            {
                "Order": "ot",
                # "Sort Field": "equipment_name",
                "Order Description": "description",
                "Priority": "priority",
                "Basic Start Date": "start_date",
                "Basic End Date": "end_date",
            }
        )
        .with_columns(equipment_name=pl.col("Sort Field").str.extract(r"(TK\d{3}|CEX\d{2})", 1))
    )

    datalake = DataLake(context=context)  # Direct instantiation
    datalake.upload_tibble(tibble=df, az_path=DATA_CATALOG["work_order_history"]["analytics_path"])
    return df


#
# @dg.asset
# def work_order_history(context: dg.AssetExecutionContext):
#     dl = DataLake(context)
#     df = dl.read_tibble(f"abfs://bhp-analytics-data/MAINTENANCE/WORK_ORDERS_HISTORY/work_orders_history.parquet")
#     return df
