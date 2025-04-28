from io import BytesIO

import dagster as dg
import pandas as pd

from kdags.resources.tidyr import DataLake, MSGraph


@dg.asset
def read_raw_work_orders_history():
    datalake = DataLake()
    df = datalake.list_paths("abfs://bhp-raw-data/FIORI/WORK_ORDERS_HISTORY", recursive=False)

    equipments = df["file_path"].to_list()
    frames = []
    for equipment in equipments:
        work_order_text_df = pd.read_excel(
            BytesIO(datalake.read_bytes(f"abfs://bhp-raw-data/{equipment}/history_text.xlsx"))
        ).rename(columns={"updated": "updated_at", "documents": "documents"})
        work_order_df = (
            pd.read_excel(BytesIO(datalake.read_bytes(f"abfs://bhp-raw-data/{equipment}/Ordenes de trabajo.xlsx")))
            .rename(
                columns={
                    "Orden": "ot",
                    "Sort Field": "equipment_name",
                    "Descripción de la orden": "description",
                    "Prioridad": "priority",
                    "Fecha de inicio básica": "start_date",
                    "Fecha de término básica": "end_date",
                }
            )
            .drop(
                columns=[
                    "Status\xa0del\xa0usuario",
                    "Status del sistema",
                    "Puesto de trabajo principal",
                ]
            )
        )
        work_order_df = pd.merge(work_order_df, work_order_text_df, on="ot", how="outer")
        frames.append(work_order_df)
        df = pd.concat(frames)
    return df


@dg.asset
def spawn_work_order_history(read_raw_work_orders_history):

    result = {}

    sharepoint_result = MSGraph().upload_tibble_deprecated(
        site_id="KCHCLSP00022",
        filepath="/01. ÁREAS KCH/1.6 CONFIABILIDAD/CAEX/ANTECEDENTES/MAINTENANCE/WORK_ORDERS_HISTORY/work_orders_history.xlsx",
        df=read_raw_work_orders_history,
        format="excel",
    )
    result["sharepoint"] = {"file_url": sharepoint_result.web_url, "format": "excel"}

    # 2. Upload to Data Lake as Parquet
    datalake = DataLake()
    datalake_path = datalake.upload_tibble(
        az_path="abfs://bhp-analytics-data/MAINTENANCE/WORK_ORDERS_HISTORY/work_orders_history.parquet",
        tibble=read_raw_work_orders_history,
        format="parquet",
    )
    result["datalake"] = {"path": datalake_path, "format": "parquet"}

    # Add record count
    result["count"] = len(read_raw_work_orders_history)

    return result


@dg.asset
def read_work_order_history():
    dl = DataLake()
    content = dl.read_bytes(f"abfs://bhp-analytics-data/MAINTENANCE/WORK_ORDERS_HISTORY/work_orders_history.parquet")
    df = pd.read_parquet(BytesIO(content))
    return df
