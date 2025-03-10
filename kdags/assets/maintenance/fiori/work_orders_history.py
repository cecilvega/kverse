from io import BytesIO
from kdags.resources import DataLake, MSGraph
import pandas as pd
import dagster as dg


@dg.asset
def read_raw_work_orders_history():
    datalake = DataLake()
    df = datalake.list_paths("kcc-raw-data", "BHP/FIORI/WORK_ORDERS_HISTORY", recursive=False)

    equipments = df["file_path"].to_list()
    frames = []
    for equipment in equipments:
        work_order_text_df = pd.read_excel(
            BytesIO(datalake.read_bytes("kcc-raw-data", f"{equipment}/history_text.xlsx"))
        ).rename(columns={"updated": "updated_at", "documents": "documents"})
        work_order_df = (
            pd.read_excel(BytesIO(datalake.read_bytes("kcc-raw-data", f"{equipment}/Ordenes de trabajo.xlsx")))
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
def materialize_work_order_history(read_raw_work_orders_history):

    result = {}

    # 1. Upload to SharePoint as Excel
    msgraph = MSGraph()
    sharepoint_result = msgraph.upload_tibble(
        site_id="KCHCLSP00022",
        file_path="/01. ÁREAS KCH/1.6 CONFIABILIDAD/CAEX/ANTECEDENTES/MANTENIMIENTO/FIORI/work_orders_history.xlsx",
        df=read_raw_work_orders_history,
        format="excel",
    )
    result["sharepoint"] = {"file_url": sharepoint_result.web_url, "format": "excel"}

    # 2. Upload to Data Lake as Parquet
    datalake = DataLake()
    datalake_path = datalake.upload_tibble(
        container="kcc-analytics-data",
        file_path="BHP/MAINTENANCE/WORK_ORDERS_HISTORY/work_orders_history.parquet",
        df=read_raw_work_orders_history,
        format="parquet",
    )
    result["datalake"] = {"path": datalake_path, "format": "parquet"}

    # Add record count
    result["count"] = len(read_raw_work_orders_history)

    return result


@dg.asset
def read_work_order_history():
    dl = DataLake()
    content = dl.read_bytes("kcc-analytics-data", "BHP/MAINTENANCE/WORK_ORDERS_HISTORY/work_orders_history.parquet")
    df = pd.read_parquet(BytesIO(content))
    return df
