from io import BytesIO
from kdags.resources import DataLake, MSGraph
import pandas as pd
import dagster as dg


@dg.asset
def read_raw_work_orders_history():
    datalake = DataLake()
    df = datalake.list_paths("kcc-raw-data", "FIORI/WORK_ORDERS_HISTORY", recursive=False)

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
def materialize_work_order_history(reconciled_icc):
    """Exports reconciled component reports to SharePoint as Excel."""
    # Prepare data for upload
    buffer = BytesIO()
    reconciled_icc.to_excel(buffer, engine="openpyxl", index=False)
    buffer.seek(0)

    # Upload to SharePoint
    msgraph = MSGraph()
    result = msgraph.upload_dataframe(
        site_id="KCHCLSP00022",
        folder_path="/01. ÁREAS KCH/1.6 CONFIABILIDAD/CAEX/ANTECEDENTES/MANTENIMIENTO/FIORI",
        file_name=f"ot_fiori.xlsx",
        buffer=buffer,
    )

    return {"file_url": result.web_url, "count": len(reconciled_icc)}
