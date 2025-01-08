from office365.graph_client import GraphClient

from kdags.assets.maintenance.reader import get_attendances
from kdags.resources.msgraph import acquire_token_func
from kdags.resources.firebase import init_firebase
from dagster import asset
from io import BytesIO


@asset
def attendances():
    init_firebase()
    df = get_attendances()
    graph_client = GraphClient(acquire_token_func)
    buffer = BytesIO()
    df.to_excel(buffer, engine="openpyxl", index=False)
    buffer.seek(0)

    graph_client.me.drive.root.get_by_path("/KVERSE/KDATA/MAINTENANCE/RAW/ATTENDANCES").upload(
        name="attendances.xlsx", content=buffer.getvalue()
    ).execute_query()
    return df
