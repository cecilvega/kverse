from office365.graph_client import GraphClient

from kdags.resources.msgraph import acquire_token_func
from .reader import read_component_arrivals
from dagster import asset
from io import BytesIO


@asset
def component_arrivals():
    df = read_component_arrivals()
    graph_client = GraphClient(acquire_token_func)
    buffer = BytesIO()
    df.to_excel(buffer, engine="openpyxl", index=False)
    buffer.seek(0)

    graph_client.me.drive.root.get_by_path("/KVERSE/KDATA/PLANIFICATION/ANALYTICS").upload(
        name="component_arrivals.xlsx", content=buffer.getvalue()
    ).execute_query()
    return df
