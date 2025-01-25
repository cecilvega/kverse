from io import BytesIO

from dagster import asset
from office365.graph_client import GraphClient

from kdags.assets.maintenance.reader import get_attendances
from kdags.resources.firebase import init_firebase
from kdags.resources.msgraph import acquire_token_func


@asset
def events():
    init_firebase()
    df = get_attendances()

    df = (
        df[
            [
                "date",
                "turno",
                "nombre",
                "rut",
                "categoria",
                "subcategoria",
                "estado",
                "habitacion",
                "cama",
                "createdBy",
                "createdAt",
            ]
        ]
        .rename_axis("attendance_id")
        .reset_index()
        .sort_values(["date", "turno", "nombre"], ascending=False)
    )

    graph_client = GraphClient(acquire_token_func)
    buffer = BytesIO()
    df.to_excel(buffer, engine="openpyxl", index=False)
    buffer.seek(0)

    graph_client.me.drive.root.get_by_path("/KVERSE/MANTENIMIENTO/AYUDANTE_MANTENIMIENTO").upload(
        name="asistencias.xlsx", content=buffer.getvalue()
    ).execute_query()
    return df
