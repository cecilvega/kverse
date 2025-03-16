from io import BytesIO

import pandas as pd
from dagster import asset
from firebase_admin import firestore

from kdags.resources.tidyr import MSGraph, init_firebase


def get_attendances():
    db = firestore.client()
    docs = db.collection("attendances").stream()

    attendances = {}
    for doc in docs:
        data = doc.to_dict()
        columns = ["date", "ingreso", "createdAt"]
        # data[columns] = data[columns].astype("datetime64[ns]")
        for col in columns:
            data[col] = pd.to_datetime(data[col]).tz_localize(None)
        # data.loc[:, columns] = data[columns].apply(lambda x: pd.to_datetime(x).dt.tz_localize(None))

        attendances[doc.id] = {"id": doc.id, **data}

    df = pd.DataFrame.from_dict(attendances, orient="index")
    return df


@asset
def attendances():
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

    graph_client = MSGraph().client
    buffer = BytesIO()
    df.to_excel(buffer, engine="openpyxl", index=False)
    buffer.seek(0)

    graph_client.me.drive.root.get_by_path("/KVERSE/MANTENIMIENTO/AYUDANTE_MANTENIMIENTO").upload(
        name="asistencias.xlsx", content=buffer.getvalue()
    ).execute_query()
    return df
