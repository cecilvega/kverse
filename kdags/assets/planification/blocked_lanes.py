from io import BytesIO

import pandas as pd
from office365.graph_client import GraphClient
from kdags.resources.msgraph.auth import acquire_token_func


def read_blocked_lanes():
    graph_client = GraphClient(acquire_token_func)
    file = (
        graph_client.me.drive.root.get_by_path("/KVERSE/KDATA/PLANIFICATION/RAW/POOL/unapproved_components.xlsx")
        .get()
        .execute_query()
    )
    content = file.get_content().execute_query().value
    df = pd.read_excel(BytesIO(content))

    df = (
        df.rename(
            columns={
                "Componente": "component",
                "Equipo": "equipo",
                "Pool": "pool_slot",
                "Fecha Cambio": "changeout_date",
                "Fecha Aprobación": "arrival_date",
            }
        )
        .assign(equipo=lambda x: x["equipo"].astype(str))
        .dropna(subset=["pool_slot"])
    )
    return df
