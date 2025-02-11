from datetime import datetime
from io import BytesIO

import pandas as pd
from office365.graph_client import GraphClient
from kdags.resources.msgraph.base import acquire_token_func


def read_base_pool_proj():
    graph_client = GraphClient(acquire_token_func)
    file = (
        graph_client.me.drive.root.get_by_path("/KVERSE/KDATA/PLANIFICATION/RAW/POOL/pool_proj.xlsx")
        .get()
        .execute_query()
    )
    content = file.get_content().execute_query().value
    df = pd.read_excel(BytesIO(content))

    df = df.assign(
        equipo=df["equipo"].astype(str),
    )
    df.loc[df["arrival_week"].notnull(), "arrival_date"] = df.loc[df["arrival_week"].notnull()]["arrival_week"].map(
        lambda x: datetime.strptime(x + "-1", "%Y-W%W-%w")
    )

    return df
