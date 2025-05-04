from io import BytesIO

import dagster as dg
import polars as pl

from kdags.resources.tidyr import MSGraph
from .assets import process_component_changeouts
from .constants import *


@dg.asset
def spence_component_changeouts():
    msgraph = MSGraph()
    file_content = msgraph.read_bytes(
        sp_path="sp://KCHCLSP00060/1.- Gestión de Componentes/2.- Spence/1.- Planilla Control cambio de componentes/Planilla control cambio componentes/NUEVA PLANILLA DE CONTROL CAMBIO DE COMPONENTES SPENCE.xlsx",
    )
    columns = list({k: v for k, v in COLUMN_MAPPING.items() if k not in ["MODELO", "OS  181"]}.keys())
    df = pl.read_excel(
        BytesIO(file_content),
        sheet_name="Planilla Cambio Componente  980",
        infer_schema_length=0,
        columns=columns,
    ).with_columns([pl.lit("980E-5").alias("MODELO"), pl.lit(-1).alias("OS  181")])
    df = process_component_changeouts(df, site_name="SPENCE")
    return df
