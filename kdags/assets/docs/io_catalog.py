import dagster as dg
from kdags.resources.tidyr import DataLake, MSGraph, MasterData
import polars as pl


@dg.asset
def publish_sp_io_catalog(context: dg.AssetExecutionContext):
    df = MasterData.read_io_map()
    df = df.with_columns(
        publish_path=pl.col("publish_path").str.replace(
            "sp://KCHCLSP00022",
            "https://globalkomatsu.sharepoint.com/sites/KCHCLSP00022/Shared Documents/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD",
        )
    ).rename(
        {
            "name": "Nombre Archivo",
            "publish_path": "URL Archivo",
            "module": "Módulo",
            "job_name": "Ejecutor",
            "source": "Fuente",
            "description": "Descripción Archivo",
            "cron_schedule": "Frecuencia",
            "input_path": "Input",
            "output_path": "Output",
        }
    )
    msgraph = MSGraph()
    upload_results = []
    sp_paths = [
        # "sp://KCHCLGR00058/___/DOCS/io.xlsx",
        "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/DOCS/catalogo_datos.xlsx",
    ]
    for sp_path in sp_paths:
        upload_results.append(msgraph.upload_tibble(tibble=df, sp_path=sp_path, context=context))
    return upload_results
