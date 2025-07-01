import dagster as dg
import polars as pl
from dagster import AssetExecutionContext

from kdags.resources.tidyr import DataLake, MSGraph
from kdags.config import DATA_CATALOG


@dg.asset(compute_kind="publish")
def publish_data_catalog(context: dg.AssetExecutionContext):
    msgraph = MSGraph(context)
    rows = []
    for asset, asset_data in DATA_CATALOG.items():
        if isinstance(asset_data, dict) and "publish_path" in asset_data:
            rows.append(
                {
                    "asset_group": asset_data.get("group_name", ""),
                    "asset": asset,
                    "publish_path": asset_data["publish_path"],
                }
            )

    # Create the Polars DataFrame
    df = pl.DataFrame(rows)
    df = df.with_columns(
        file_name=pl.col("publish_path").str.extract(r"([^/]*)$", 1),
        publish_url=pl.col("publish_path")
        .str.replace(r"/[^/]*$", "")
        .str.replace(
            "sp://KCHCLGR00058",
            "https://globalkomatsu.sharepoint.com/sites/KCHCLGR00058/Shared%20Documents",
        )
        .str.replace(
            "sp://KCHCLSP00022",
            "https://globalkomatsu.sharepoint.com/sites/KCHCLSP00022/Shared%20Documents",
        ),
    )
    tibble = df.rename(
        {
            "asset_group": "Grupo",
            "asset": "Activo Datos",
            "file_name": "Nombre Archivo",
            "publish_url": "URL Activo Datos",
        }
    ).drop("publish_path")
    msgraph.upload_tibble(
        tibble=tibble,
        sp_path=DATA_CATALOG["publish_catalog"]["publish_path"],
    )
    return df
