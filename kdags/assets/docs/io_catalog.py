import dagster as dg
from kdags.resources.tidyr import DataLake, MSGraph, MasterData


@dg.asset
def publish_sp_io_catalog(context: dg.AssetExecutionContext):
    """
    Takes the final oil_analysis data (read by read_oil_analysis) and uploads it to SharePoint.
    """
    io_map_df = MasterData.read_io_map()

    msgraph = MSGraph()
    upload_results = []
    sp_paths = [
        # "sp://KCHCLGR00058/___/DOCS/io.xlsx",
        "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/DOCS/io.xlsx",
    ]
    for sp_path in sp_paths:
        context.log.info(f"Publishing to {sp_path}")
        upload_results.append(msgraph.upload_tibble(tibble=io_map_df, sp_path=sp_path))
    return upload_results
