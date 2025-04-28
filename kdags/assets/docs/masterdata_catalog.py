import dagster as dg
from kdags.resources.tidyr import DataLake, MSGraph, MasterData


@dg.asset
def publish_sp_masterdata_catalog(context: dg.AssetExecutionContext):
    """
    Takes the final oil_analysis data (read by read_oil_analysis) and uploads it to SharePoint.
    """
    equipments_df = MasterData.equipments()
    components_df = MasterData.components()
    taxonomy_df = MasterData.taxonomy()

    msgraph = MSGraph()
    upload_results = []
    equipments_sp_paths = [
        # "sp://KCHCLGR00058/___/DOCS/equipos.xlsx",
        "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/DOCS/maestro_equipos.xlsx",
    ]
    for sp_path in equipments_sp_paths:
        context.log.info(f"Publishing to {sp_path}")
        upload_results.append(msgraph.upload_tibble(tibble=equipments_df, sp_path=sp_path))
    components_sp_paths = [
        # "sp://KCHCLGR00058/___/DOCS/componentes.xlsx",
        "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/DOCS/maestro_componentes.xlsx",
    ]
    for sp_path in components_sp_paths:
        context.log.info(f"Publishing to {sp_path}")
        upload_results.append(msgraph.upload_tibble(tibble=components_df, sp_path=sp_path))
    taxonomy_sp_paths = [
        # "sp://KCHCLGR00058/___/DOCS/taxonomia.xlsx",
        "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/DOCS/maestro_taxonomia.xlsx",
    ]
    for sp_path in taxonomy_sp_paths:
        context.log.info(f"Publishing to {sp_path}")
        upload_results.append(msgraph.upload_tibble(tibble=taxonomy_df, sp_path=sp_path))

    return upload_results
