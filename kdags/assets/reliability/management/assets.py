import dagster as dg
from kdags.resources.tidyr import DataLake, MSGraph, MasterData


@dg.asset
def publish_sp_masterdata(context: dg.AssetExecutionContext):
    """
    Takes the final oil_analysis data (read by read_oil_analysis) and uploads it to SharePoint.
    """
    io_map_df = MasterData.read_io_map()
    equipments_df = MasterData.equipments()
    components_df = MasterData.components()
    taxonomy_df = MasterData.taxonomy()

    msgraph = MSGraph()  # Direct instantiation
    upload_results = []
    sp_result_io_map = msgraph.upload_tibble("sp://KCHCLGR00058/___/IO.xlsx", io_map_df)
    msgraph.upload_tibble("sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/IO.xlsx", io_map_df)
    sp_result_equipments = msgraph.upload_tibble("sp://KCHCLGR00058/___/MAESTROS/equipos.xlsx", equipments_df)
    msgraph.upload_tibble(
        "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/MAESTROS/equipos.xlsx", equipments_df
    )
    sp_result_components = msgraph.upload_tibble("sp://KCHCLGR00058/___/MAESTROS/componentes.xlsx", components_df)
    msgraph.upload_tibble(
        "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/MAESTROS/componentes.xlsx", components_df
    )
    sp_result_taxonomy = msgraph.upload_tibble("sp://KCHCLGR00058/___/MAESTROS/taxonomia.xlsx", taxonomy_df)
    msgraph.upload_tibble(
        "sp://KCHCLSP00022/01. ÁREAS KCH/1.6 CONFIABILIDAD/JEFE_CONFIABILIDAD/MAESTROS/taxonomia.xlsx", taxonomy_df
    )
    upload_results.extend([sp_result_io_map, sp_result_equipments, sp_result_components, sp_result_taxonomy])
    return upload_results
