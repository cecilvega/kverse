import dagster as dg

# === DOCS ===
publish_data_job = dg.define_asset_job(
    name="publish_data_job",
    selection=dg.AssetSelection.assets(
        "publish_data_catalog", "publish_work_order_history", "publish_oil_analysis", "publish_component_history"
    ).upstream(),
    description="Publicar datos a Sharepoint",
)


# === COMPONENTS ===

component_history_job = dg.define_asset_job(
    name="component_history_job",
    selection=dg.AssetSelection.assets("mutate_component_changeouts").upstream()
    | dg.AssetSelection.assets("mutate_so_report").upstream()
    | dg.AssetSelection.assets("mutate_component_history").upstream(),
    description="Cambios de componente",
)

component_fleet_job = dg.define_asset_job(
    name="component_fleet_job",
    selection=dg.AssetSelection.assets("mutate_component_fleet").upstream(),
    description="...",
)

component_reparations_job = dg.define_asset_job(
    name="component_reparations_job",
    selection=dg.AssetSelection.assets("mutate_component_reparations").upstream(),
    description="Reparación de componentes",
)

# === REPARATION ===
harvest_so_report_job = dg.define_asset_job(
    name="harvest_so_report_job",
    selection=dg.AssetSelection.assets("harvest_so_report").upstream(),
    description="Component Status RESO",
)
so_report_job = dg.define_asset_job(
    name="so_report_job",
    selection=dg.AssetSelection.assets("mutate_so_report").upstream(),
    description="Component Status RESO",
)
harvest_so_documents_job = dg.define_asset_job(
    name="harvest_so_documents_job",
    selection=dg.AssetSelection.assets(
        "harvest_so_documents",
    ).upstream(),
    description="Component Status RESO",
)
harvest_so_details_job = dg.define_asset_job(
    name="harvest_so_details_job",
    selection=dg.AssetSelection.assets(
        "harvest_so_details",
    ).upstream(),
    description="Component Status RESO",
)
quotations_job = dg.define_asset_job(
    name="quotations_job",
    selection=dg.AssetSelection.assets("mutate_so_quotations").upstream(),
)


# === DOCS ===
docs_job = dg.define_asset_job(
    name="docs_job",
    selection=dg.AssetSelection.assets(
        "publish_sp_io_catalog", "publish_sp_masterdata_catalog", "publish_sp_schema_catalog"
    ).upstream(),
    description="Job for publishing documentation.",
)


# === RELIABILITY ===


ep_job = dg.define_asset_job(
    name="ep_job",
    selection=dg.AssetSelection.assets("publish_sp_ep").upstream(),
    description="...",
)


warranties_job = dg.define_asset_job(
    name="warranties_job",
    selection=dg.AssetSelection.assets("mutate_warranties").upstream(),
    description="...",
)

icc_job = dg.define_asset_job(
    name="icc_job",
    selection=dg.AssetSelection.assets("mutate_icc").upstream(),
    tags={"source": "icc"},
)


pool_inventory_job = dg.define_asset_job(
    name="pool_inventory_job",
    selection=dg.AssetSelection.assets(
        "component_serials", "mutate_component_lifeline", "mutate_component_states", "mutate_component_snapshots"
    ).upstream(),
)


# === MAINTENANCE ===

pm_history_job = dg.define_asset_job(
    name="pm_history_job",
    selection=dg.AssetSelection.assets("spawn_pm_history").upstream(),
    description="Archivo con listado historial de PMs",
)

work_orders_job = dg.define_asset_job(
    name="work_orders_job",
    selection=dg.AssetSelection.assets("mutate_work_order_history").upstream(),
    description="Archivo con todas las OT's Fiori",
)


# === OPERATION ===

komtrax_job = dg.define_asset_job(
    name="komtrax_job",
    selection=dg.AssetSelection.assets("mutate_smr").upstream(),
    description="Procesamiento Komtrax DW",
)

op_file_idx_job = dg.define_asset_job(
    name="op_file_idx_job", selection=dg.AssetSelection.assets("spawn_op_file_idx").upstream()
)

plm_job = dg.define_asset_job(
    name="plm_job",
    selection=dg.AssetSelection.assets("spawn_plm3_haul").upstream()
    | dg.AssetSelection.assets("spawn_plm3_alarms").upstream(),
    description="PLM Haulcycle y alarmas",
)

ge_job = dg.define_asset_job(
    name="ge_job",
    selection=dg.AssetSelection.assets("mutate_raw_events").upstream(),
    description="GE Eventos",
)
