import dagster as dg


# === DOCS ===
docs_job = dg.define_asset_job(
    name="docs_job",
    selection=dg.AssetSelection.assets(
        "publish_sp_io_catalog", "publish_sp_masterdata_catalog", "publish_sp_schema_catalog"
    ).upstream(),
    description="Job for publishing documentation.",
)


publish_sp_job = dg.define_asset_job(
    name="test_job",
    selection=dg.AssetSelection.assets("hola").upstream(),
    description="GE Eventos",
)


# === RELIABILITY ===

ep_job = dg.define_asset_job(
    name="ep_job",
    selection=dg.AssetSelection.assets("publish_sp_ep").upstream(),
    description="...",
    run_tags={"source": "icc"},
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

# === REPARATION ===
quotations_job = dg.define_asset_job(
    name="quotations_job",
    selection=dg.AssetSelection.assets(["mutate_quotations"]).upstream(),
)


harvest_so_details_job = dg.define_asset_job(
    name="harvest_so_details_job",
    selection=dg.AssetSelection.assets("harvest_so_details").upstream(),
    description="Component Status RESO",
)

harvest_so_documents_job = dg.define_asset_job(
    name="harvest_so_documents_job",
    selection=dg.AssetSelection.assets("harvest_so_documents").upstream(),
    description="Component Status RESO",
)

# === MAINTENANCE ===

pm_history_job = dg.define_asset_job(
    name="pm_history_job",
    selection=dg.AssetSelection.assets("spawn_pm_history").upstream(),
    description="Archivo con listado historial de PMs",
)

work_order_history_job = dg.define_asset_job(
    name="work_order_history_job",
    selection=dg.AssetSelection.assets("spawn_work_order_history").upstream(),
    description="Archivo con todas las OT's Fiori",
)


# === OPERATION ===

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
