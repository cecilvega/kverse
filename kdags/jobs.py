import dagster as dg

__all__ = ["jobs"]


# === DOCS ===
publish_data_job = dg.define_asset_job(
    name="publish_data_job",
    selection=dg.AssetSelection.assets(
        "publish_data_catalog",
        "publish_notifications",
        "publish_oil_analysis",
        "publish_component_history",
        "publish_component_reparations",
        "publish_quotations",
        "publish_ep",
        "publish_icc",
    ).upstream(),
    description="Publicar datos a Sharepoint",
)


# === COMPONENTS ===

component_history_job = dg.define_asset_job(
    name="component_history_job",
    selection=dg.AssetSelection.assets("mutate_component_changeouts").upstream()
    | dg.AssetSelection.assets("mutate_so_report").upstream()
    | dg.AssetSelection.assets("mutate_component_history").upstream()
    | dg.AssetSelection.assets("mutate_icc").upstream(),
    description="Cambios de componente",
)

tabulate_quotations_job = dg.define_asset_job(
    name="tabulate_quotations_job",
    selection=dg.AssetSelection.assets("mutate_quotations").upstream(),
    description="Cambios de componente",
)

component_fleet_job = dg.define_asset_job(
    name="component_fleet_job",
    selection=dg.AssetSelection.assets(
        "publish_component_fleet", "fleet_risk_analysis", "publish_parts_fleet"
    ).upstream(),
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
    selection=dg.AssetSelection.assets("mutate_so_report", "mutate_component_reparations").upstream(),
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

reso_documents_job = dg.define_asset_job(
    name="reso_documents_job",
    selection=dg.AssetSelection.assets("mutate_mt_docs").upstream(),
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
    selection=dg.AssetSelection.assets("mutate_ep").upstream(),
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

fiori_job = dg.define_asset_job(
    name="fiori_job",
    selection=dg.AssetSelection.assets("mutate_notifications").upstream(),
    description="Archivo con todas las OT's Fiori",
)


# === OPERATION ===

# komtrax_job = dg.define_asset_job(
#     name="komtrax_job",
#     selection=dg.AssetSelection.assets("mutate_smr").upstream(),
#     description="Procesamiento Komtrax DW",
# )

manifest_ = dg.define_asset_job(
    name="op_file_idx_job", selection=dg.AssetSelection.assets("spawn_op_file_idx").upstream()
)

plm_job = dg.define_asset_job(
    name="plm_job",
    selection=dg.AssetSelection.assets("spawn_plm3_haul").upstream()
    | dg.AssetSelection.assets("spawn_plm3_alarms").upstream(),
    description="PLM Haulcycle y alarmas",
)

oil_analysis_job = dg.define_asset_job(
    name="oil_analysis_job",
    selection=dg.AssetSelection.assets("mutate_oil_analysis").upstream(),
    description="Muestras aceite SCAAE",
)

ge_job = dg.define_asset_job(
    name="ge_job",
    selection=dg.AssetSelection.assets("mutate_events").upstream(),
    description="GE Eventos",
)

jobs = [
    # === COMPONENTS ===
    component_reparations_job,
    harvest_so_report_job,
    warranties_job,
    # === DOCS ===
    docs_job,
    tabulate_quotations_job,
    # === RELIABILITY ===
    component_fleet_job,
    component_history_job,
    pool_inventory_job,
    ep_job,
    quotations_job,
    icc_job,
    # === REPARATION ===
    so_report_job,
    harvest_so_details_job,
    reso_documents_job,
    harvest_so_documents_job,
    # === MAINTENANCE ===
    fiori_job,
    pm_history_job,
    # === OPERATION ===
    # op_file_idx_job,
    plm_job,
    ge_job,
    oil_analysis_job,
    # komtrax_job,
    publish_data_job,
]
