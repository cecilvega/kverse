import dagster as dg


# === DOCS ===
docs_job = dg.ScheduleDefinition(
    job=dg.define_asset_job(
        name="docs_job",
        selection=dg.AssetSelection.assets(
            "publish_sp_io_catalog", "publish_sp_masterdata_catalog", "publish_sp_schema_catalog"
        ).upstream(),
        description="Job for publishing documentation.",
    ),
    cron_schedule="0 11 * * 0",  # sunday at 11:00
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

publish_sp_job = dg.ScheduleDefinition(
    job=dg.define_asset_job(
        name="test_job",
        selection=dg.AssetSelection.assets("hola").upstream(),
        description="GE Eventos",
    ),
    cron_schedule="30 6 * * *",
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)


# === RELIABILITY ===

ep_job = dg.ScheduleDefinition(
    job=dg.define_asset_job(
        name="ep_job",
        selection=dg.AssetSelection.assets("publish_sp_ep").upstream(),
        description="...",
    ),
    cron_schedule="0 11 * * 0",  # sunday at 11:00
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


warranties_job = dg.ScheduleDefinition(
    job=dg.define_asset_job(
        name="warranties_job",
        selection=dg.AssetSelection.assets("mutate_warranties").upstream(),
        description="...",
    ),
    cron_schedule="0 11 * * 0",  # sunday at 11:00
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

icc_job = dg.ScheduleDefinition(
    job=dg.define_asset_job(
        name="icc_job",
        selection=dg.AssetSelection.assets("mutate_icc").upstream(),
        tags={"source": "icc"},
    ),
    cron_schedule="0 21 * * *",  # Diario a las 21:00
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


component_reparations_job = dg.ScheduleDefinition(
    job=dg.define_asset_job(
        name="component_reparations_job",
        selection=dg.AssetSelection.assets("mutate_component_changeouts").upstream()
        | dg.AssetSelection.assets("mutate_so_report").upstream()
        | dg.AssetSelection.assets("mutate_component_reparations").upstream(),
        description="Cambios de componente",
    ),
    cron_schedule="0 9,21 * * *",  # daily at 9:00 and 21:00
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


pool_inventory_job = dg.ScheduleDefinition(
    job=dg.define_asset_job(
        name="pool_inventory_job",
        selection=dg.AssetSelection.assets(
            "component_serials", "mutate_component_lifeline", "mutate_component_states", "mutate_component_snapshots"
        ).upstream(),
    ),
    cron_schedule="0 21 * * *",  # Diario a las 21:00
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

# === REPARATION ===
quotations_job = dg.ScheduleDefinition(
    job=dg.define_asset_job(
        name="quotations_job",
        selection=dg.AssetSelection.assets(["mutate_quotations"]).upstream(),
    ),
    cron_schedule="0 10 * * FRI",
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

harvest_so_report_job = dg.ScheduleDefinition(
    job=dg.define_asset_job(
        name="harvest_so_report_job",
        selection=dg.AssetSelection.assets("harvest_so_report").upstream(),
        description="Component Status RESO",
    ),
    cron_schedule="0 7 * * *",
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

harvest_so_details_job = dg.ScheduleDefinition(
    job=dg.define_asset_job(
        name="harvest_so_details_job",
        selection=dg.AssetSelection.assets("harvest_so_details").upstream(),
        description="Component Status RESO",
    ),
    cron_schedule="0 7 * * *",
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

harvest_so_documents_job = dg.ScheduleDefinition(
    job=dg.define_asset_job(
        name="harvest_so_documents_job",
        selection=dg.AssetSelection.assets("harvest_so_documents").upstream(),
        description="Component Status RESO",
    ),
    cron_schedule="0 8 * * *",
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

# === MAINTENANCE ===

oil_analysis_job = dg.ScheduleDefinition(
    job=dg.define_asset_job(
        name="oil_analysis_job",
        selection=dg.AssetSelection.assets("mutate_oil_analysis").upstream(),
        description="Muestras aceite SCAAE",
    ),
    cron_schedule="15 11 * * *",  # daily at 11:15
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

pm_history_job = dg.ScheduleDefinition(
    job=dg.define_asset_job(
        name="pm_history_job",
        selection=dg.AssetSelection.assets("spawn_pm_history").upstream(),
        description="Archivo con listado historial de PMs",
    ),
    cron_schedule="0 9 * * FRI",
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

work_order_history_job = dg.ScheduleDefinition(
    job=dg.define_asset_job(
        name="work_order_history_job",
        selection=dg.AssetSelection.assets("spawn_work_order_history").upstream(),
        description="Archivo con todas las OT's Fiori",
    ),
    cron_schedule="0 9 1 * *",
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)


# === OPERATION ===

op_file_idx_job = dg.ScheduleDefinition(
    job=dg.define_asset_job(name="op_file_idx_job", selection=dg.AssetSelection.assets("spawn_op_file_idx").upstream()),
    cron_schedule="30 6 * * *",
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

plm_job = dg.ScheduleDefinition(
    job=dg.define_asset_job(
        name="plm_job",
        selection=dg.AssetSelection.assets("spawn_plm3_haul").upstream()
        | dg.AssetSelection.assets("spawn_plm3_alarms").upstream(),
        description="PLM Haulcycle y alarmas",
    ),
    cron_schedule="30 6 * * *",
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

ge_job = dg.ScheduleDefinition(
    job=dg.define_asset_job(
        name="ge_job",
        selection=dg.AssetSelection.assets("mutate_raw_events").upstream(),
        description="GE Eventos",
    ),
    cron_schedule="30 6 * * *",
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
