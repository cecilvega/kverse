from dagster import (
    AssetSelection,
    ScheduleDefinition,
    define_asset_job,
    DefaultScheduleStatus,
)

# === PLANNING ===
component_changeouts_job = ScheduleDefinition(
    job=define_asset_job(
        name="component_changeouts_job",
        selection=AssetSelection.assets("component_changeouts").upstream(),
        description="Cambios de componente",
    ),
    cron_schedule="30 6 * * *",
    execution_timezone="America/Santiago",
    default_status=DefaultScheduleStatus.RUNNING,
)
# === REPARATION ===
scrape_component_status_job = ScheduleDefinition(
    job=define_asset_job(
        name="scrape_component_status_job",
        selection=AssetSelection.assets("scrape_component_status").upstream(),
        description="Component Status RESO",
    ),
    cron_schedule="0 9 * * FRI",
    execution_timezone="America/Santiago",
    default_status=DefaultScheduleStatus.RUNNING,
)

component_status_job = ScheduleDefinition(
    job=define_asset_job(
        name="component_status_job",
        selection=AssetSelection.assets("component_status").upstream(),
        description="Component Status RESO",
    ),
    cron_schedule="0 9 * * FRI",
    execution_timezone="America/Santiago",
    default_status=DefaultScheduleStatus.RUNNING,
)


# === MAINTENANCE ===
pm_history_job = ScheduleDefinition(
    job=define_asset_job(
        name="pm_history_job",
        selection=AssetSelection.assets("spawn_pm_history").upstream(),
        description="Archivo con listado historial de PMs",
    ),
    cron_schedule="0 9 * * FRI",
    execution_timezone="America/Santiago",
    default_status=DefaultScheduleStatus.RUNNING,
)

work_order_history_job = ScheduleDefinition(
    job=define_asset_job(
        name="work_order_history_job",
        selection=AssetSelection.assets("spawn_work_order_history").upstream(),
        description="Archivo con todas las OT's Fiori",
    ),
    cron_schedule="0 9 1 * *",
    execution_timezone="America/Santiago",
    default_status=DefaultScheduleStatus.RUNNING,
)

oil_analysis_job = ScheduleDefinition(
    job=define_asset_job(
        name="oil_analysis_job",
        selection=AssetSelection.assets("publish_sharepoint_oil_analysis").upstream(),
        description="Muestras aceite SCAAE",
    ),
    cron_schedule="30 6 * * *",
    execution_timezone="America/Santiago",
    default_status=DefaultScheduleStatus.RUNNING,
)

attendances_job = ScheduleDefinition(
    job=define_asset_job(name="attentances_job", selection=AssetSelection.groups("maintenance")),
    cron_schedule="0 0 * * *",
    execution_timezone="America/Santiago",
    # default_status=DefaultScheduleStatus.RUNNING,
)

# === RELIABILITY ===

icc_job = ScheduleDefinition(
    job=define_asset_job(
        name="icc_job",
        selection=AssetSelection.assets("publish_sharepoint_icc").upstream(),
        tags={"source": "icc"},
    ),
    cron_schedule="30 6 * * *",
    execution_timezone="America/Santiago",
    default_status=DefaultScheduleStatus.RUNNING,
)


component_reparations_job = ScheduleDefinition(
    job=define_asset_job(
        name="component_reparations_job",
        selection=AssetSelection.assets("component_reparations").upstream(),
        tags={"source": "icc"},
    ),
    cron_schedule="30 6 * * *",
    execution_timezone="America/Santiago",
    default_status=DefaultScheduleStatus.RUNNING,
)

# === OPERATION ===

op_file_idx_job = ScheduleDefinition(
    job=define_asset_job(
        name="op_file_idx_job",
        selection=AssetSelection.assets("spawn_op_file_idx").upstream(),
        tags={"source": "icc"},
    ),
    cron_schedule="30 6 * * *",
    execution_timezone="America/Santiago",
    default_status=DefaultScheduleStatus.RUNNING,
)

plm_job = ScheduleDefinition(
    job=define_asset_job(
        name="plm_job",
        selection=AssetSelection.assets("spawn_plm3_haul").upstream()
        | AssetSelection.assets("spawn_plm3_alarms").upstream(),
        description="PLM Haulcycle y alarmas",
    ),
    cron_schedule="30 6 * * *",
    execution_timezone="America/Santiago",
    default_status=DefaultScheduleStatus.RUNNING,
)

ge_job = ScheduleDefinition(
    job=define_asset_job(
        name="ge_job",
        selection=AssetSelection.assets("spawn_events").upstream(),
        description="GE Eventos",
    ),
    cron_schedule="30 6 * * *",
    execution_timezone="America/Santiago",
    default_status=DefaultScheduleStatus.RUNNING,
)
