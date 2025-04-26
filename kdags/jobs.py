from dagster import (
    AssetSelection,
    ScheduleDefinition,
    define_asset_job,
    DefaultScheduleStatus,
)

# === RELIABILITY ===
management_job = ScheduleDefinition(
    job=define_asset_job(
        name="management_job",
        selection=AssetSelection.assets("publish_sp_masterdata").upstream(),
        description="...",
    ),
    cron_schedule="0 11 * * 0",  # sunday at 11:00
    execution_timezone="America/Santiago",
    default_status=DefaultScheduleStatus.RUNNING,
)

# === PLANNING ===
component_changeouts_job = ScheduleDefinition(
    job=define_asset_job(
        name="component_changeouts_job",
        selection=AssetSelection.assets("component_changeouts").upstream(),
        description="Cambios de componente",
    ),
    cron_schedule="0 3,15 * * *",  # daily at 3:00 and 15:00
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
    cron_schedule="0 9 * * FRI",  # viernes 09:00
    execution_timezone="America/Santiago",
    default_status=DefaultScheduleStatus.RUNNING,
)

component_status_job = ScheduleDefinition(
    job=define_asset_job(
        name="component_status_job",
        selection=AssetSelection.assets("publish_sp_component_status").upstream(),
        description="Component Status RESO",
    ),
    cron_schedule="0 10 * * FRI",
    execution_timezone="America/Santiago",
    default_status=DefaultScheduleStatus.RUNNING,
)


# === MAINTENANCE ===

oil_analysis_job = ScheduleDefinition(
    job=define_asset_job(
        name="oil_analysis_job",
        selection=AssetSelection.assets("publish_sp_oil_analysis").upstream(),
        description="Muestras aceite SCAAE",
    ),
    cron_schedule="15 11 * * *",  # daily at 11:00
    execution_timezone="America/Santiago",
    default_status=DefaultScheduleStatus.RUNNING,
)

pm_history_job = ScheduleDefinition(
    job=define_asset_job(
        name="pm_history_job",
        selection=AssetSelection.assets("spawn_pm_history").upstream(),
        description="Archivo con listado historial de PMs",
    ),
    cron_schedule="0 9 * * FRI",
    execution_timezone="America/Santiago",
    default_status=DefaultScheduleStatus.STOPPED,
)

work_order_history_job = ScheduleDefinition(
    job=define_asset_job(
        name="work_order_history_job",
        selection=AssetSelection.assets("spawn_work_order_history").upstream(),
        description="Archivo con todas las OT's Fiori",
    ),
    cron_schedule="0 9 1 * *",
    execution_timezone="America/Santiago",
    default_status=DefaultScheduleStatus.STOPPED,
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
        selection=AssetSelection.assets("publish_sp_icc").upstream(),
        tags={"source": "icc"},
    ),
    cron_schedule="0 21 * * *",  # Diario a las 21:00
    execution_timezone="America/Santiago",
    default_status=DefaultScheduleStatus.RUNNING,
)

component_reparations_job = ScheduleDefinition(
    job=define_asset_job(
        name="component_reparations_job", selection=AssetSelection.assets("publish_sp_component_reparations").upstream()
    ),
    cron_schedule="0 10 * * FRI",
    execution_timezone="America/Santiago",
    default_status=DefaultScheduleStatus.RUNNING,
)
pool_rotation_job = ScheduleDefinition(
    job=define_asset_job(name="pool_rotation_job", selection=AssetSelection.assets("pool_rotation").upstream()),
    cron_schedule="0 10 * * FRI",
    execution_timezone="America/Santiago",
    default_status=DefaultScheduleStatus.RUNNING,
)

quotations_job = ScheduleDefinition(
    job=define_asset_job(name="quotations_job", selection=AssetSelection.assets("publish_sp_quotations").upstream()),
    cron_schedule="0 10 * * FRI",
    execution_timezone="America/Santiago",
    default_status=DefaultScheduleStatus.RUNNING,
)


# === OPERATION ===

op_file_idx_job = ScheduleDefinition(
    job=define_asset_job(name="op_file_idx_job", selection=AssetSelection.assets("spawn_op_file_idx").upstream()),
    cron_schedule="30 6 * * *",
    execution_timezone="America/Santiago",
    default_status=DefaultScheduleStatus.STOPPED,
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
    default_status=DefaultScheduleStatus.STOPPED,
)

ge_job = ScheduleDefinition(
    job=define_asset_job(
        name="ge_job",
        selection=AssetSelection.assets("spawn_events").upstream(),
        description="GE Eventos",
    ),
    cron_schedule="30 6 * * *",
    execution_timezone="America/Santiago",
    default_status=DefaultScheduleStatus.STOPPED,
)
