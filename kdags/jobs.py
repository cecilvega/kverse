from dagster import (
    AssetSelection,
    ScheduleDefinition,
    define_asset_job,
    DefaultScheduleStatus,
)

# Maintenance Jobs
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
        selection=AssetSelection.assets("spawn_oil_analysis").upstream(),
        description="Muestras aceite SCAAE",
    ),
    cron_schedule="30 6 * * *",
    execution_timezone="America/Santiago",
    default_status=DefaultScheduleStatus.RUNNING,
)


icc_job = ScheduleDefinition(
    job=define_asset_job(
        name="icc_job",
        selection=AssetSelection.assets("spawn_icc").upstream(),
        tags={"source": "icc"},
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

# Operation Jobs

op_file_idx_job = ScheduleDefinition(
    job=define_asset_job(
        name="op_file_index_job",
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
