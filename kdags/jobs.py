from dagster import (
    AssetSelection,
    ScheduleDefinition,
    define_asset_job,
    DefaultScheduleStatus,
)

pm_history_job = ScheduleDefinition(
    job=define_asset_job(
        name="maintenance_pm_history_job",
        selection=AssetSelection.assets("materialize_pm_history").upstream(),
        description="Archivo con listado historial de PMs",
    ),
    cron_schedule="0 9 * * FRI",
    execution_timezone="America/Santiago",
    default_status=DefaultScheduleStatus.RUNNING,
)

work_order_history_job = ScheduleDefinition(
    job=define_asset_job(
        name="maintenance_work_order_history_job",
        selection=AssetSelection.assets("materialize_work_order_history").upstream(),
        description="Archivo con todas las OT's Fiori",
    ),
    cron_schedule="0 9 * * FRI",
    execution_timezone="America/Santiago",
    default_status=DefaultScheduleStatus.RUNNING,
)


icc_job = ScheduleDefinition(
    job=define_asset_job(
        name="maintenance_icc_job",
        selection=AssetSelection.assets("materialize_icc").upstream(),
        tags={"source": "icc"},
    ),
    cron_schedule="0 9 * * FRI",
    execution_timezone="America/Santiago",
    default_status=DefaultScheduleStatus.RUNNING,
)


attendances_job = ScheduleDefinition(
    job=define_asset_job(name="attentances_job", selection=AssetSelection.groups("maintenance")),
    cron_schedule="0 0 * * *",
    execution_timezone="America/Santiago",
    # default_status=DefaultScheduleStatus.RUNNING,
)
