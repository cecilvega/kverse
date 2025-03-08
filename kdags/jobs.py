from dagster import (
    AssetSelection,
    ScheduleDefinition,
    define_asset_job,
    DefaultScheduleStatus,
)

fiori_job = ScheduleDefinition(
    job=define_asset_job(
        name="fiori_job",
        selection=AssetSelection.assets("materialize_work_order_history").upstream(),
    ),
    cron_schedule="0 9 * * FRI",
    execution_timezone="America/Santiago",
    default_status=DefaultScheduleStatus.RUNNING,
)


icc_job = ScheduleDefinition(
    job=define_asset_job(
        name="icc_job",
        selection=AssetSelection.assets("materialize_sp_icc").upstream(),
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
