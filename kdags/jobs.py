from dagster import (
    AssetSelection,
    ScheduleDefinition,
    define_asset_job,
    DefaultScheduleStatus,
)


icc_job = ScheduleDefinition(
    job=define_asset_job(
        name="icc_job",
        selection=AssetSelection.assets("materialize_sp_icc").upstream(),
    ),
    cron_schedule="0 0 * * *",
)


attendances_job = ScheduleDefinition(
    job=define_asset_job(
        name="attentances_job", selection=AssetSelection.groups("maintenance")
    ),
    cron_schedule="0 0 * * *",
    # default_status=DefaultScheduleStatus.RUNNING,
)
