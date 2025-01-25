from dagster import AssetSelection, ScheduleDefinition, define_asset_job, DefaultScheduleStatus

daily_refresh_schedule = ScheduleDefinition(
    job=define_asset_job(name="hackernews_assets_job", selection=AssetSelection.groups("hackernews")),
    cron_schedule="0 0 * * *",
)


daily_attendances_job = ScheduleDefinition(
    job=define_asset_job(name="attentances_job", selection=AssetSelection.groups("maintenance")),
    cron_schedule="0 0 * * *",
    default_status=DefaultScheduleStatus.RUNNING,
)
