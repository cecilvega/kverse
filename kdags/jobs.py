from dagster import AssetSelection, ScheduleDefinition, define_asset_job

daily_refresh_schedule = ScheduleDefinition(
    job=define_asset_job(name="hackernews_assets_job", selection=AssetSelection.groups("hackernews")),
    cron_schedule="0 0 * * *",
)
