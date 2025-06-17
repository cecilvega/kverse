import dagster as dg
from kdags.jobs import harvest_so_report_job, component_history_job, harvest_so_report_job

component_history_schedule = dg.ScheduleDefinition(
    name="component_history_schedule",
    job=component_history_job,
    cron_schedule="0 9,21 * * *",  # daily at 9:00 and 21:00
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


harvest_so_report_schedule = dg.ScheduleDefinition(
    name="harvest_so_report_schedule",
    job=harvest_so_report_job,
    cron_schedule="0 7 * * *",
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

# === MAINTENANCE ===

oil_analysis_schedule = dg.ScheduleDefinition(
    name="oil_analysis_schedule",
    job=dg.define_asset_job(
        name="oil_analysis_job",
        selection=dg.AssetSelection.assets("mutate_oil_analysis").upstream(),
        description="Muestras aceite SCAAE",
    ),
    cron_schedule="15 11 * * *",  # daily at 11:15
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
