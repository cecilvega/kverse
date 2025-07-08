import dagster as dg
from kdags.jobs import (
    component_history_job,
    harvest_so_report_job,
    publish_data_job,
    oil_analysis_job,
    fiori_job,
    harvest_so_details_job,
)

__all__ = ["schedules"]


# === DOCS ===
publish_data_schedule = dg.ScheduleDefinition(
    name="publish_data_schedule",
    job=publish_data_job,
    cron_schedule="0 20 * * *",
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

ep_schedule = dg.ScheduleDefinition(
    name="ep_schedule",
    job=component_history_job,
    cron_schedule="0 0 * * FRI",  # daily at 9:00 and 21:00
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

fiori_schedule = dg.ScheduleDefinition(
    name="fiori_schedule",
    job=fiori_job,
    cron_schedule="0 0 * * THU",  # daily at 9:00 and 21:00
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


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
    cron_schedule="0 6 * * *",
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

harvest_so_details_schedule = dg.ScheduleDefinition(
    name="harvest_so_details_schedule",
    job=harvest_so_details_job,
    cron_schedule="45 6 * * *",
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

# === MAINTENANCE ===

oil_analysis_schedule = dg.ScheduleDefinition(
    name="oil_analysis_schedule",
    job=oil_analysis_job,
    cron_schedule="15 11 * * *",  # daily at 11:15
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


schedules = [
    component_history_schedule,
    harvest_so_report_schedule,
    oil_analysis_schedule,
    publish_data_schedule,
    ep_schedule,
    fiori_schedule,
    harvest_so_details_schedule,
]
