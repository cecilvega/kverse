import dagster as dg

component_reparations_schedule = dg.ScheduleDefinition(
    job=dg.define_asset_job(
        name="component_reparations_job",
        selection=dg.AssetSelection.assets("mutate_component_changeouts").upstream()
        | dg.AssetSelection.assets("mutate_so_report").upstream()
        | dg.AssetSelection.assets("mutate_component_reparations").upstream(),
        description="Cambios de componente",
    ),
    cron_schedule="0 9,21 * * *",  # daily at 9:00 and 21:00
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


harvest_so_report_schedule = dg.ScheduleDefinition(
    job=dg.define_asset_job(
        name="harvest_so_report_job",
        selection=dg.AssetSelection.assets("harvest_so_report").upstream(),
        description="Component Status RESO",
    ),
    cron_schedule="0 7 * * *",
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

# === MAINTENANCE ===

oil_analysis_schedule = dg.ScheduleDefinition(
    job=dg.define_asset_job(
        name="oil_analysis_job",
        selection=dg.AssetSelection.assets("mutate_oil_analysis").upstream(),
        description="Muestras aceite SCAAE",
    ),
    cron_schedule="15 11 * * *",  # daily at 11:15
    execution_timezone="America/Santiago",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
