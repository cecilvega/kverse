import dagster as dg
from dagster import DefaultSensorStatus


@dg.asset_sensor(
    asset_key=dg.AssetKey("harvest_so_report"),
    job_name="so_report_job",
    default_status=DefaultSensorStatus.RUNNING,
)
def so_report_sensor(context: dg.SensorEvaluationContext):
    return dg.RunRequest()


@dg.asset_sensor(
    asset_key=dg.AssetKey("harvest_so_details"),
    job_name="quotations_job",
    default_status=DefaultSensorStatus.RUNNING,
)
def quotations_sensor(context: dg.SensorEvaluationContext):
    return dg.RunRequest()


# @dg.asset_sensor(
#     asset_key=dg.AssetKey("harvest_reso_job"),
#     job_name="so_details_job",
#     default_status=DefaultSensorStatus.RUNNING,
# )
# def so_report_sensor(context: dg.SensorEvaluationContext):
#     return dg.RunRequest()
