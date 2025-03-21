import pandas as pd
import polars as pl

# from airflow.decorators import task
#
# import resources.tidypolars as tp
# from kdags.assets.operations.assets.common.utils import *
# from kdags.assets.operations.assets.truck.effective_grade.mutate import EffectiveGrade
# from kdags.assets.operations.assets.truck.effective_grade.read import read_csv_eg
#
# __all__ = [
#     "read_csv_eg",
#     "EffectiveGrade",
#     "transmute_staging_effective_grade",
#     "transmute_analytics_effective_grade",
# ]
#
#
# @task
# def transmute_staging_effective_grade(local: bool = False) -> list:
#     spawned = spawn_staging(
#         "GE/EFFECTIVE_GRADE",
#         read_csv_eg,
#         ["faena", "header_model", "header_machine_name", "filename_serial", "Date"],
#         local=local,
#     )
#     return spawned
#
#
# @task
# def transmute_analytics_effective_grade() -> list:
#     files = tp.ListABFS("az://kcc-process-data/OPERACIONES/CHILE/KCH/GE/EFFECTIVE_GRADE").as_frame().to_pandas()
#     files = files.loc[files["faena"].isin(FAENAS)].reset_index(drop=True)
#     paths = files["az_path"].to_list()
#     df_raw = tp.ReadABFS.from_paths(paths).readr(pd.read_parquet)
#     df = EffectiveGrade().mutate(df_raw)
#     spawn_paths = tp.SpawnABFS.from_dicts(
#         {"az://kcc-analytics-data/OPERACIONES/CHILE/KCH/GE/EFFECTIVE_GRADE/effective_grade.parquet": pl.from_pandas(df)}
#     ).spawn_pl()
#     return spawn_paths
