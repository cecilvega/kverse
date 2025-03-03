# import pandas as pd
# import polars as pl
# from airflow.decorators import task
#
# import resources.tidypolars as tp
# from kdags.assets.operations.common.utils import *
# from kdags.assets.operations.truck.torque_swing.mutate import TorqueSwing
# from kdags.assets.operations.truck.torque_swing.read import read_csv_trqsw
#
# __all__ = ["read_csv_trqsw", "TorqueSwing", "transmute_staging_torque_swing", "transmute_analytics_torque_swing"]
#
#
# @task
# def transmute_staging_torque_swing(local: bool = False) -> list:
#     spawned = spawn_staging(
#         "GE/TORQUE_SWING",
#         read_csv_trqsw,
#         ["faena", "header_model", "header_machine_name", "filename_serial", "Date"],
#         local=local,
#     )
#     return spawned
#
#
# @task
# def transmute_analytics_torque_swing() -> list:
#     files = tp.ListABFS("az://kcc-process-data/OPERACIONES/CHILE/KCH/GE/TORQUE_SWING").as_frame().to_pandas()
#     files = files.loc[files["faena"].isin(FAENAS)].reset_index(drop=True)
#     paths = files["az_path"].to_list()
#     df = tp.ReadABFS.from_paths(paths).readr(pd.read_parquet)
#     df = TorqueSwing().mutate(df)
#     spawn_paths = tp.SpawnABFS.from_dicts(
#         {"az://kcc-analytics-data/OPERACIONES/CHILE/KCH/GE/TORQUE_SWING/torque_swing.parquet": pl.from_pandas(df)}
#     ).spawn_pl()
#     return spawn_paths
