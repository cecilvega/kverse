import os
from datetime import datetime
from pathlib import Path

import dagster as dg
import polars as pl

from kdags.resources.tidyr import DataLake, MSGraph


@dg.asset
def mutate_plm3_haul(context: dg.AssetExecutionContext) -> pl.DataFrame:
    column_mapping = {
        "Cust_Unit": "equipment_name",  # Truck #
        "PDate": "record_date",  # Date
        "PTime": "record_dt",  # Time
        "Total_Payload": "payload_net",  # Payload (Net)
        "Swingload_Num": "swingloads",  # Swingloads
        # "": "Operator ID",  # Operator ID
        # "": "Status",  # Status
        # "": "Flags",  # Flags
        "Empty_Carry_Back_Load": "carry_back",  # Carry Back
        "Total_Cycle_Time": "total_cycle_time",  # Total Cycle Time
        "Empty_Haul_Time": "empty_run_time",  # E-Run Time
        "Empty_Stop_Time": "empty_stop_time",  # E-Stop Time
        "Loading_Time": "loading_time",  # Loading Time
        "Loaded_Haul_Time": "loaded_haul_time",  # L-Run Time
        "Loaded_Stop_Time": "loaded_stop_time",  # L-Stop Time
        "Dumping_Time": "dumping_time",  # Dumping Time
        "Loaded_Start_Time": "loaded_start_time",  # Loading Start Time
        "Time_Body_Lift_Off": "dumping_start_time",  # Dumping Start Time
        "Loaded_Haul_Distance": "loaded_haul_distance",  # L-Haul Distance
        "Empty_Haul_Distance": "empty_haul_distance",  # E-Haul Distance
        "Loaded_Max_Speed": "loaded_max_speed",  # L-Max Speed
        "Time_Loaded_Max_Speed": "loaded_max_speed_time",  # L-Max Speed Time
        "Empty_Max_Speed": "empty_max_speed",  # E-Max Speed
        "Time_Empty_Max_Speed": "empty_max_speed_time",  # E-Max Speed Time
        "TimePeakPosFrameTorque": "peak_positive_torque_time",  # Max +T Time
        "Peak_Pos_Frame_Torque": "peak_positive_torque",  # Max +T
        "TimePeakNegFrameTorque": "peak_negative_torque_time",  # Max -T Time
        "Peak_Neg_Frame_Torque": "peak_negative_torque",  # Max -T
        "Peak_Sprung_Load": "peak_sprung_load",  # Max Sprung
        "Time_Peak_Sprung_Load": "peak_sprung_load_time",  # Max Sprung Time
        "Lft_Front_Tire_Ton_mph": "left_front_tire_tkph",  # LF TKPH
        "Rt_Front_Tire_Ton_mph": "right_front_tire_tkph",  # RF TKPH
        "Rear_Tire_Ton_mph": "rear_tire_tkph",  # R TKPH
        "Gross_Payload": "gross_payload",  # Gross Payload
    }

    distance_columns = [
        "loaded_haul_distance",
        "empty_haul_distance",
        "loaded_max_speed",
        "empty_max_speed",
        "left_front_tire_tkph",
        "right_front_tire_tkph",
        "rear_tire_tkph",
    ]
    time_columns = [
        "loaded_start_time",
        "dumping_start_time",
        "loaded_max_speed_time",
        "empty_max_speed_time",
        "peak_positive_torque_time",
        "peak_negative_torque_time",
        "peak_sprung_load_time",
    ]
    filepaths = [
        f for f in Path(r"C:\Users\andmn\PycharmProjects\bhp-process-data\PLM3\HAUL").rglob("*") if f.is_file()
    ]
    frames = []
    for filepath in filepaths:
        frames.append(pl.read_csv(filepath).drop(["Operator_ID"]))
    df = pl.concat(frames).unique(["Cust_Unit", "PDate", "PTime"])

    df = (
        df.with_columns(
            [
                pl.col("PDate").str.strptime(pl.Date, "%Y-%m-%d").alias("PDate"),
                pl.col("PTime").str.to_time("%H:%M:%S.%f").alias("PTime"),
            ]
        )
        .with_columns(PTime=pl.col("PDate").dt.combine(pl.col("PTime")))
        .rename(column_mapping)
        .select(list(column_mapping.values()))
        .with_columns([pl.from_epoch(pl.col(c), time_unit="s").dt.time().alias(c) for c in time_columns])
        .with_columns([(pl.col(c) * 1.609344).round(2).alias(c) for c in distance_columns])
        .filter(pl.col("record_dt") >= datetime(2020, 10, 1))
        .sort(["equipment_name", "record_dt"])
    )
    df.write_parquet(r"C:\Users\andmn\PycharmProjects\haul.parquet")
    df.write_csv(r"C:\Users\andmn\PycharmProjects\haul.csv")
    return df


@dg.asset
def mutate_plm3_alarms(context: dg.AssetExecutionContext) -> pl.DataFrame:
    filepaths = [
        f for f in Path(r"C:\Users\andmn\PycharmProjects\bhp-process-data\PLM3\ALARMS").rglob("*") if f.is_file()
    ]
    frames = []
    for filepath in filepaths:
        frames.append(pl.read_csv(filepath))
    df = pl.concat(frames).unique(["Cust_Unit", "Cleared_Date", "Set_Date", "Set_Time", "Cleared_Time", "Description"])

    date_cols = ["Cleared_Date", "Set_Date"]
    time_cols = ["Set_Time", "Cleared_Time"]
    df = (
        df.with_columns([pl.col(col).str.strptime(pl.Date, "%Y-%m-%d").alias(col) for col in date_cols])
        .with_columns([pl.col(col).str.to_time("%H:%M:%S.%f").alias(col) for col in time_cols])
        .with_columns(
            record_start_dt=pl.col("Set_Date").dt.combine(pl.col("Set_Time")),
            record_end_dt=pl.col("Cleared_Date").dt.combine(pl.col("Cleared_Time")),
        )
        .drop(["Cleared_Date", "Set_Date", "Set_Time", "Cleared_Time"])
        .rename(
            {
                "Cust_Unit": "equipment_name",
                "Alarm_Type": "parameter_code",
                "Description": "parameter_name",
            }
        )
        .rename(lambda col_name: col_name.lower())
        .filter(pl.col("record_start_dt") >= datetime(2020, 10, 1))
        .sort(["equipment_name", "record_start_dt", "record_end_dt"])
    )

    df.write_parquet(r"C:\Users\andmn\PycharmProjects\alarms.parquet")
    df.write_csv(r"C:\Users\andmn\PycharmProjects\alarms.csv")
    return df


@dg.asset
def spawn_plm3_haul(context: dg.AssetExecutionContext, mutate_plm3_haul: pl.DataFrame) -> dict:
    df = mutate_plm3_haul.to_pandas()
    result = {}
    # MSGraph().delete_file(
    #     site_id="KCHCLSP00022", filepath="/01. ÁREAS KCH/1.6 CONFIABILIDAD/CAEX/ANTECEDENTES/OPERATION/PLM/haul.csv"
    # )
    # mutate_plm3_haul.write_csv(Path(os.environ["ONEDRIVE_LOCAL_PATH"]) / "OPERATION/PLM/haul.csv")
    #
    # result["sharepoint"] = {
    #     "file_url": "https://globalkomatsu.sharepoint.com/sites/KCHCLSP00022/Shared%20Documents/01.%20%C3%81REAS%20KCH/1.6%20CONFIABILIDAD"
    #     + "/CAEX/ANTECEDENTES/OPERATION/PLM/haul.csv",
    #     "format": "csv",
    # }

    datalake_path = DataLake().upload_tibble(
        az_path="az://bhp-analytics-data/OPERATION/PLM3/haul.parquet",
        tibble=df,
    )
    result["datalake"] = {"path": datalake_path, "format": "parquet"}

    result["count"] = len(df)

    return result


@dg.asset
def spawn_plm3_alarms(context: dg.AssetExecutionContext, mutate_plm3_alarms: pl.DataFrame) -> dict:
    df = mutate_plm3_alarms.to_pandas()
    result = {}

    # MSGraph().delete_file(
    #     site_id="KCHCLSP00022", filepath="/01. ÁREAS KCH/1.6 CONFIABILIDAD/CAEX/ANTECEDENTES/OPERATION/PLM/alarms.csv"
    # )
    # mutate_plm3_alarms.write_csv(Path(os.environ["ONEDRIVE_LOCAL_PATH"]) / "OPERATION/PLM/alarms.csv")
    # result["sharepoint"] = {
    #     "file_url": "https://globalkomatsu.sharepoint.com/sites/KCHCLSP00022/Shared%20Documents/01.%20%C3%81REAS%20KCH/1.6%20CONFIABILIDAD"
    #     + "/CAEX/ANTECEDENTES/OPERATION/PLM/haul.csv",
    #     "format": "csv",
    # }

    datalake_path = DataLake().upload_tibble(
        az_path="az://bhp-analytics-data/OPERATION/PLM3/alarms.parquet",
        tibble=df,
    )
    result["datalake"] = {"path": datalake_path, "format": "parquet"}

    # Add record count
    result["count"] = len(df)

    return result
