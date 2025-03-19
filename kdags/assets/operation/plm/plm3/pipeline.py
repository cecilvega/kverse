import os
from datetime import datetime
from pathlib import Path

import dagster as dg
import polars as pl

from kdags.resources.tidyr import DataLake, MSGraph


@dg.asset
def mutate_plm3_haul(context: dg.AssetExecutionContext) -> pl.DataFrame:
    file_paths = [
        f
        for f in (Path(os.environ["ONEDRIVE_LOCAL_PATH"]).parent / "BHPDATA/bhp-raw-data/PLM3/HAUL").rglob("*")
        if f.is_file()
    ]
    frames = []
    for file_path in file_paths:
        frames.append(pl.read_csv(file_path).drop(["Operator_ID"]))
    df = pl.concat(frames).unique(["Cust_Unit", "PDate", "PTime"])

    df = (
        df.with_columns(
            [
                pl.col("PDate").str.strptime(pl.Date, "%Y-%m-%d").alias("PDate"),
                pl.col("PTime").str.to_time("%H:%M:%S.%f").alias("PTime"),
            ]
        )
        .with_columns(record_dt=pl.col("PDate").dt.combine(pl.col("PTime")))
        .drop(["PDate", "PTime"])
        .rename(
            {
                "Cust_Unit": "equipment_name",
            }
        )
        .rename(lambda col_name: col_name.lower())
        .filter(pl.col("record_dt") >= datetime(2020, 10, 1))
        .sort(["equipment_name", "record_dt"])
    )

    return df


@dg.asset
def mutate_plm3_alarms(context: dg.AssetExecutionContext) -> pl.DataFrame:
    file_paths = [
        f
        for f in (Path(os.environ["ONEDRIVE_LOCAL_PATH"]).parent / "BHPDATA/bhp-raw-data/PLM3/ALARMS").rglob("*")
        if f.is_file()
    ]
    frames = []
    for file_path in file_paths:
        frames.append(pl.read_csv(file_path))
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
    return df


@dg.asset
def spawn_plm3_haul(context: dg.AssetExecutionContext, mutate_plm3_haul: pl.DataFrame) -> dict:
    df = mutate_plm3_haul.clone()
    result = {}
    MSGraph().delete_file(
        site_id="KCHCLSP00022", filepath="/01. ÁREAS KCH/1.6 CONFIABILIDAD/CAEX/ANTECEDENTES/OPERATION/PLM/haul.csv"
    )
    mutate_plm3_haul.write_csv(Path(os.environ["ONEDRIVE_LOCAL_PATH"]) / "OPERATION/PLM/haul.csv")

    result["sharepoint"] = {
        "file_url": "https://globalkomatsu.sharepoint.com/sites/KCHCLSP00022/Shared%20Documents/01.%20%C3%81REAS%20KCH/1.6%20CONFIABILIDAD"
        + "/CAEX/ANTECEDENTES/OPERATION/PLM/haul.csv",
        "format": "csv",
    }

    datalake_path = DataLake().upload_tibble(
        uri="abfs://bhp-analytics-data/OPERATION/PLM3/haul.parquet",
        df=df,
        format="parquet",
    )
    result["datalake"] = {"path": datalake_path, "format": "parquet"}

    result["count"] = len(df)

    return result


@dg.asset
def spawn_plm3_alarms(context: dg.AssetExecutionContext, mutate_plm3_alarms: pl.DataFrame) -> dict:
    df = mutate_plm3_alarms.clone()
    result = {}

    MSGraph().delete_file(
        site_id="KCHCLSP00022", filepath="/01. ÁREAS KCH/1.6 CONFIABILIDAD/CAEX/ANTECEDENTES/OPERATION/PLM/alarms.csv"
    )
    mutate_plm3_alarms.write_csv(Path(os.environ["ONEDRIVE_LOCAL_PATH"]) / "OPERATION/PLM/alarms.csv")
    result["sharepoint"] = {
        "file_url": "https://globalkomatsu.sharepoint.com/sites/KCHCLSP00022/Shared%20Documents/01.%20%C3%81REAS%20KCH/1.6%20CONFIABILIDAD"
        + "/CAEX/ANTECEDENTES/OPERATION/PLM/haul.csv",
        "format": "csv",
    }

    datalake_path = DataLake().upload_tibble(
        uri="abfs://bhp-analytics-data/OPERATION/PLM3/alarms.parquet",
        df=df,
        format="parquet",
    )
    result["datalake"] = {"path": datalake_path, "format": "parquet"}

    # Add record count
    result["count"] = len(df)

    return result
