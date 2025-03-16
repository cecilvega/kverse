import os
import tempfile
from zipfile import ZipFile
import dagster as dg
import polars as pl
from pypxlib import Table
import pandas as pd
from kdags.resources.tidyr import DataLake, MSGraph
from pathlib import Path


@dg.asset
def mutate_plm3_haul(context: dg.AssetExecutionContext) -> pl.DataFrame:
    file_paths = [
        f
        for f in (Path(os.environ["ONEDRIVE_LOCAL_PATH"]).parent / "BHPDATA/bhp-raw-data/PLM3/HAUL").rglob("*")
        if f.is_file()
    ]
    frames = []
    for file_path in file_paths:
        frames.append(pl.read_csv(file_path))
    df = pl.concat(frames)

    df = (
        df.with_columns(pl.col("PDate").str.strptime(pl.Date, "%Y-%m-%d"))
        .unique(["Cust_Unit", "PDate", "PTime"])
        .sort(["Cust_Unit", "PDate", "PTime"])
        .rename({"PDate": "metric_date", "Cust_Unit": "equipment_name"})
    )

    df = df.drop(["Operator_ID"])
    # First, convert the date string to a date type
    # df = df.with_columns(pl.col("metric_date").str.to_date("%Y-%m-%d").alias("metric_date"))

    # Then, convert the time string to a time type
    df = df.with_columns(pl.col("PTime").str.to_time().alias("PTime"))

    # Finally, combine the date and time columns into a datetime
    df = df.with_columns(pl.col("metric_date").dt.combine(pl.col("PTime")).alias("metric_datetime")).filter(
        pl.col("metric_datetime").dt.year() >= 2021
    )
    df = df.drop(["PTime"])
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
    df = pl.concat(frames)

    df = (
        df.with_columns(pl.col("Cleared_Date").str.strptime(pl.Date, "%Y-%m-%d"))
        .unique(["Cust_Unit", "Cleared_Date", "Cleared_Time"])
        .sort(["Cust_Unit", "Cleared_Date", "Cleared_Time"])
        .rename({"Cleared_Date": "metric_date", "Cust_Unit": "equipment_name"})
    )
    return df


@dg.asset
def spawn_plm3_haul(context: dg.AssetExecutionContext, mutate_plm3_haul: pl.DataFrame) -> dict:
    df = mutate_plm3_haul.clone()
    result = {}
    MSGraph().delete_file(
        site_id="KCHCLSP00022", file_path="/01. ÁREAS KCH/1.6 CONFIABILIDAD/CAEX/ANTECEDENTES/OPERATION/PLM/haul.csv"
    )
    mutate_plm3_haul.write_csv(Path(os.environ["ONEDRIVE_LOCAL_PATH"]) / "OPERATION/PLM/haul.csv")
    # sharepoint_result = MSGraph().upload_tibble(
    #     site_id="KCHCLSP00022",
    #     file_path="/01. ÁREAS KCH/1.6 CONFIABILIDAD/CAEX/ANTECEDENTES/OPERATION/PLM/haul.csv",
    #     df=df,
    #     format="csv",
    # )
    result["sharepoint"] = {
        "file_url": "https://globalkomatsu.sharepoint.com/sites/KCHCLSP00022/Shared%20Documents/01.%20%C3%81REAS%20KCH/1.6%20CONFIABILIDAD"
        + "/CAEX/ANTECEDENTES/OPERATION/PLM/haul.csv",
        "format": "csv",
    }

    # 2. Upload to Data Lake as Parquet

    datalake_path = DataLake().upload_tibble(
        uri="abfs://bhp-analytics-data/OPERATION/PLM3/haul.parquet",
        df=df,
        format="parquet",
    )
    result["datalake"] = {"path": datalake_path, "format": "parquet"}

    # Add record count
    result["count"] = len(df)

    return result


@dg.asset
def spawn_plm3_alarms(context: dg.AssetExecutionContext, mutate_plm3_alarms: pl.DataFrame) -> dict:
    df = mutate_plm3_alarms.clone()
    result = {}
    # sharepoint_result = MSGraph().upload_tibble(
    #     site_id="KCHCLSP00022",
    #     file_path="/01. ÁREAS KCH/1.6 CONFIABILIDAD/CAEX/ANTECEDENTES/OPERATION/PLM/alarms.csv",
    #     df=df,
    #     format="csv",
    # )
    # result["sharepoint"] = {"file_url": sharepoint_result.web_url, "format": "csv"}
    MSGraph().delete_file(
        site_id="KCHCLSP00022", file_path="/01. ÁREAS KCH/1.6 CONFIABILIDAD/CAEX/ANTECEDENTES/OPERATION/PLM/alarms.csv"
    )
    mutate_plm3_alarms.write_csv(Path(os.environ["ONEDRIVE_LOCAL_PATH"]) / "OPERATION/PLM/alarms.csv")
    result["sharepoint"] = {
        "file_url": "https://globalkomatsu.sharepoint.com/sites/KCHCLSP00022/Shared%20Documents/01.%20%C3%81REAS%20KCH/1.6%20CONFIABILIDAD"
        + "/CAEX/ANTECEDENTES/OPERATION/PLM/haul.csv",
        "format": "csv",
    }

    # 2. Upload to Data Lake as Parquet

    datalake_path = DataLake().upload_tibble(
        uri="abfs://bhp-analytics-data/OPERATION/PLM3/alarms.parquet",
        df=df,
        format="parquet",
    )
    result["datalake"] = {"path": datalake_path, "format": "parquet"}

    # Add record count
    result["count"] = len(df)

    return result
