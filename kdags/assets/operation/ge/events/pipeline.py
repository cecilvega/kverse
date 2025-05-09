import os
from pathlib import Path

import dagster as dg
import polars as pl

from kdags.resources.tidyr import DataLake, MSGraph
from .reader import read_csv_events


@dg.asset
def read_raw_events(context: dg.AssetExecutionContext, read_op_file_idx) -> pl.DataFrame:
    # Fix This part
    op_file_idx = (
        read_op_file_idx.filter(pl.col("data_type") == "EVENTS")
        .with_columns(filestem=pl.col("filepath").str.extract(r"([^/\\]+)(?:\.[^.]*)?$", 1))
        .filter(
            (pl.col("filestem").str.contains("events", literal=True))
            & (~pl.col("filestem").str.contains("events_ge", literal=True))
        )
    )

    filepaths = op_file_idx.select(["filepath", "equipment_name"]).to_dicts()
    tibbles = []
    for v in filepaths:
        content = Path(v["filepath"]).read_bytes()
        tibble = read_csv_events(content)
        if not tibble.is_empty():
            tibbles.append(
                tibble.with_columns(filepath=pl.lit(v["filepath"]), filepath_equipment_name=pl.lit(v["equipment_name"]))
            )

    df = pl.concat(tibbles)

    return df


@dg.asset
def mutate_raw_events(context: dg.AssetExecutionContext, read_raw_events):
    df = read_raw_events.clone().unique(["Time", "Event #", "Sub ID", "Type", "truck_id"])
    df = df.with_columns(
        record_dt=pl.col("Time")
        .str.replace_all('="', "")
        .str.replace_all('"', "")
        .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S.%f", strict=False)
    )
    df = (
        df.with_columns(Name=pl.col("Name").str.replace_all("--", "", literal=True))
        .with_columns(
            parameter_code=pl.concat_str(
                [
                    "E" + pl.col("Event #").cast(pl.String),
                    "S" + pl.col("Sub ID").cast(pl.String),
                ]
            ),
            parameter_name=pl.concat_str(
                [pl.col("Name"), pl.col("Sub ID Name")],
                separator=" -- ",
                ignore_nulls=True,
            ),
        )
        .drop(["#", "Event #", "Sub ID", "Name", "Sub ID Name"])
        .rename({"Type": "recording_type", "truck_id": "header_equipment_name"})
        .with_columns(header_equipment_name="TK" + pl.col("header_equipment_name").str.strip_chars(" "))
        .sort(["record_dt", "recording_type", "parameter_code"])
    )
    dl = DataLake()
    dl.upload_tibble(az_path="az://bhp-analytics-data/OPERATION/GE/events.parquet", tibble=df)
    return df


@dg.asset
def spawn_events(context: dg.AssetExecutionContext, mutate_raw_events):
    df = mutate_raw_events.clone()
    result = {}

    MSGraph().delete_file(
        site_id="KCHCLSP00022", filepath="/01. ÁREAS KCH/1.6 CONFIABILIDAD/CAEX/ANTECEDENTES/OPERATION/PLM/alarms.csv"
    )
    mutate_raw_events.drop(["filepath", "date_created"]).write_csv(
        Path(os.environ["ONEDRIVE_LOCAL_PATH"]) / "OPERATION/GE/events.csv"
    )
    result["sharepoint"] = {
        "file_url": "https://globalkomatsu.sharepoint.com/sites/KCHCLSP00022/Shared%20Documents/01.%20%C3%81REAS%20KCH/1.6%20CONFIABILIDAD"
        + "/CAEX/ANTECEDENTES/OPERATION/GE/events.csv",
        "format": "csv",
    }

    datalake_path = DataLake().upload_tibble(
        az_path="abfs://bhp-analytics-data/OPERATION/GE/events.parquet",
        tibble=df,
    )
    result["datalake"] = {"path": datalake_path, "format": "parquet"}

    # Add record count
    result["count"] = len(df)

    return result


@dg.asset
def read_events():
    dl = DataLake()
    uri = "abfs://bhp-analytics-data/OPERATION/GE/events.parquet"
    if dl.az_path_exists(uri):
        return dl.read_tibble(az_path=uri)
    else:
        return pl.DataFrame()
