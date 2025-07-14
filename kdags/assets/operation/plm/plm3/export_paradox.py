import dagster as dg
import polars as pl
from .paradox_utils import extract_from_paradox
from pathlib import Path
import os


def get_haul_path(df, partition_date):
    cust_unit = df["Cust_Unit"][0]
    if partition_date is None:
        # Get max date from either haul PDate or alarms Cleared_Date
        partition_date = df.select(pl.col("PDate").max()).item()
    return f"PLM3/HAUL/{cust_unit}/{partition_date.strftime('y=%Y/m=%m/d=%d')}/haul.csv"


def get_alarms_path(df, partition_date):
    cust_unit = df["Cust_Unit"][0]
    if partition_date is None:
        # Get max date from either haul PDate or alarms Cleared_Date
        partition_date = df.select(pl.col("Cleared_Date").max()).item()
    return f"PLM3/ALARMS/{cust_unit}/{partition_date.strftime('y=%Y/m=%m/d=%d')}/alarms.csv"


@dg.asset
def export_plm3(context, ddm_manifest: pl.DataFrame):
    data_list = (
        ddm_manifest.filter(pl.col("data_type") == "PLM3")
        .rename({"filepath": "zip_path"})
        .select(["zip_path", "partition_date"])
        .to_dicts()
    )

    for item in data_list:
        zip_path = item["zip_path"]
        partition_date = item.get("partition_date")
        try:
            haul_df, alarms_df = extract_from_paradox(zip_path)
        except Exception as e:
            print("Error processing", zip_path)
            haul_df, alarms_df = pl.DataFrame(), pl.DataFrame()
        # base_path = Path(os.environ["ONEDRIVE_LOCAL_PATH"]).parent / "BHPDATA/bhp-raw-data"
        base_path = Path(r"C:\Users\andmn\PycharmProjects\bhp-process-data")
        if not haul_df.is_empty():
            haul_path = base_path / get_haul_path(haul_df, partition_date)
            haul_path.parent.mkdir(parents=True, exist_ok=True)
            haul_df.write_csv(haul_path)
        if not alarms_df.is_empty():
            alarms_path = base_path / get_alarms_path(alarms_df, partition_date)
            alarms_path.parent.mkdir(parents=True, exist_ok=True)
            alarms_df.write_csv(alarms_path)
