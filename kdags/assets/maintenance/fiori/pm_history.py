import dagster as dg
import pandas as pd
from kdags.resources import DataLake, MSGraph
from kdags.config.masterdata import MasterData

# 105 o 103
# 600 3000 600 103 opción 1 area comercial


@dg.asset
def read_raw_pm_history():
    dl = DataLake()
    df = dl.read_tibble("bhp-raw-data", "FIORI/PM_HISTORY/pm_history.csv", use_polars=False)
    df = df.copy()

    # Define the status mapping to standardized English values
    status_mapping = {
        "completado": "completed",
        "complete": "completed",
        "completar": "completed",
        "manualmente completada": "manually_completed",
        "incomplete": "incomplete",
        "manually completed": "manually_completed",
        "en progreso": "in_progress",
        "programado": "scheduled",
        "parcial": "partial",
        "incompleto": "incomplete",
        "partial": "partial",
        "in progress": "in_progress",
    }

    df = df.rename(
        columns={
            "actual_start_time": "start_date",
            "actual_end_time": "end_date",
            "status": "pm_status",
            "file_stem": "pm_id",
        }
    )

    for c in ["start_date", "end_date"]:
        df[c] = df[c].str.replace("a.m.", "AM").str.replace("p.m.", "PM")
        df[c] = pd.to_datetime(df[c], format="%d/%m/%Y %I:%M %p", errors="coerce")

    int_columns = ["pm_id"]
    df[int_columns] = df[int_columns].astype(int)

    df["pm_status"] = df["pm_status"].map(status_mapping)

    return df


@dg.asset
def mutate_pm_history(read_raw_pm_history):
    df = read_raw_pm_history.copy()
    equipments_df = MasterData.equipments()[["equipment_name", "site_name"]]
    df = pd.merge(df, equipments_df, how="left", on="equipment_name", validate="m:1").dropna(subset=["site_name"])
    df = df.sort_values(["site_name", "equipment_name", "start_date"]).reset_index(drop=True)

    return df


@dg.asset
def materialize_pm_history(mutate_pm_history):
    result = {}

    datalake = DataLake()
    datalake_path = datalake.upload_tibble(
        container="bhp-analytics-data",
        file_path="MAINTENANCE/PM_HISTORY/pm_history.parquet",
        df=mutate_pm_history,
        format="parquet",
    )
    result["datalake"] = {"path": datalake_path, "format": "parquet"}

    msgraph = MSGraph()
    df = mutate_pm_history.copy()
    df = df.loc[df["site_name"] == "MEL"].drop(columns=["site_name"]).reset_index(drop=True)
    base_path = "https://globalkomatsu.sharepoint.com/sites/KCHCLSP00022/Shared%20Documents/01.%20%C3%81REAS%20KCH/1.6%20CONFIABILIDAD/CAEX/ANTECEDENTES/MANTENIMIENTO/PAUTAS_MANTENIMIENTO"
    "/pm_history.xlsx"
    df = df.assign(
        file_url=df["pm_id"].astype(str).str.cat(df["summary_content"], sep="_").map(lambda x: f"{base_path}/{x}.pdf")
    )
    sharepoint_result = msgraph.upload_tibble(
        site_id="KCHCLSP00022",
        file_path="/01. ÁREAS KCH/1.6 CONFIABILIDAD/CAEX/ANTECEDENTES/MANTENIMIENTO/PAUTAS_MANTENIMIENTO/pm_history.xlsx",
        df=df,
        format="excel",
    )
    result["sharepoint"] = {"file_url": sharepoint_result.web_url, "format": "excel"}

    # Add record count
    result["count"] = len(mutate_pm_history)

    return result


@dg.asset
def read_pm_history():
    dl = DataLake()
    df = dl.read_tibble("bhp-analytics-data", "MAINTENANCE/PM_HISTORY/pm_history.parquet", use_polars=False)

    return df
