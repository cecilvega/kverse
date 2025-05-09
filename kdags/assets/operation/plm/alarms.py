from kdags.resources.tidyr import DataLake
import polars as pl
import dagster as dg


@dg.asset
def read_alarms():
    dl = DataLake()
    uri = "az://bhp-analytics-data/OPERATION/PLM3/alarms.parquet"
    if dl.az_path_exists(uri):
        return dl.read_tibble(az_path=uri)
    else:
        return pl.DataFrame()
