from kdags.resources.tidyr import DataLake
import polars as pl
import dagster as dg


@dg.asset
def read_haul():
    dl = DataLake()
    uri = "abfs://bhp-analytics-data/OPERATION/PLM3/haul.parquet"
    if dl.az_path_exists(uri):
        return dl.read_tibble(az_path=uri)
    else:
        return pl.DataFrame()
