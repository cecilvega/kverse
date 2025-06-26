import dagster as dg
import polars as pl

from kdags.config import DATA_CATALOG
from kdags.resources.tidyr import DataLake


@dg.asset(group_name="components")
def mutate_mounted_components(): ...
