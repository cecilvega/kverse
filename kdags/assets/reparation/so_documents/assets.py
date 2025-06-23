import dagster as dg
from kdags.config import DATA_CATALOG

# --- Relative module imports
from kdags.resources.tidyr import DataLake
import polars as pl


@dg.asset(group_name="reparation")
def raw_so_documents(context: dg.AssetExecutionContext):
    dl = DataLake(context)
    # try:
    df = dl.read_tibble(DATA_CATALOG["so_documents"]["raw_path"])
    # except Exception as e:
    #     df = pl.DataFrame(schema=DOCUMENTS_LIST_SCHEMA)

    return df
