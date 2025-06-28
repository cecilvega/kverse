import dagster as dg
from kdags.config import DATA_CATALOG

# --- Relative module imports
from kdags.resources.tidyr import DataLake
import polars as pl
from ..extract_utils import *
from ..extra_utils import get_documents


@dg.asset(group_name="reparation", compute_kind="mutate")
def mutate_sd_parts(context: dg.AssetExecutionContext, component_reparations: pl.DataFrame, so_documents: pl.DataFrame):
    dl = DataLake(context)
    preliminary_reports = get_documents(
        component_reparations=component_reparations,
        so_documents=so_documents,
        subcomponent_tag="5A30",
        file_type="preliminary_report",
    )
    # final_reports

    df = process_multiple_reports_expanded(context, preliminary_reports)

    # dl.upload_tibble(tibble=df, az_path=DATA_CATALOG["so_documents"]["analytics_path"])
    return df
