import dagster as dg
from kdags.config import DATA_CATALOG

# --- Relative module imports
from kdags.resources.tidyr import DataLake
import polars as pl
from .extract_utils import *


@dg.asset(group_name="reparation", compute_kind="mutate")
def mutate_reso_cms(context: dg.AssetExecutionContext, component_reparations: pl.DataFrame, so_documents: pl.DataFrame):
    dl = DataLake(context)
    records_list = (
        component_reparations.join(
            so_documents.filter(pl.col("file_type") == "preliminary_report")
            # .select(["service_order", "component_serial", "az_path"])
            .unique(subset=["service_order", "component_serial"]),
            how="left",
            on=["service_order", "component_serial"],
        )
        .drop_nulls("file_size")
        .filter(pl.col("subcomponent_tag") == "5A30")
        .sort("reception_date")
        .tail(5)
        .select(["service_order", "component_serial", "az_path"])
        .to_dicts()
    )

    df = process_multiple_reports_expanded(context, records_list)

    # dl.upload_tibble(tibble=df, az_path=DATA_CATALOG["so_documents"]["analytics_path"])
    return df
