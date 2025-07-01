import dagster as dg
import polars as pl
from kdags.resources.tidyr import DataLake, MSGraph, MasterData
from kdags.config import DATA_CATALOG, TIDY_NAMES, tidy_tibble


@dg.asset(compute_kind="publish")
def publish_component_history(
    context: dg.AssetExecutionContext, component_history: pl.DataFrame, so_quotations: pl.DataFrame, icc: pl.DataFrame
):
    msgraph = MSGraph(context)

    df = (
        component_history.join(
            MasterData.equipments().select(["site_name", "equipment_model", "equipment_name"]),
            how="left",
            on="equipment_name",
        )
        .join(
            so_quotations.select(
                [
                    "service_order",
                    "component_serial",
                    "purchase_order",
                    "quotation_dt",
                    "edited_by",
                    "repair_cost",
                    "quotation_remarks",
                ]
            ),
            how="left",
            on=["service_order", "component_serial"],
        )
        .sort("changeout_date")
    )

    merge_columns = ["equipment_name", "component_name", "position_name", "changeout_date"]
    df = df.join(
        icc.select([*merge_columns, "icc_file_name", "icc_url"]),
        how="left",
        on=merge_columns,
    )

    df = df.rename(TIDY_NAMES, strict=False).pipe(tidy_tibble, context)
    msgraph.upload_tibble(
        tibble=df,
        sp_path=DATA_CATALOG["component_history"]["publish_path"],
    )
    return df
