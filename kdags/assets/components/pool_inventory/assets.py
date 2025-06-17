import dagster as dg
from kdags.resources.tidyr import MasterData, DataLake
import polars as pl
from kdags.config import DATA_CATALOG
from datetime import datetime


@dg.asset
def mutate_component_lifeline(
    context: dg.AssetExecutionContext,
    component_changeouts: pl.DataFrame,
    component_history: pl.DataFrame,
    mutate_reparation_history: pl.DataFrame,
    component_serials: pl.DataFrame,
):
    dl = DataLake(context)
    merge_columns = [
        "equipment_name",
        "component_name",
        "subcomponent_name",
        "position_name",
        "changeout_date",
    ]
    mounted_df = component_changeouts.select(
        [
            "equipment_name",
            "position_name",
            "changeout_date",
            "installed_component_serial",
            "installed_sap_equipment_name",
        ]
    ).rename(
        {
            "installed_component_serial": "component_serial",
            "installed_sap_equipment_name": "sap_equipment_name",
            "changeout_date": "mounted_date",
            "equipment_name": "mounted_equipment_name",
            "position_name": "mounted_position_name",
        }
    )
    df = component_serials.join(
        component_history,
        how="left",
        on=["component_name", "subcomponent_name", "component_serial", "sap_equipment_name"],
    )
    df = df.select(
        [
            *merge_columns,
            "component_serial",
            "sap_equipment_name",
            "retired_date",
            "is_retired",
            "service_order",
            "reception_date",
        ]
    ).join_asof(
        mounted_df,
        by=["component_serial", "sap_equipment_name"],
        left_on="changeout_date",
        right_on="mounted_date",
        strategy="forward",
        coalesce=True,
    )
    df = df.join(
        mutate_reparation_history.select(
            [
                "service_order",
                "component_serial",
                "repair_count",
                "repair_recency_rank",
                "cumulative_component_hours",
            ]
        ),
        how="left",
        on=["service_order", "component_serial"],
    )

    dl.upload_tibble(tibble=df, az_path=DATA_CATALOG["component_lifeline"]["analytics_path"])
    return df


@dg.asset
def component_lifeline(context: dg.AssetExecutionContext):
    dl = DataLake(context)
    df = dl.read_tibble(DATA_CATALOG["component_lifeline"]["analytics_path"])
    return df
