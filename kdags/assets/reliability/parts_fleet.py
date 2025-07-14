import dagster as dg
import polars as pl

from kdags.config import DATA_CATALOG, TIDY_NAMES, tidy_tibble
from kdags.resources.tidyr import DataLake, MasterData, MSGraph


def add_status_column(
    df: pl.DataFrame,
    value_column: str = "total_part_hours",
    condemnatory_column: str = "part_lc_hours",
    precautionary_column: str = "part_lp_hours",
    status_column: str = "part_status",
) -> pl.DataFrame:
    """
    Add a status column to a Polars DataFrame based on condemnatory limits.

    Parameters:
    -----------
    df : pl.DataFrame
        Input DataFrame
    value_column : str
        Column name containing the values to analyze (default: "total_part_hours")
    condemnatory_column : str
        Column name containing the condemnatory limits (default: "part_lc_hours")
    precautionary_column : str
        Column name containing the precautionary limits (default: "part_lp_hours")
    status_column : str
        Name for the new status column (default: "status")

    Returns:
    --------
    pl.DataFrame
        DataFrame with added status column
    """

    return df.with_columns(
        [
            pl.when(pl.col(value_column) >= pl.col(condemnatory_column))
            .then(pl.lit("CRITICO"))
            .when(pl.col(value_column) >= pl.col(precautionary_column))
            .then(pl.lit("PRECAUTORIO"))
            .otherwise(pl.lit("NORMAL"))
            .alias(status_column)
        ]
    )


@dg.asset
def mutate_parts_fleet(context: dg.AssetExecutionContext, mutate_component_fleet, parts_base, mutate_part_reparations):
    dl = DataLake(context)
    df = (
        mutate_part_reparations.filter(pl.col("part_repair_recency_rank") == 0)
        .drop_nulls(["part_lifecycle_hours"])
        .filter(pl.col("part_lifecycle_hours") != 0)
        .drop(
            [
                "final_part_serial",
                "initial_part_serial",
                "num_components_visited",
                "part_name_right",
                "subpart_name_right",
                "part_repair_recency_rank",
            ]
        )
    )
    # Unirlo a la base para cuantificar nulos

    merge_columns = ["subcomponent_tag", "component_serial", "service_order"]
    partial_cr_df = mutate_component_fleet.select(
        [
            *merge_columns,
            "equipment_name",
            "total_component_hours",
            "repair_count",
            "runtime_hours",
        ]
    ).join(
        parts_base.select(["subcomponent_tag", "part_name", "subpart_name"])
        .unique()
        .sort(["subcomponent_tag", "part_name", "subpart_name"]),
        on="subcomponent_tag",
        how="full",
        coalesce=True,
    )
    df = (
        partial_cr_df.join(
            df,
            on=[*merge_columns, "part_name", "subpart_name"],
            how="left",
        )
        .drop(["part_lifecycle_hours", "part_lifecycle_repairs", "total_movements"])
        .rename(
            {
                "total_lifecycle_hours": "part_lifecycle_hours",
                "total_repairs": "part_repairs",
            }
        )
    )

    df = df.drop(
        [
            "component_hours",
            "reception_date",
            "part_movement_history",
            "repair_count",
            "part_movement_count",
        ]
    )
    df = df.with_columns(total_part_hours=pl.col("runtime_hours") + pl.col("part_lifecycle_hours")).with_columns(
        censoring=pl.when((pl.col("total_part_hours").is_null()) | (pl.col("total_part_hours") > 80000))
        .then(pl.lit(True))
        .otherwise(pl.lit(False))
    )
    df = df.join(
        pl.DataFrame(
            [
                {
                    "part_name": "low_speed_gear",
                    "part_lp_hours": 21785,
                    "part_lc_hours": 47576,
                },
                {
                    "part_name": "sun_pinion",
                    "part_lp_hours": 20000,
                    "part_lc_hours": 48214,
                },
                {
                    "part_name": "coupling_plate",
                    "part_lp_hours": 24514,
                    "part_lc_hours": 50867,
                },
                {
                    "part_name": "driveshaft",
                    "part_lp_hours": 20000,
                    "part_lc_hours": 45723,
                },
                {
                    "part_name": "ring_gear",
                    "part_lp_hours": 20000,
                    "part_lc_hours": 52000,
                },
                {
                    "part_name": "high_speed_gear",
                    "part_lp_hours": 20000,
                    "part_lc_hours": 51333,
                },
            ]
        ),
        how="left",
        on=["part_name"],
    )

    df = df.pipe(add_status_column)

    dl.upload_tibble(df, DATA_CATALOG["parts_fleet"]["analytics_path"])
    return df


@dg.asset(compute_kind="publish")
def publish_component_fleet(context: dg.AssetExecutionContext, mutate_component_fleet: pl.DataFrame):
    msgraph = MSGraph(context)

    df = mutate_component_fleet.clone()
    df = df.rename(TIDY_NAMES, strict=False).pipe(tidy_tibble, context)
    msgraph.upload_tibble(
        tibble=df,
        sp_path=DATA_CATALOG["component_fleet"]["publish_path"],
    )
    return df


@dg.asset(compute_kind="publish")
def publish_parts_fleet(context: dg.AssetExecutionContext, mutate_parts_fleet: pl.DataFrame):
    msgraph = MSGraph(context)

    df = mutate_parts_fleet.clone()
    df = df.rename(TIDY_NAMES, strict=False).pipe(tidy_tibble, context)
    msgraph.upload_tibble(
        tibble=df,
        sp_path=DATA_CATALOG["parts_fleet"]["publish_path"],
    )
    return df
