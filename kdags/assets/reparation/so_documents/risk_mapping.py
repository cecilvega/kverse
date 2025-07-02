import polars as pl
import dagster as dg
from kdags.config import DATA_CATALOG
from kdags.resources.tidyr import DataLake, MasterData
import re
from collections import defaultdict, deque
from typing import Dict, List, Optional, Tuple, Set


def aggregate_part_lifecycle_analysis(
    df: pl.DataFrame,
    service_costs_df: pl.DataFrame = None,
    overhaul_intervals: Dict[str, float] = None,
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Aggregate part lifecycle data to part_name level and create fleet status.
    """

    # Step 1: Get currently mounted parts (recency_rank = 0)
    current_parts = df.filter((pl.col("part_repair_recency_rank") == 0) & (pl.col("part_serial").is_not_null()))

    # Step 2: Aggregate to part_name level per component
    part_level = current_parts.group_by(["component_serial", "subcomponent_tag", "part_name"]).agg(
        [
            # Mean lifecycle hours across all subparts
            pl.col("total_lifecycle_hours").mean().alias("mean_part_hours"),
            pl.col("total_lifecycle_hours").min().alias("min_part_hours"),
            pl.col("total_lifecycle_hours").max().alias("max_part_hours"),
            # Repair statistics - AVERAGE across subparts
            pl.col("total_repairs").mean().alias("mean_repair_count"),
            # Component hours at last repair
            pl.col("component_hours").max().alias("component_hours_at_repair"),
            # Count of subparts
            pl.col("subpart_name").n_unique().alias("num_subparts"),
        ]
    )

    # Step 3: Create fleet-wide statistics
    fleet_status = part_level.group_by(["subcomponent_tag", "part_name"]).agg(
        [
            # Fleet-wide hour statistics
            pl.col("mean_part_hours").mean().alias("fleet_mean_hours"),
            pl.col("mean_part_hours").std().alias("fleet_std_hours"),
            pl.col("mean_part_hours").quantile(0.25).alias("fleet_p25_hours"),
            pl.col("mean_part_hours").quantile(0.50).alias("fleet_p50_hours"),
            pl.col("mean_part_hours").quantile(0.75).alias("fleet_p75_hours"),
            # Repair count - AVERAGE across all components
            pl.col("mean_repair_count").mean().alias("fleet_mean_repairs"),
            # Component count - how many components have this part
            pl.col("component_serial").n_unique().alias("num_components_with_part"),
        ]
    )

    # Step 4: Calculate sensible control limits based on statistics
    fleet_status = fleet_status.with_columns(
        [
            # Lower bound: Use 25th percentile or mean - 1.5*std, whichever is higher
            pl.max_horizontal(
                [
                    pl.col("fleet_p25_hours"),
                    (pl.col("fleet_mean_hours") - 1.5 * pl.col("fleet_std_hours")),
                    pl.lit(10000),  # Minimum threshold
                ]
            ).alias("lower_bound_hours"),
            # Upper bound: Use 75th percentile or mean + 1.5*std, whichever is lower
            pl.min_horizontal(
                [
                    pl.col("fleet_p75_hours") * 1.2,  # 20% above 75th percentile
                    (pl.col("fleet_mean_hours") + 1.5 * pl.col("fleet_std_hours")),
                    pl.col("fleet_mean_hours") * 1.8,  # Max 80% above mean
                ]
            ).alias("upper_bound_hours"),
        ]
    )

    # If we have cost data, adjust bounds based on failure patterns
    if service_costs_df is not None:
        fleet_status = adjust_bounds_by_cost_patterns(fleet_status, service_costs_df)

    # Flag at-risk parts in part_level data
    part_level = part_level.join(
        fleet_status.select(
            [
                "subcomponent_tag",
                "part_name",
                "lower_bound_hours",
                "upper_bound_hours",
                "fleet_mean_hours",
            ]
        ),
        on=["subcomponent_tag", "part_name"],
        how="left",
    ).with_columns(
        [
            pl.when(pl.col("mean_part_hours") < pl.col("lower_bound_hours"))
            .then(pl.lit("HIGH_RISK_LOW_HOURS"))  # Part failing too early
            .when(pl.col("mean_part_hours") > pl.col("upper_bound_hours"))
            .then(pl.lit("HIGH_RISK_HIGH_HOURS"))  # Part overdue for replacement
            .otherwise(pl.lit("NORMAL"))
            .alias("risk_status")
        ]
    )

    return part_level, fleet_status


def adjust_bounds_by_cost_patterns(fleet_df: pl.DataFrame, costs_df: pl.DataFrame) -> pl.DataFrame:
    """
    Adjust control bounds based on cost patterns indicating premature failures.
    """
    # Calculate high-cost repairs by subcomponent
    cost_analysis = costs_df.group_by("subcomponent_tag").agg(
        [
            pl.col("repair_cost").mean().alias("avg_repair_cost"),
            pl.col("repair_cost").quantile(0.9).alias("high_cost_threshold"),
        ]
    )

    # Define what constitutes high cost by subcomponent type
    cost_thresholds = {
        "0980": 200000,  # Traction motor
        "5A30": 70000,  # Other component
    }

    return fleet_df.join(cost_analysis, on="subcomponent_tag", how="left").with_columns(
        [
            # If average costs are high, reduce expected hours (parts failing early)
            pl.when((pl.col("subcomponent_tag") == "0980") & (pl.col("avg_repair_cost") > 180000))
            .then(pl.col("lower_bound_hours") * 0.8)
            .when((pl.col("subcomponent_tag") == "5A30") & (pl.col("avg_repair_cost") > 60000))
            .then(pl.col("lower_bound_hours") * 0.85)
            .otherwise(pl.col("lower_bound_hours"))
            .alias("lower_bound_hours"),
            # Keep upper bound as is
            pl.col("upper_bound_hours"),
        ]
    )


# Create a summary view function
def create_fleet_summary_view(fleet_status_df: pl.DataFrame) -> pl.DataFrame:
    """Create a clean summary view with key metrics."""
    return fleet_status_df.select(
        [
            "subcomponent_tag",
            "part_name",
            pl.col("fleet_mean_hours").round(0).alias("avg_hours"),
            pl.col("lower_bound_hours").round(0).alias("lower_bound"),
            pl.col("upper_bound_hours").round(0).alias("upper_bound"),
            pl.col("fleet_mean_repairs").round(1).alias("avg_repairs"),
            pl.col("num_components_with_part").alias("fleet_size"),
            # Add status indicator
            pl.lit("OK").alias("fleet_health"),  # You can make this dynamic
        ]
    )


@dg.asset(compute_kind="aggregate")
def fleet_risk_analysis(
    context: dg.AssetExecutionContext,
    mutate_part_reparations: pl.DataFrame,
    so_quotations: pl.DataFrame,  # Another asset with cost data
    component_reparations: pl.DataFrame,
) -> Dict[str, pl.DataFrame]:
    """Analyze fleet-wide part risks."""

    quotations = (
        so_quotations.select(["service_order", "component_serial", "repair_cost"])
        .join(
            component_reparations.select(["service_order", "component_serial", "subcomponent_tag"]),
            how="left",
            on=["service_order", "component_serial"],
            validate="1:1",
        )
        .drop_nulls()
    )
    overhaul_intervals = {
        "0980": 25000,
        "05A30": 20000,
    }

    part_level, fleet_status = aggregate_part_lifecycle_analysis(
        mutate_part_reparations, quotations, overhaul_intervals
    )

    # Log summary
    at_risk_count = part_level.filter(pl.col("risk_status") == "HIGH_RISK").height
    context.log.info(f"Found {at_risk_count} at-risk part installations")

    return {"part_level": part_level, "fleet_status": fleet_status}
