import polars as pl
import dagster as dg
from kdags.config import DATA_CATALOG
from kdags.resources.tidyr import DataLake, MasterData
import re
from collections import defaultdict, deque
from typing import Dict, List, Optional, Tuple, Set


def clean_serial_data(df: pl.DataFrame) -> pl.DataFrame:
    """Clean serial number data by converting invalid values to None."""
    invalid_serials = ["no", "NO", "0000", "0", "", " ", "None", "none"]

    return df.with_columns(
        [
            pl.when(pl.col("initial_part_serial").is_in(invalid_serials))
            .then(None)
            .otherwise(pl.col("initial_part_serial"))
            .alias("initial_part_serial"),
            pl.when(pl.col("final_part_serial").is_in(invalid_serials))
            .then(None)
            .otherwise(pl.col("final_part_serial"))
            .alias("final_part_serial"),
        ]
    )


def build_part_movement_graph(df: pl.DataFrame, component_hours_col: str = "component_hours") -> Dict:
    """
    Build a graph of all part movements across components.

    Returns:
        Dict with structure:
        {
            part_serial: {
                'movements': [(from_component, to_component, date, hours, service_order), ...],
                'appearances': [(component, date, hours, is_initial, is_final, service_order), ...]
            }
        }
    """
    part_graph = defaultdict(lambda: {"movements": [], "appearances": []})

    # Clean data first
    df_clean = clean_serial_data(df)

    # Record all appearances of each part
    for row in df_clean.iter_rows(named=True):
        initial = row["initial_part_serial"]
        final = row["final_part_serial"]
        component = row["component_serial"]
        date = row["reception_date"]
        hours = row.get(component_hours_col, 0.0) or 0.0
        so = row["service_order"]

        if initial:
            part_graph[initial]["appearances"].append(
                {
                    "component": component,
                    "date": date,
                    "hours": hours,
                    "is_initial": True,
                    "is_final": False,
                    "service_order": so,
                    "part_name": row["part_name"],
                    "subpart_name": row["subpart_name"],
                }
            )

        if final:
            part_graph[final]["appearances"].append(
                {
                    "component": component,
                    "date": date,
                    "hours": 0.0,  # Final doesn't carry hours
                    "is_initial": False,
                    "is_final": True,
                    "service_order": so,
                    "part_name": row["part_name"],
                    "subpart_name": row["subpart_name"],
                }
            )

    # Build movements by analyzing where parts went
    for part_serial, data in part_graph.items():
        appearances = sorted(data["appearances"], key=lambda x: x["date"])

        for i in range(len(appearances)):
            app = appearances[i]

            # If this is an initial appearance, find where it came from
            if app["is_initial"] and i > 0:
                # Look backwards for a removal (is_initial but not is_final in same SO)
                for j in range(i - 1, -1, -1):
                    prev_app = appearances[j]
                    # Find where it was removed from
                    if prev_app["is_initial"] and prev_app["component"] != app["component"]:
                        # Check if there's a removal record
                        removal_date = None
                        for k in range(j, i):
                            check_app = appearances[k]
                            if (
                                check_app["component"] == prev_app["component"]
                                and not check_app["is_final"]
                                and check_app["is_initial"]
                            ):
                                removal_date = check_app["date"]
                                break

                        if removal_date and removal_date < app["date"]:
                            data["movements"].append(
                                {
                                    "from_component": prev_app["component"],
                                    "to_component": app["component"],
                                    "movement_date": app["date"],
                                    "hours_at_origin": prev_app["hours"],
                                    "from_service_order": prev_app["service_order"],
                                    "to_service_order": app["service_order"],
                                }
                            )
                            break

    return dict(part_graph)


def trace_part_lifecycle(
    part_serial: str,
    part_graph: Dict,
    current_component: str,
    current_date,
    max_depth: int = 100,
) -> Tuple[float, int, List[Dict]]:
    """
    Recursively trace a part's complete lifecycle to accumulate total hours.

    Returns:
        Tuple of (total_hours, repair_count, movement_history)
    """
    if part_serial not in part_graph:
        return 0.0, 0, []

    visited = set()
    total_hours = 0.0
    repair_count = 0
    movement_history = []

    # BFS to trace all historical positions
    queue = deque([(current_component, current_date, 0)])

    while queue and len(visited) < max_depth:
        component, date, depth = queue.popleft()

        if (component, date) in visited:
            continue
        visited.add((component, date))

        # Find all appearances of this part in this component before current date
        appearances = [
            app
            for app in part_graph[part_serial]["appearances"]
            if app["component"] == component and app["date"] <= date
        ]

        for app in appearances:
            if app["is_initial"] and app["hours"] > 0:
                total_hours += app["hours"]
                repair_count += 1
                movement_history.append(
                    {
                        "component": component,
                        "date": app["date"],
                        "hours": app["hours"],
                        "service_order": app["service_order"],
                        "depth": depth,
                    }
                )

        # Find movements TO this component before current date
        movements_to = [
            m
            for m in part_graph[part_serial]["movements"]
            if m["to_component"] == component and m["movement_date"] <= date
        ]

        for movement in movements_to:
            # Add the source component to queue for further tracing
            queue.append((movement["from_component"], movement["movement_date"], depth + 1))

    return total_hours, repair_count, movement_history


@dg.asset(compute_kind="mutate")
def part_repairs_lifecycle(context: dg.AssetExecutionContext, pivot_parts: pl.DataFrame) -> pl.DataFrame:
    """
    Enhanced tracking that accumulates hours across a part's entire lifecycle.
    """
    # Build the part movement graph
    component_hours_col: str = "component_hours"
    df = pivot_parts.clone()
    context.log.info("Building part movement graph...")
    part_graph = build_part_movement_graph(df, component_hours_col)

    # Clean and sort data
    df_clean = clean_serial_data(df)
    df_sorted = df_clean.sort(
        [
            "component_serial",
            "subcomponent_tag",
            "part_name",
            "subpart_name",
            "reception_date",
        ],
        descending=[False, False, False, False, True],
    )

    # Process each row and trace complete lifecycle
    results = []

    for row in df_sorted.iter_rows(named=True):
        initial = row["initial_part_serial"]
        final = row["final_part_serial"]

        # Initialize tracking variables
        lifecycle_hours = 0.0
        lifecycle_repairs = 0
        movement_history = []
        repair_status = ""

        # Determine which serial to trace (prefer final if exists)
        trace_serial = final if final else initial

        if trace_serial:
            # Trace the complete lifecycle of this part
            lifecycle_hours, lifecycle_repairs, movement_history = trace_part_lifecycle(
                trace_serial, part_graph, row["component_serial"], row["reception_date"]
            )

            # Determine repair status
            if initial and final:
                if initial == final:
                    repair_status = "REPAIRED_SAME_PART"
                else:
                    repair_status = "PART_SWAPPED"
            elif initial and not final:
                repair_status = "UNKNOWN_FINAL_PART"
            elif not initial and final:
                repair_status = "UNKNOWN_INITIAL_PART"
        else:
            repair_status = "NO_SERIAL_DATA"

        # Create result row
        result_row = row.copy()
        result_row["part_lifecycle_hours"] = lifecycle_hours
        result_row["part_lifecycle_repairs"] = lifecycle_repairs
        result_row["part_repair_status"] = repair_status
        result_row["part_movement_count"] = len(movement_history)
        result_row["part_movement_history"] = str(movement_history)  # Convert to string for DataFrame

        results.append(result_row)

    return pl.DataFrame(results)


@dg.asset(compute_kind="mutate")
def mutate_part_reparations(context: dg.AssetExecutionContext, part_repairs_lifecycle: pl.DataFrame) -> pl.DataFrame:
    """
    Create a comprehensive summary for all part serials with recency ranking.

    Adds:
    - part_repair_recency_rank: 0 = currently mounted (most recent), 1 = previous repair, etc.
    - part_serial: unified column (uses final_part_serial if exists, otherwise initial_part_serial)
    - Summary statistics for each part's lifecycle
    """
    dl = DataLake(context)
    # First, add the recency rank within each component/part combination
    df = part_repairs_lifecycle.clone()
    df_with_rank = df.with_columns(
        [
            # Create unified part_serial column (prefer final over initial)
            pl.when(pl.col("final_part_serial").is_not_null())
            .then(pl.col("final_part_serial"))
            .otherwise(pl.col("initial_part_serial"))
            .alias("part_serial"),
        ]
    ).with_columns(
        [
            # Rank by recency (0 = most recent)
            pl.col("reception_date")
            .rank(method="dense", descending=True)
            .over(["component_serial", "subcomponent_tag", "part_name", "subpart_name"])
            .sub(1)  # Make it 0-based
            .alias("part_repair_recency_rank")
        ]
    )

    # Now create comprehensive summary with lifecycle stats
    # Group by part_serial to get full history
    part_history = (
        df_with_rank.group_by("part_serial")
        .agg(
            [
                # Component history
                pl.col("component_serial").unique().alias("components_visited"),
                pl.col("component_serial").n_unique().alias("num_components_visited"),
                # Date range
                # Lifecycle stats
                pl.col("part_lifecycle_hours").max().alias("total_lifecycle_hours"),
                pl.col("part_lifecycle_repairs").max().alias("total_repairs"),
                pl.col("part_movement_count").max().alias("total_movements"),
                # Part identification
                pl.col("part_name").first().alias("part_name"),
                pl.col("subpart_name").first().alias("subpart_name"),
            ]
        )
        .filter(pl.col("part_serial").is_not_null())
    )

    # Join back with the ranked data to include all details
    comprehensive_summary = df_with_rank.join(
        part_history,
        on="part_serial",
        how="left",
    ).sort(["part_serial", "component_serial", "part_repair_recency_rank"])

    return comprehensive_summary
