import polars as pl
from datetime import date
from typing import List, Set, Dict, Optional, Tuple
import dagster as dg


def summarize_parts_life(context: dg.AssetExecutionContext, df):
    return (
        df.with_columns(
            part_hours=pl.when(pl.col("cycle_event") == "ARRIVAL").then(pl.col("component_hours")).otherwise(pl.lit(0)),
            part_count=pl.when(pl.col("cycle_event") == "DEPARTURE").then(pl.lit(1)).otherwise(pl.lit(0)),
        )
        .group_by(["part_name", "subpart_name", "part_of_interest"], maintain_order=True)
        .agg(
            part_hours=pl.sum("part_hours"),
            part_count=pl.sum("part_count"),
            status=pl.first("status"),
            service_order=pl.first("service_order"),
            component_serial=pl.first("component_serial"),
            comment=pl.first("comment"),
        )
    )


def get_reparation_report(
    context: dg.AssetExecutionContext, pivot_parts_df: pl.DataFrame, target: Dict[str, any]
) -> pl.DataFrame:
    """
    PHASE 1: DEFINE THE STARTING POINT
    Filters for the specific "Final Reparation Report" which is the "after" picture of the
    component as it LEFT the workshop.
    """
    context.log.info(
        f"Fetching Final Reparation Report for Component: '{target['component_serial']}', Service Order: '{target['service_order']}', Part Name: '{target['part_name']}'"
    )
    report_df = pivot_parts_df.filter(
        (pl.col("component_serial") == target["component_serial"])
        & (pl.col("service_order") == target["service_order"])
        & (pl.col("part_name") == target["part_name"])
    )
    assert not report_df.is_empty(), f"FATAL: No data found for the specified target report: {target}."
    context.log.info(f"Found {report_df.height} part locations in the target report for the specified part name.")
    return report_df


def _find_previous_departure_event(
    context: dg.AssetExecutionContext,
    part_of_interest: str,
    part_name_of_interest: str,
    arrival_event: Dict,
    is_repair_in_place: bool,
    full_df: pl.DataFrame,
) -> Optional[Dict]:
    """
    CONCEPTUAL FUNCTION 3: Find the *Previous* "Departure"
    This is the critical step that finds the next event in the historical chain.
    """
    base_filter = (
        (pl.col("final_part_serial") == part_of_interest)
        & (pl.col("part_name") == part_name_of_interest)
        & (pl.col("reception_date") < arrival_event["reception_date"])
    )

    if is_repair_in_place:
        # If it was a repair-in-place, the part MUST have a continuous history in the SAME component.
        # The search is CONSTRAINED to the same component serial.
        context.log.info(
            f"Repair-in-place detected. CONSTRAINING search for previous departure to component '{arrival_event['component_serial']}'."
        )
        constrained_filter = base_filter & (pl.col("component_serial") == arrival_event["component_serial"])
        previous_departure_df = full_df.filter(constrained_filter).sort(by="reception_date", descending=True)

        # Data Integrity Check: If the constrained search finds nothing, but a global search WOULD have found something,
        # it means the data is contradictory.
        if previous_departure_df.is_empty():
            global_search_result = full_df.filter(base_filter)
            assert (
                global_search_result.is_empty()
            ), f"FATAL Data Error: Part '{part_of_interest}' was repaired-in-place in component '{arrival_event['component_serial']}', but its prior history appears in a different component. This is a logical contradiction."
    else:
        # If it was a swap, we perform a GLOBAL search to find where the part came from.
        context.log.info("Part swap detected. Performing GLOBAL search for previous departure.")
        previous_departure_df = full_df.filter(base_filter).sort(by="reception_date", descending=True)

    if previous_departure_df.is_empty():
        return None

    return previous_departure_df.row(0, named=True)


def trace_part_lifecycle(context: dg.AssetExecutionContext, starting_event: Dict, full_df: pl.DataFrame) -> List[Dict]:
    """
    PHASE 2: THE DEFINITIVE TRACING ALGORITHM
    Traces the complete life story of a single part backwards in time by correctly
    chaining its life cycles. A life cycle consists of an "Arrival" (initial state)
    and a "Departure" (final state).
    """
    part_of_interest = starting_event.get("final_part_serial")
    part_name_of_interest = starting_event.get("part_name")
    subpart_name = starting_event.get("subpart_name")

    context.log.info(
        f"--- Starting Full Trace for Part: [{part_of_interest}] (Type: {part_name_of_interest}) from Location: [{subpart_name}] ---"
    )

    lifecycle = []
    current_departure_event = starting_event
    recency_rank = 0

    while True:
        # --- STEP 1: Find and Analyze the Current Life Cycle ---
        # CONCEPTUAL FUNCTION 1: Find the "Arrival" for the Current "Departure"
        arrival_event_df = full_df.filter(
            (pl.col("component_serial") == current_departure_event["component_serial"])
            & (pl.col("service_order") == current_departure_event["service_order"])
            & (pl.col("part_name") == part_name_of_interest)
            & (pl.col("subpart_name") == current_departure_event["subpart_name"])
        )
        assert (
            not arrival_event_df.is_empty()
        ), f"FATAL Data Error: Could not find a corresponding arrival record for the departure of part '{part_of_interest}' in SO '{current_departure_event['service_order']}'."

        arrival_event = arrival_event_df.row(0, named=True)

        # The Retrofit Rule: Check if the arrival event is a hard stop.
        if arrival_event.get("part_name") == "low_speed_gear" and arrival_event.get("retrofit") is True:
            context.log.info(
                f"RETROFIT detected for part '{part_of_interest}' at SO: {arrival_event.get('service_order')}. Trace will stop here."
            )
            arrival_record = {
                **arrival_event,
                "recency_repair_rank": recency_rank,
                "cycle_event": "DEPARTURE",
                "status": "RETROFIT",
                "comment": "RETROFIT applied. Part is considered new; history trace ends here.",
            }
            lifecycle.append(arrival_record)
            break

        # CONCEPTUAL FUNCTION 2: Analyze the Life Cycle (Repair or Swap?)
        initial_part_in_cycle = arrival_event.get("initial_part_serial")
        is_repair_in_place = initial_part_in_cycle == part_of_interest

        # --- Record the events for this life cycle ---
        departure_record = {**current_departure_event, "recency_repair_rank": recency_rank, "cycle_event": "DEPARTURE"}
        arrival_record = {**arrival_event, "recency_repair_rank": recency_rank, "cycle_event": "ARRIVAL"}

        if is_repair_in_place:
            status = "REPAIRED_IN_PLACE"
            comment = f"Part arrived and departed in the same component."
        else:
            status = "PART_SWAP"
            comment = f"Part was installed, replacing '{initial_part_in_cycle}'."

        departure_record["status"], arrival_record["status"] = status, status
        departure_record["comment"], arrival_record["comment"] = comment, comment

        lifecycle.extend([departure_record, arrival_record])

        # --- STEP 2: Find the *Previous* Life Cycle ---
        previous_departure_event = _find_previous_departure_event(
            context, part_of_interest, part_name_of_interest, arrival_event, is_repair_in_place, full_df
        )

        if not previous_departure_event:
            context.log.info(f"Trace for part '{part_of_interest}' ends. No earlier departure event found.")
            lifecycle[-1]["comment"] += " This is the first known life cycle for this part (birth)."
            break

        current_departure_event = previous_departure_event
        recency_rank += 1

    return lifecycle


def trace_all_parts_from_report(
    context: dg.AssetExecutionContext, report_df: pl.DataFrame, full_df: pl.DataFrame
) -> List[Dict]:
    """
    Initiates a lifecycle trace for each part found in the final reparation report.
    This function orchestrates the tracing process.
    """
    all_events = []
    processed_parts: Set[Tuple[str, str]] = set()

    for row in report_df.iter_rows(named=True):
        part_to_trace = row.get("final_part_serial")
        part_name_to_trace = row.get("part_name")
        part_key = (part_to_trace, part_name_to_trace)

        if not part_to_trace:
            # Logic to handle missing final part can be added here if needed.
            continue

        if part_name_to_trace and part_key not in processed_parts:
            part_history = trace_part_lifecycle(context, row, full_df)
            for event in part_history:
                event["part_of_interest"] = part_to_trace
            all_events.extend(part_history)
            processed_parts.add(part_key)
    return all_events


def format_results_to_dataframe(events_list: List[Dict]) -> pl.DataFrame:
    """
    Converts the final list of event dictionaries into a structured, clean Polars DataFrame
    for the final output.
    """
    if not events_list:
        return pl.DataFrame()

    df = pl.DataFrame(events_list)
    required_cols = [
        "part_of_interest",
        "component_serial",
        "service_order",
        "reception_date",
        "part_name",
        "subpart_name",
        "initial_part_serial",
        "final_part_serial",
        "component_hours",
        "recency_repair_rank",
        "cycle_event",
        "status",
        "comment",
    ]
    for col in required_cols:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))
    return df.select(required_cols)


if __name__ == "__main__":
    # --- Main Execution Block with Example Data ---
    # This block demonstrates how to use the functions with sample data.
    # In your real code, you would replace this with your data loading logic.

    # --- Mock Dagster Context for Logging ---
    # In a real Dagster environment, this object is provided automatically.
    class MockDagsterContext:
        class MockLog:
            def info(self, msg):
                print(f"INFO: {msg}")

            def warning(self, msg):
                print(f"WARNING: {msg}")

            def error(self, msg):
                print(f"ERROR: {msg}")

        log = MockLog()

    context = MockDagsterContext()
    # Create a sample DataFrame that covers all the logical cases we've discussed.
    pivot_parts = pl.DataFrame(
        [
            # --- History for Driveshaft P101 ---
            {
                "component_serial": "MOTOR_A",
                "service_order": "SO_4",
                "reception_date": date(2025, 7, 18),
                "part_name": "driveshaft",
                "subpart_name": "driveshaft",
                "initial_part_serial": "P202",
                "final_part_serial": "P101",
                "component_hours": 5000.0,
                "retrofit": False,
            },
            {
                "component_serial": "MOTOR_B",
                "service_order": "SO_3",
                "reception_date": date(2024, 1, 10),
                "part_name": "driveshaft",
                "subpart_name": "driveshaft",
                "initial_part_serial": "P101",
                "final_part_serial": "P303",
                "component_hours": 4000.0,
                "retrofit": False,
            },
            {
                "component_serial": "MOTOR_B",
                "service_order": "SO_2",
                "reception_date": date(2023, 5, 20),
                "part_name": "driveshaft",
                "subpart_name": "driveshaft",
                "initial_part_serial": "P101",
                "final_part_serial": "P101",
                "component_hours": 3000.0,
                "retrofit": False,
            },
            {
                "component_serial": "MOTOR_C",
                "service_order": "SO_1",
                "reception_date": date(2022, 1, 15),
                "part_name": "driveshaft",
                "subpart_name": "driveshaft",
                "initial_part_serial": "P101",
                "final_part_serial": "P404",
                "component_hours": 2000.0,
                "retrofit": False,
            },
            # --- History for Low Speed Gear P808 with a Retrofit ---
            {
                "component_serial": "MOTOR_G",
                "service_order": "SO_10",
                "reception_date": date(2025, 6, 1),
                "part_name": "low_speed_gear",
                "subpart_name": "low_speed_gear_3hr",
                "initial_part_serial": None,
                "final_part_serial": "P808",
                "component_hours": 10000.0,
                "retrofit": False,
            },
            {
                "component_serial": "MOTOR_H",
                "service_order": "SO_9",
                "reception_date": date(2024, 8, 5),
                "part_name": "low_speed_gear",
                "subpart_name": "low_speed_gear_6hr",
                "initial_part_serial": "P808",
                "final_part_serial": "P909",
                "component_hours": 9000.0,
                "retrofit": True,
            },  # RETROFIT EVENT
            {
                "component_serial": "MOTOR_I",
                "service_order": "SO_8",
                "reception_date": date(2023, 1, 1),
                "part_name": "low_speed_gear",
                "subpart_name": "low_speed_gear_9hr",
                "initial_part_serial": "P808",
                "final_part_serial": "P808",
                "component_hours": 8500.0,
                "retrofit": False,
            },  # This event should NOT be found.
            # --- SCENARIOS FOR INCOMPLETE DATA ---
            {
                "component_serial": "MOTOR_D",
                "service_order": "SO_5",
                "reception_date": date(2023, 8, 1),
                "part_name": "sun_pinion",
                "subpart_name": "sun_pinion",
                "initial_part_serial": "P505",
                "final_part_serial": None,
                "component_hours": 6000.0,
                "retrofit": False,
            },
            {
                "component_serial": "MOTOR_E",
                "service_order": "SO_6",
                "reception_date": date(2024, 3, 3),
                "part_name": "ring_gear",
                "subpart_name": "ring_gear",
                "initial_part_serial": None,
                "final_part_serial": "P606",
                "component_hours": 7000.0,
                "retrofit": False,
            },
            {
                "component_serial": "MOTOR_F",
                "service_order": "SO_7",
                "reception_date": date(2023, 11, 22),
                "part_name": "ring_gear",
                "subpart_name": "ring_gear",
                "initial_part_serial": "P606",
                "final_part_serial": "P707",
                "component_hours": 8000.0,
                "retrofit": False,
            },
        ]
    )

    # --- Define and run the desired scenario ---

    # The primary, complete scenario
    target_report_def = {"component_serial": "MOTOR_A", "service_order": "SO_4"}

    # To test Scenario A (missing final part), uncomment the line below:
    # target_report_def = {"component_serial": "MOTOR_D", "service_order": "SO_5"}

    # To test Scenario B (missing initial part, with history), uncomment the line below:
    target_report_def = {"component_serial": "MOTOR_E", "service_order": "SO_6"}

    # To test the new Retrofit rule, uncomment the line below:
    # target_report_def = {"component_serial": "MOTOR_G", "service_order": "SO_10"}

    # Run the full process
    final_reparation_report = get_reparation_report(context, pivot_parts, target_report_def)
    all_lifecycle_events = trace_all_parts_from_report(context, final_reparation_report, pivot_parts)
    final_results_df = format_results_to_dataframe(all_lifecycle_events)

    # Print the final, structured output.
    context.log.info("--- FINAL TRACE RESULTS ---")
    context.log.info(final_results_df)
    final_results_df.to_dicts()
