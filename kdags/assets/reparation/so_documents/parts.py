import polars as pl
import dagster as dg
from kdags.config import DATA_CATALOG
from kdags.resources.tidyr import DataLake, MasterData
import re


def clean_part_serial(value):
    """
    Comprehensive cleaning for part_serial values
    Handles all the inconsistencies found in the data
    """
    if value is None:
        return None

    # Convert to string and strip
    value = str(value).strip().replace(" ", "")

    # Handle completely invalid/empty cases
    invalid_values = [
        "",
        "no",
        "ilegible",
        "ilejible",
        "illegible",
        "nuevo",
        "new",
        "n/a",
        "na",
        "none",
        "faltante",
        "reemplazar",
        "timken",
        "fag",
        "motor",
        "solo",
        "de",
        "c",
        "n",
        "mex",
        "mii",
        "0",
        "00",
        "0000",
        "-2147483648",
    ]

    if value.lower() in invalid_values:
        return None

    # Remove common prefixes/suffixes that indicate issues
    if value.lower().startswith("(ilegible"):
        return None

    # Remove newlines and normalize whitespace
    value = re.sub(r"\s+", " ", value)

    # Extract serial before parentheses, "NUEVO", service order numbers, etc.
    patterns_to_extract = [
        r"^([A-Za-z0-9]+)(?:\s*\([^)]*\))",  # Before parentheses
        r"^([A-Za-z0-9]+)(?:\s+NUEVO\.?)",  # Before NUEVO
        r"^([A-Za-z0-9]+)(?:\s+nuevo\.?)",  # Before nuevo
        r"^\.([A-Za-z0-9]+)",  # Remove leading dot
        r"^([A-Za-z0-9]+)\.?$",  # Just the serial with optional trailing dot
    ]

    for pattern in patterns_to_extract:
        match = re.match(pattern, value, re.IGNORECASE)
        if match:
            cleaned = match.group(1).lower()

            return cleaned

    # If no pattern matches but it's not in invalid list, clean and return
    cleaned = value.lower().strip(".")

    # Final validation
    if len(cleaned) < 2:  # Too short
        return None
    if cleaned.isdigit() and len(cleaned) >= 7:  # Likely service order
        return None

    return cleaned


def apply_backfill_strategy(long_df):
    """
    Apply part-level backfill strategy: For each service_order, use final report as base.
    For each individual part: if missing from final OR has null value,
    fill from next service_order's preliminary report.
    """

    # Join with component_reparations to get reception_date and component_hours
    enriched_df = long_df.sort(["component_serial", "reception_date"])

    # Clean part_serial values first
    enriched_df = enriched_df.with_columns(
        pl.col("part_serial").map_elements(
            clean_part_serial,
            return_dtype=pl.Utf8,
        )
    )

    result = []

    for component_serial in enriched_df["component_serial"].unique():
        # Get all data for this component, sorted chronologically
        component_data = enriched_df.filter(pl.col("component_serial") == component_serial).sort("reception_date")

        # Get unique service_orders for this component, in chronological order
        service_orders = component_data["service_order"].unique().sort()

        for i, service_order in enumerate(service_orders):
            # Get current service_order data
            so_data = component_data.filter(pl.col("service_order") == service_order)
            final_records = so_data.filter(pl.col("document_type") == "final_report").to_dicts()

            # Get ALL possible part names from the dataset (to check what's missing)
            all_possible_parts = component_data["part_name"].unique().to_list()

            # Get next service_order's preliminary data for backfilling
            next_preliminary_data = {}
            if i + 1 < len(service_orders):
                next_service_order = service_orders[i + 1]
                next_so_data = component_data.filter(
                    (pl.col("service_order") == next_service_order) & (pl.col("document_type") == "preliminary_report")
                )
                # Create lookup dict for next preliminary parts
                for record in next_so_data.to_dicts():
                    next_preliminary_data[record["part_name"]] = record

            # Process each possible part for this service_order
            for part_name in all_possible_parts:
                # Check if part exists in final report
                final_part = next((r for r in final_records if r["part_name"] == part_name), None)

                if final_part:
                    # CASE 1: Part exists in final report
                    main_record = final_part.copy()

                    # Check if final report has null/missing part_serial
                    if final_part["part_serial"] is None:
                        # Backfill from next service_order's preliminary
                        if part_name in next_preliminary_data:
                            next_part = next_preliminary_data[part_name]
                            if next_part["part_serial"] is not None:
                                main_record["part_serial"] = next_part["part_serial"]
                else:
                    # CASE 2: Part NOT found in final report
                    # Try to backfill entire record from next service_order's preliminary
                    if part_name in next_preliminary_data:
                        next_part = next_preliminary_data[part_name]
                        if next_part["part_serial"] is not None:
                            # Create a record with final_report document_type but preliminary data
                            main_record = next_part.copy()
                            main_record["service_order"] = service_order  # Keep original service_order
                            main_record["document_type"] = "final_report"  # Mark as final (backfilled)
                        else:
                            continue  # Skip if next preliminary also has null
                    else:
                        continue  # Skip if part not found in next preliminary either

                # Add the processed record
                result.append(main_record)

    return pl.DataFrame(result)


@dg.asset(compute_kind="mutate")
def mutate_part_reparations(context: dg.AssetExecutionContext, component_reparations: pl.DataFrame):
    dl = DataLake(context)
    mt_df = dl.read_tibble(DATA_CATALOG["mt_docs"]["analytics_path"])
    mt_df = mt_df.filter(pl.col("part_serial") != "").join(
        component_reparations.select(["service_order", "component_serial", "reception_date", "component_hours"]),
        on=["service_order", "component_serial"],
        how="left",
    )

    mt_df = mt_df.pipe(apply_backfill_strategy)
    mt_df = (
        mt_df.filter(pl.col("part_serial").is_not_null())
        .sort(["part_serial", "reception_date"])
        .unique(
            subset=["component_serial", "reception_date", "part_name", "part_serial"],
            maintain_order=True,
            keep="last",
        )
    )

    sd_df = dl.read_tibble(DATA_CATALOG["sd_docs"]["analytics_path"])
    sd_df = sd_df.filter(pl.col("part_serial") != "").join(
        component_reparations.select(["service_order", "component_serial", "reception_date", "component_hours"]),
        on=["service_order", "component_serial"],
        how="left",
    )
    print(sd_df.columns)
    sd_df = sd_df.select(
        [
            "service_order",
            "component_serial",
            "part_serial",
            "part_name",
            "component_hours",
            "reception_date",
            "document_type",
        ]
    ).pipe(apply_backfill_strategy)
    sd_df = (
        sd_df.filter(pl.col("part_serial").is_not_null())
        .sort(["part_serial", "reception_date"])
        .unique(
            subset=["component_serial", "reception_date", "part_name", "part_serial"],
            maintain_order=True,
            keep="last",
        )
    )

    df = pl.concat([mt_df, sd_df], how="diagonal")

    # Calculate metrics without double-counting
    df = df.with_columns(
        [
            # Total repairs this specific part has had (across all components)
            pl.int_range(pl.len()).over(["part_name", "part_serial"]).alias("part_repair_count") + 1,
            # Recency rank for this specific part (1 = most recent)
            pl.int_range(pl.len()).reverse().over(["part_name", "part_serial"]).alias("part_repair_recency_rank"),
            # Cumulative hours on this specific part (across all components)
            pl.col("component_hours").cum_sum().over(["part_name", "part_serial"]).alias("cumulative_part_hours"),
            # How many different components this part has been in
            pl.col("component_serial").n_unique().over(["part_name", "part_serial"]).alias("part_component_count"),
            # For component-centric view: repairs of this part_name on this component
            pl.int_range(pl.len()).over(["component_serial", "part_name"]).alias("component_part_name_repair_count")
            + 1,
            # Component-centric: different serials used for this part_name on this component
            pl.col("part_serial")
            .n_unique()
            .over(["component_serial", "part_name"])
            .alias("component_part_name_change_count"),
        ]
    )

    return df
