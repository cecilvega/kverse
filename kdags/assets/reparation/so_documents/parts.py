import polars as pl
import dagster as dg
from kdags.config import DATA_CATALOG
from kdags.resources.tidyr import DataLake, MasterData
import re

SUBPARTS_MAPPING = {
    "subcomponent_tag": [
        "0980",
        "0980",
        "0980",
        "0980",
        "0980",
        "0980",
        "0980",
        "0980",
        "0980",
        "0980",
        "0980",
        "5A30",
    ],
    "subpart_name": [
        "coupling_plate",
        "driveshaft",
        "high_speed_gear_a",
        "high_speed_gear_b",
        "high_speed_gear_c",
        "low_speed_gear_12hr",
        "low_speed_gear_3hr",
        "low_speed_gear_6hr",
        "low_speed_gear_9hr",
        "ring_gear",
        "sun_pinion",
        "vastago",
    ],
    "part_name": [
        "coupling_plate",
        "driveshaft",
        "high_speed_gear",
        "high_speed_gear",
        "high_speed_gear",
        "low_speed_gear",
        "low_speed_gear",
        "low_speed_gear",
        "low_speed_gear",
        "ring_gear",
        "sun_pinion",
        "vastago",
    ],
}


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


#
#
# def apply_backfill_strategy(long_df):
#     """
#     Apply part-level backfill strategy with proper tracking of data sources
#     """
#
#     # Join with component_reparations to get reception_date and component_hours
#     enriched_df = long_df.sort(["component_serial", "reception_date"])
#
#     # Clean part_serial values first
#     enriched_df = enriched_df.with_columns(
#         pl.col("part_serial").map_elements(
#             clean_part_serial,
#             return_dtype=pl.Utf8,
#         )
#     )
#
#     result = []
#
#     for component_serial in enriched_df["component_serial"].unique():
#         # Get all data for this component, sorted chronologically
#         component_data = enriched_df.filter(pl.col("component_serial") == component_serial).sort("reception_date")
#
#         # Get unique service_orders for this component, in chronological order
#         service_orders = component_data["service_order"].unique().sort()
#
#         for i, service_order in enumerate(service_orders):
#             # Get current service_order data
#             so_data = component_data.filter(pl.col("service_order") == service_order)
#             final_records = so_data.filter(pl.col("document_type") == "final_report").to_dicts()
#             preliminary_records = so_data.filter(pl.col("document_type") == "preliminary_report").to_dicts()
#
#             # Get base metadata for this service_order (reception_date, component_hours, etc.)
#             base_metadata = None
#             if final_records:
#                 base_metadata = final_records[0]
#             elif preliminary_records:
#                 base_metadata = preliminary_records[0]
#             else:
#                 continue  # No data for this service_order
#
#             # Get ALL possible part names from the dataset
#             all_possible_parts = component_data["part_name"].unique().to_list()
#
#             # Get next service_order's preliminary data for backfilling
#             next_preliminary_data = {}
#             if i + 1 < len(service_orders):
#                 next_service_order = service_orders[i + 1]
#                 next_so_data = component_data.filter(
#                     (pl.col("service_order") == next_service_order) & (pl.col("document_type") == "preliminary_report")
#                 )
#                 # Create lookup dict for next preliminary parts
#                 for record in next_so_data.to_dicts():
#                     next_preliminary_data[record["part_name"]] = record
#
#             # Process each possible part for this service_order
#             for part_name in all_possible_parts:
#                 # Check if part exists in final report
#                 final_part = next((r for r in final_records if r["part_name"] == part_name), None)
#
#                 if final_part:
#                     # CASE 1: Part exists in final report
#                     main_record = final_part.copy()
#
#                     # Check if final report has null/missing part_serial
#                     if final_part["part_serial"] is None:
#                         # Backfill part_serial from next service_order's preliminary
#                         if part_name in next_preliminary_data:
#                             next_part = next_preliminary_data[part_name]
#                             if next_part["part_serial"] is not None:
#                                 main_record["part_serial"] = next_part["part_serial"]
#                                 main_record["data_source"] = "final_backfilled_from_next_preliminary"
#                             else:
#                                 main_record["data_source"] = "final_original"
#                         else:
#                             main_record["data_source"] = "final_original"
#                     else:
#                         main_record["data_source"] = "final_original"
#
#                 else:
#                     # CASE 2: Part NOT found in final report
#                     # Try to backfill entire record from next service_order's preliminary
#                     if part_name in next_preliminary_data:
#                         next_part = next_preliminary_data[part_name]
#                         if next_part["part_serial"] is not None:
#                             # Create record using CURRENT service_order's metadata + next preliminary's part data
#                             main_record = base_metadata.copy()  # Use current SO's metadata (reception_date, etc.)
#                             main_record["part_name"] = next_part["part_name"]
#                             main_record["part_serial"] = next_part["part_serial"]
#                             main_record["document_type"] = "final_report"  # Keep as final_report
#                             main_record["data_source"] = "missing_from_final_backfilled_from_next_preliminary"
#                         else:
#                             continue  # Skip if next preliminary also has null
#                     else:
#                         continue  # Skip if part not found in next preliminary either
#
#                 # Add the processed record
#                 result.append(main_record)
#
#     return pl.DataFrame(result)


def apply_backfill_strategy(df):
    return df
    # return df.filter(pl.col("document_type") == "final_report")


@dg.asset
def parts_base(component_reparations) -> pl.DataFrame:
    document_types = ["preliminary_report", "final_report"]
    cr_columns = ["subcomponent_tag", "component_serial", "service_order", "reception_date"]
    df = component_reparations.clone().select(cr_columns)
    df = pl.concat(
        [
            df.filter(pl.col("subcomponent_tag") == "0980").join(
                pl.DataFrame(SUBPARTS_MAPPING).filter(pl.col("subcomponent_tag") == "0980").drop("subcomponent_tag"),
                how="cross",
            ),
            df.filter(pl.col("subcomponent_tag") == "5A30").join(
                pl.DataFrame(SUBPARTS_MAPPING).filter(pl.col("subcomponent_tag") == "5A30").drop("subcomponent_tag"),
                how="cross",
            ),
        ],
        how="diagonal",
    )
    df = df.join(pl.DataFrame({"document_type": document_types}), how="cross").sort(
        [
            "subcomponent_tag",
            "component_serial",
            "subpart_name",
            "service_order",
            "reception_date",
            "document_type",
        ],
        descending=[True, True, True, True, True, False],
    )
    return df


@dg.asset(compute_kind="mutate")
def mutate_part_reparations(
    context: dg.AssetExecutionContext, component_reparations: pl.DataFrame, parts_base: pl.DataFrame
):
    dl = DataLake(context)

    mt_df = dl.read_tibble(DATA_CATALOG["mt_docs"]["analytics_path"])
    mt_df = mt_df.join(
        component_reparations.select(["service_order", "component_serial", "reception_date", "component_hours"]),
        on=["service_order", "component_serial"],
        how="left",
    )
    # .filter(pl.col("part_serial") != "")

    mt_df = mt_df.pipe(apply_backfill_strategy)
    mt_df = (
        mt_df.filter(pl.col("part_serial").is_not_null()).sort(["part_serial", "reception_date"])
        # .unique(
        #     subset=["component_serial", "reception_date", "part_name", "part_serial", "document_type"],
        #     maintain_order=True,
        #     keep="last",
        # )
    )

    sd_df = dl.read_tibble(DATA_CATALOG["sd_docs"]["analytics_path"])
    sd_df = sd_df.filter(pl.col("part_serial") != "").join(
        component_reparations.select(["service_order", "component_serial", "reception_date", "component_hours"]),
        on=["service_order", "component_serial"],
        how="left",
    )

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
        sd_df.filter(pl.col("part_serial").is_not_null()).sort(["part_serial", "reception_date"])
        # .unique(
        #     subset=["component_serial", "reception_date", "part_name", "part_serial"],
        #     maintain_order=True,
        #     keep="last",
        # )
    )

    df = pl.concat([mt_df, sd_df], how="diagonal")

    # Calculate metrics without double-counting
    df = df.with_columns(
        [
            # Total repairs this specific part has had (across all components)
            pl.int_range(pl.len()).over(["part_serial"]).alias("part_repair_count") + 1,
            # Recency rank for this specific part (1 = most recent)
            pl.int_range(pl.len()).reverse().over(["part_serial"]).alias("part_repair_recency_rank"),
            # Cumulative hours on this specific part (across all components)
            pl.col("component_hours").cum_sum().over(["part_serial"]).alias("cumulative_part_hours"),
            # How many different components this part has been in
            pl.col("component_serial").n_unique().over(["part_serial"]).alias("part_component_count"),
        ]
    )

    return df
