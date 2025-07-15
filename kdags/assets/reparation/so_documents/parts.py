import polars as pl
import dagster as dg
from kdags.config import DATA_CATALOG
from kdags.resources.tidyr import DataLake, MasterData
import re
from .part_lifecycle_graph import *

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


@dg.asset
def parts_base(component_reparations) -> pl.DataFrame:
    document_types = ["preliminary_report", "final_report"]
    cr_columns = ["subcomponent_tag", "component_serial", "service_order", "reception_date"]
    df = (
        component_reparations.clone()
        .select(cr_columns)
        .join(MasterData.component_serials().select(["component_serial"]), how="inner", on="component_serial")
    )

    df = pl.concat(
        [
            df.filter(pl.col("subcomponent_tag") == "0980").join(
                pl.DataFrame(SUBPARTS_MAPPING).filter(pl.col("subcomponent_tag") == "0980").drop("subcomponent_tag"),
                how="cross",
            )
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


@dg.asset
def raw_parts(context: dg.AssetExecutionContext, parts_base: pl.DataFrame) -> pl.DataFrame:
    dl = DataLake(context)
    mt_df = dl.read_tibble(DATA_CATALOG["mt_docs"]["analytics_path"])

    df = pl.concat(
        [
            mt_df.with_columns(subcomponent_tag=pl.lit("0980")),
        ],
        how="diagonal",
    )

    merge_columns = [
        "subcomponent_tag",
        "component_serial",
        "service_order",
        "subpart_name",
        "document_type",
    ]
    df = parts_base.join(
        df.rename({"part_name": "subpart_name"}).select(
            [
                *[
                    "subcomponent_tag",
                    "component_serial",
                    "service_order",
                    "subpart_name",
                    "document_type",
                ],
                "part_serial",
            ]
        ),
        how="left",
        on=merge_columns,
        validate="1:1",
    ).sort(
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

    df = df.with_columns(
        pl.col("part_serial").map_elements(
            clean_part_serial,
            return_dtype=pl.Utf8,
        )
    )
    return df


@dg.asset(compute_kind="mutate")
def pivot_parts(context: dg.AssetExecutionContext, component_reparations: pl.DataFrame, raw_parts: pl.DataFrame):

    preliminary_df = (
        raw_parts.filter(pl.col("document_type") == "preliminary_report")
        .rename({"part_serial": "initial_part_serial"})
        .drop("document_type")
    )
    final_df = (
        raw_parts.filter(pl.col("document_type") == "final_report")
        .rename({"part_serial": "final_part_serial"})
        .drop("document_type")
    )
    # Join preliminary and final reports to create the pivot
    # Join on all grouping columns to match corresponding preliminary and final reports
    df = final_df.join(
        preliminary_df,
        on=[
            "subcomponent_tag",
            "service_order",
            "reception_date",
            "component_serial",
            "part_name",
            "subpart_name",
        ],
        how="full",
        # validate="1:1",
        coalesce=True,
    ).join(
        component_reparations.group_by(["subcomponent_tag", "component_serial", "service_order"]).agg(
            component_hours=pl.max("component_hours")
        ),
        how="left",
        on=["subcomponent_tag", "component_serial", "service_order"],
        validate="m:1",
    )

    return df
