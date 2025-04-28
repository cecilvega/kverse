import polars as pl
import dagster as dg
from kdags.resources.tidyr import MSGraph, DataLake

LINKED_COMPONENT_HISTORY_ANALYTICS_PATH = (
    "az://bhp-analytics-data/RELIABILITY/COMPONENT_REPARATIONS/linked_component_history.parquet"
)


@dg.asset(description="Filters the raw component changeouts data based on date and subcomponent type.")
def component_changeouts_filtered(
    component_changeouts: pl.DataFrame,
) -> pl.DataFrame:
    """Filters the raw component changeouts data."""
    return (
        component_changeouts.clone()
        .select(
            [
                "equipment_name",
                "component_name",
                "subcomponent_name",
                "position_name",
                "changeout_date",
                "customer_work_order",
                "sap_equipment_name",
            ]
        )
        .filter(pl.col("changeout_date").dt.year() >= 2021)
        .filter(~(pl.col("subcomponent_name").is_in(["motor", "radiador", "subframe"])))
        .sort(["sap_equipment_name", "changeout_date"])
        .unique(subset=["customer_work_order", "sap_equipment_name"], keep="last", maintain_order=True)
    )


@dg.asset(description="Performs the initial left join between filtered changeouts and component status.")
def component_changeouts_initial_join(
    context: dg.AssetExecutionContext,
    component_changeouts_filtered: pl.DataFrame,
    component_status: pl.DataFrame,
) -> pl.DataFrame:
    """
    Performs the initial left join based on customer_work_order and sap_equipment_name.
    Does not yet filter based on date condition or join success.
    """
    cc_df = component_changeouts_filtered
    cs_df = component_status.clone()
    context.log.info(f"Performing initial join for {cc_df.height} changeout rows.")

    status_cols_to_join = [
        "customer_work_order",
        "sap_equipment_name",
        "reception_date",
        "service_order",
    ]
    joined_df = (
        cc_df.join(
            cs_df.select(status_cols_to_join).filter(
                (pl.col("customer_work_order") != -1) & (pl.col("sap_equipment_name") != -1)
            ),
            on=[
                "customer_work_order",
                "sap_equipment_name",
            ],
            how="left",
            suffix="_reso",  # Suffix applied to right df cols if names collide
        )
        .sort(["sap_equipment_name", "changeout_date", "reception_date"])
        .unique(subset=["customer_work_order", "sap_equipment_name"], keep="last", maintain_order=True)
    )
    context.log.info(f"Initial join complete, result has {joined_df.height} rows.")
    return joined_df


# --- Final Asset 1: Direct Matches ---
@dg.asset(
    description="Component changeouts successfully matched directly by work order, equipment name, and date condition."
)
def changeouts_matched_direct(
    context: dg.AssetExecutionContext,
    component_changeouts_initial_join: pl.DataFrame,
) -> pl.DataFrame:
    """
    Filters the initial join results for successful matches meeting the date criteria.
    Equivalent to 'matched_df'.
    """
    joined_df = component_changeouts_initial_join
    match_condition = (pl.col("reception_date").is_not_null()) & (pl.col("changeout_date") < pl.col("reception_date"))
    matched_df = joined_df.filter(match_condition)
    context.log.info(f"Filtered for direct matches: {matched_df.height} rows.")
    return matched_df


# --- Intermediate Asset: Initially Unmatched Rows ---
@dg.asset(description="Rows from initial join that failed the match or date condition.")
def changeouts_unmatched_initial(
    context: dg.AssetExecutionContext,
    component_changeouts_initial_join: pl.DataFrame,
    component_changeouts_filtered: pl.DataFrame,  # Needed for original columns
) -> pl.DataFrame:
    """
    Filters the initial join results for rows that need ASOF join processing.
    Selects only the original columns from component_changeouts_filtered.
    """
    joined_df = component_changeouts_initial_join
    original_cc_columns = component_changeouts_filtered.columns
    match_condition = (pl.col("reception_date").is_not_null()) & (pl.col("changeout_date") < pl.col("reception_date"))
    unmatched_initial_df = joined_df.filter(~match_condition).select(original_cc_columns)
    context.log.info(f"Filtered for initially unmatched rows: {unmatched_initial_df.height} rows.")
    return unmatched_initial_df


# --- Intermediate Asset: Used Service Orders ---
@dg.asset(description="Service orders corresponding to directly matched changeouts.")
def used_service_orders(
    changeouts_matched_direct: pl.DataFrame,
) -> pl.DataFrame:
    """Extracts the service orders used in the direct matches."""
    # Ensure the column exists and handle potential nulls from the join
    if "service_order" not in changeouts_matched_direct.columns:
        # Handle case where service_order might not be present or renamed
        # This might happen if the join failed unexpectedly or columns were dropped.
        # Returning an empty DataFrame with the expected schema is a safe fallback.
        return pl.DataFrame({"service_order": []}, schema={"service_order": pl.Int64})

    return changeouts_matched_direct.select("service_order").drop_nulls()


# --- Intermediate Asset: Component Status Available for ASOF ---
@dg.asset(description="Component status rows not used in the direct match.")
def reso_available_for_asof(
    context: dg.AssetExecutionContext,
    component_status: pl.DataFrame,
    used_service_orders: pl.DataFrame,
) -> pl.DataFrame:
    """Filters component status using an anti-join against used service orders."""
    cs_df = component_status
    available_df = cs_df.join(used_service_orders, on="service_order", how="anti").filter(
        pl.col("sap_equipment_name") != -1
    )
    context.log.info(f"Component status rows available for ASOF join: {available_df.height}")
    return available_df


# --- Intermediate Asset: ASOF Join Results ---
@dg.asset(description="Performs ASOF join for initially unmatched rows against available component status.")
def component_changeouts_asof_join_results(
    context: dg.AssetExecutionContext,
    changeouts_unmatched_initial: pl.DataFrame,
    reso_available_for_asof: pl.DataFrame,
) -> pl.DataFrame:
    """Sorts inputs and performs the ASOF join."""
    unmatched_df_sorted = changeouts_unmatched_initial.sort("sap_equipment_name", "changeout_date")
    reso_available_for_asof_sorted = reso_available_for_asof.sort("sap_equipment_name", "reception_date")

    context.log.info(
        f"Performing ASOF join on {unmatched_df_sorted.height} rows "
        f"against {reso_available_for_asof_sorted.height} available status rows."
    )

    asof_results_df = unmatched_df_sorted.join_asof(
        reso_available_for_asof_sorted,
        left_on="changeout_date",
        right_on="reception_date",
        by="sap_equipment_name",
        strategy="forward",
        tolerance="60d",
    )
    context.log.info(f"ASOF join completed. Result has {asof_results_df.height} rows.")
    return asof_results_df


# --- Final Asset 2: ASOF Matches ---
@dg.asset(description="Component changeouts successfully matched using ASOF join logic.")
def changeouts_matched_asof(
    context: dg.AssetExecutionContext,
    component_changeouts_asof_join_results: pl.DataFrame,
    changeouts_matched_direct: pl.DataFrame,  # Used for target schema
) -> pl.DataFrame:
    """
    Filters the ASOF join results for successful matches.
    Equivalent to 'asof_matched_df'. Output schema matches changeouts_matched_direct.
    """
    asof_results_df = component_changeouts_asof_join_results
    target_schema_columns = changeouts_matched_direct.columns

    asof_match_condition = pl.col("reception_date").is_not_null()
    asof_matched_df = asof_results_df.filter(asof_match_condition).select(target_schema_columns)

    context.log.info(f"Filtered for ASOF matches: {asof_matched_df.height} rows.")
    return asof_matched_df


# --- Final Asset 3: Unmatched ---
@dg.asset(description="Component changeouts that remained unmatched after both direct and ASOF attempts.")
def changeouts_unmatched(
    context: dg.AssetExecutionContext,
    component_changeouts_asof_join_results: pl.DataFrame,
    component_changeouts_filtered: pl.DataFrame,  # Needed for original columns
) -> pl.DataFrame:
    """
    Filters the ASOF join results for unsuccessful matches.
    Equivalent to 'asof_unmatched_df'. Output Schema: Original columns + service_order=-1.
    """
    asof_results_df = component_changeouts_asof_join_results
    original_cc_columns = component_changeouts_filtered.columns

    asof_match_condition = pl.col("reception_date").is_not_null()
    asof_unmatched_df = (
        asof_results_df.filter(~asof_match_condition)
        .select(original_cc_columns)
        .with_columns(service_order=pl.lit(-1, dtype=pl.Int64))
    )

    context.log.info(f"Filtered for final unmatched rows: {asof_unmatched_df.height} rows.")
    return asof_unmatched_df


@dg.asset(
    description=(
        "Calculates the percentage of component changeouts that were filtered out "
        "before being matched to reparation status. Currently returns the raw changeout data."
    )
)
def mutate_linked_component_history(
    context: dg.AssetExecutionContext,
    component_changeouts: pl.DataFrame,
    changeouts_matched_direct: pl.DataFrame,
    changeouts_matched_asof: pl.DataFrame,
    changeouts_unmatched: pl.DataFrame,
) -> pl.DataFrame:

    cc_df = component_changeouts.clone()

    # Get row counts
    total_raw_rows = cc_df.height
    matched_direct_rows = changeouts_matched_direct.height
    matched_asof_rows = changeouts_matched_asof.height
    unmatched_rows = changeouts_unmatched.height

    # Calculate total rows accounted for by the matching process
    total_accounted_rows = matched_direct_rows + matched_asof_rows + unmatched_rows
    # Calculate rows not considered (filtered out before/during matching)
    rows_filtered_out = total_raw_rows - total_accounted_rows
    perc_direct = (matched_direct_rows / total_raw_rows) * 100
    perc_asof = (matched_asof_rows / total_raw_rows) * 100
    perc_unmatched = (unmatched_rows / total_raw_rows) * 100
    perc_accounted = (total_accounted_rows / total_raw_rows) * 100
    percentage_filtered_out = (rows_filtered_out / total_raw_rows) * 100

    context.log.info(f"Initial Changeouts: {total_raw_rows} rows.")
    context.log.info(f" -> Matched Direct: {matched_direct_rows} ({perc_direct:.1f}%).")
    context.log.info(f" -> Matched ASOF: {matched_asof_rows} ({perc_asof:.1f}%).")
    context.log.info(f" -> Unmatched: {unmatched_rows} ({perc_unmatched:.1f}%).")
    context.log.info(f" -> Total Accounted by Matching: {total_accounted_rows} ({perc_accounted:.1f}%).")
    context.log.info(
        f" -> Rows Filtered Out (Not Accounted): {rows_filtered_out} " f"({percentage_filtered_out:.1f}%)."
    )
    context.log.info(
        f"Rows filtered out before matching: {rows_filtered_out} " f"({percentage_filtered_out:.2f}% of initial total)"
    )

    df = pl.concat([changeouts_matched_direct, changeouts_matched_asof]).select(
        [
            "equipment_name",
            "component_name",
            "subcomponent_name",
            "position_name",
            "changeout_date",
            "customer_work_order",
            "sap_equipment_name",
            "service_order",
            "reception_date",
        ]
    )
    # df = cc_df.join(match_df, on=["customer_work_order", "sap_equipment_name"], how="left")

    datalake = DataLake()  # Direct instantiation
    context.log.info(f"Writing {df.height} records to {LINKED_COMPONENT_HISTORY_ANALYTICS_PATH}")

    datalake.upload_tibble(tibble=df, az_path=LINKED_COMPONENT_HISTORY_ANALYTICS_PATH, format="parquet")
    context.add_output_metadata(
        {  # Add metadata on success
            "az_path": LINKED_COMPONENT_HISTORY_ANALYTICS_PATH,
            "rows_written": df.height,
        }
    )

    return df


@dg.asset(
    description="Reads the consolidated oil analysis data from the ADLS analytics layer.",
)
def linked_component_history(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake()
    if dl.az_path_exists(LINKED_COMPONENT_HISTORY_ANALYTICS_PATH):
        df = dl.read_tibble(az_path=LINKED_COMPONENT_HISTORY_ANALYTICS_PATH)
        context.log.info(f"Read {df.height} records from {LINKED_COMPONENT_HISTORY_ANALYTICS_PATH}.")
        return df
    else:
        context.log.warning(
            f"Data file not found at {LINKED_COMPONENT_HISTORY_ANALYTICS_PATH}. Returning empty DataFrame."
        )
        return pl.DataFrame()
