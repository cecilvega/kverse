import polars as pl
import dagster as dg
from kdags.resources.tidyr import MSGraph, DataLake

COMPONENT_REPARATIONS_ANALYTICS_PATH = (
    "az://bhp-analytics-data/RELIABILITY/COMPONENT_REPARATIONS/component_reparations.parquet"
)


@dg.asset
def component_reparations(
    context: dg.AssetExecutionContext, read_component_changeouts: pl.DataFrame, read_component_status: pl.DataFrame
) -> pl.DataFrame:
    cc_df = (
        read_component_changeouts.clone()
        .filter(pl.col("changeout_date").dt.year() >= 2021)
        .filter(~(pl.col("subcomponent_name").is_in(["motor", "radiador", "subframe"])))
    )
    reso_df = read_component_status.clone()

    # --- Re-run the logic from the provided snippet to get intermediate DFs ---

    # Step 1: Join by customer_service_order and sap_equipment_name
    status_cols_to_join = [
        "customer_work_order",  # Ensure this name is distinct or use suffix
        "sap_equipment_name",
        "reception_date",
        "service_order",
    ]
    # Use suffix to avoid potential column name collisions if 'customer_work_order' is also in cc_df
    joined_df = cc_df.join(
        reso_df.select(status_cols_to_join).filter(
            (pl.col("customer_work_order") != -1) & (pl.col("sap_equipment_name") != -1)
        ),
        on=[
            "customer_work_order",
            "sap_equipment_name",
        ],  # Assuming cc_df uses 'customer_service_order'
        how="left",
        suffix="_reso",  # Added suffix for clarity, affects right-side columns if names match
    )

    # Step 2: Split dataframe based on join success AND date validation
    match_condition = (pl.col("reception_date").is_not_null()) & (pl.col("changeout_date") < pl.col("reception_date"))
    matched_df = joined_df.filter(match_condition)
    unmatched_df = joined_df.filter(~match_condition)

    # Keep only original columns for the part that failed the first match
    unmatched_df = unmatched_df.select(cc_df.columns)

    # --- Preparation for the next step (join_asof) ---
    # Need keys used on the right side during the successful initial join
    # Assuming the join keys were `customer_work_order` and `sap_equipment_name` from reso_df
    # And `service_order` uniquely identifies the right-side row used
    used_service_orders = matched_df.select("service_order").drop_nulls()

    # Filter reso_df to get rows NOT used in the first step
    reso_available_for_asof = reso_df.join(used_service_orders, on="service_order", how="anti")

    # Ensure data is sorted for asof join
    unmatched_df_sorted = unmatched_df.sort("sap_equipment_name", "changeout_date")
    reso_available_for_asof_sorted = reso_available_for_asof.sort("sap_equipment_name", "reception_date")

    # Perform the asof join
    asof_results_df = unmatched_df_sorted.join_asof(
        reso_available_for_asof_sorted,
        left_on="changeout_date",
        right_on="reception_date",
        by="sap_equipment_name",
        strategy="forward",
        tolerance="60d",
    )

    # --- Step 3: Split asof_results_df into matched and unmatched ---

    print("\n--- Splitting Asof Join Results ---")

    # Condition for a successful asof match: a reception_date was found from the right DF
    # The asof join adds columns from the right DF. If no match, these are null.
    # We check 'reception_date' as it's the key time column from the right side.
    asof_match_condition = pl.col("reception_date").is_not_null()

    # Rows where the asof join found a match
    asof_matched_df = asof_results_df.filter(asof_match_condition).select(matched_df.columns)

    # Rows where the asof join did NOT find a match
    asof_unmatched_df = (
        asof_results_df.filter(~asof_match_condition)
        .select(cc_df.columns)
        .with_columns(service_order=pl.lit(-1, dtype=pl.Int64))
    )

    print(f"Total rows in original cc_df: {cc_df.height}")
    print(f"Rows matched in initial join (matched_df): {matched_df.height}")
    print(f"Rows matched in asof join (asof_matched_df): {asof_matched_df.height}")
    print(f"Rows remaining unmatched (asof_unmatched_df): {asof_unmatched_df.height}")

    df = pl.concat([matched_df, asof_matched_df, asof_unmatched_df], how="diagonal")

    datalake = DataLake()  # Direct instantiation
    context.log.info(f"Writing {df.height} records to {COMPONENT_REPARATIONS_ANALYTICS_PATH}")

    datalake.upload_tibble(df=df, az_path=COMPONENT_REPARATIONS_ANALYTICS_PATH, format="parquet")
    context.add_output_metadata(
        {  # Add metadata on success
            "abfs_path": COMPONENT_REPARATIONS_ANALYTICS_PATH,
            "rows_written": df.height,
        }
    )
    return df


@dg.asset(
    description="Reads the consolidated oil analysis data from the ADLS analytics layer.",
)
def read_component_reparations(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake()
    if dl.az_path_exists(COMPONENT_REPARATIONS_ANALYTICS_PATH):
        df = dl.read_tibble(az_path=COMPONENT_REPARATIONS_ANALYTICS_PATH)
        context.log.info(f"Read {df.height} records from {COMPONENT_REPARATIONS_ANALYTICS_PATH}.")
        return df
    else:
        context.log.warning(
            f"Data file not found at {COMPONENT_REPARATIONS_ANALYTICS_PATH}. Returning empty DataFrame."
        )
        return pl.DataFrame()
