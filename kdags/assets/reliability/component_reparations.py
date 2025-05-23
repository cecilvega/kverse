import polars as pl
import dagster as dg
from kdags.resources.tidyr import MSGraph, DataLake, MasterData
from kdags.config import DATA_CATALOG

COMPONENT_REPARATIONS_ANALYTICS_PATH = (
    "az://bhp-analytics-data/RELIABILITY/COMPONENT_REPARATIONS/component_reparations.parquet"
)

MERGE_COLUMNS = ["equipment_name", "component_name", "subcomponent_name", "position_name", "changeout_date"]


def component_changeouts_initial_join(
    context: dg.AssetExecutionContext,
    component_changeouts_filtered: pl.DataFrame,
    mutate_so_report: pl.DataFrame,
) -> pl.DataFrame:
    """
    Performs the initial left join based on customer_work_order and sap_equipment_name.
    Does not yet filter based on date condition or join success.
    """
    cc_df = component_changeouts_filtered.clone()
    so_df = mutate_so_report.clone().sort(["service_order", "reception_date"]).unique(["service_order"], keep="last")
    context.log.info(f"Performing initial join for {cc_df.height} changeout rows.")

    status_cols_to_join = [
        "customer_work_order",
        "sap_equipment_name",
        "reception_date",
        "service_order",
    ]
    joined_df = (
        cc_df.join(
            so_df.select(status_cols_to_join).filter(
                (pl.col("customer_work_order") != -1) & (pl.col("sap_equipment_name") != -1)
            ),
            on=[
                "customer_work_order",
                "sap_equipment_name",
            ],
            how="left",
            suffix="_reso",  # Suffix applied to right df cols if names collide
        ).sort(["sap_equipment_name", "changeout_date", "reception_date"])
        # .unique(subset=["customer_work_order", "sap_equipment_name"], keep="last", maintain_order=True)
    )
    context.log.info(f"Initial join complete, result has {joined_df.height} rows.")
    return joined_df


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


def reso_available_for_asof(
    context: dg.AssetExecutionContext,
    mutate_so_report: pl.DataFrame,
    changeouts_matched_direct: pl.DataFrame,
) -> pl.DataFrame:
    """Filters component status using an anti-join against used service orders."""
    so_df = mutate_so_report
    available_df = so_df.join(
        changeouts_matched_direct, on=["service_order", "customer_work_order"], how="anti"
    ).filter(pl.col("sap_equipment_name") != -1)
    context.log.info(f"Component status rows available for ASOF join: {available_df.height}")
    return available_df


def component_changeouts_asof_join_results(
    context: dg.AssetExecutionContext,
    changeouts_unmatched_initial: pl.DataFrame,
    reso_available_for_asof: pl.DataFrame,
) -> pl.DataFrame:
    """Sorts inputs and performs the ASOF join."""
    unmatched_df_sorted = changeouts_unmatched_initial.sort(["sap_equipment_name", "changeout_date"])
    reso_available_for_asof_sorted = reso_available_for_asof.sort(["sap_equipment_name", "reception_date"])

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
        tolerance="70d",
    )
    context.log.info(f"ASOF join completed. Result has {asof_results_df.height} rows.")
    return asof_results_df


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
    group_name="reliability",
    description=(
        "Calculates the percentage of component changeouts that were filtered out "
        "before being matched to reparation status. Currently returns the raw changeout data."
    ),
)
def mutate_component_reparations(
    context: dg.AssetExecutionContext, mutate_component_changeouts: pl.DataFrame, mutate_so_report: pl.DataFrame
) -> pl.DataFrame:
    components_df = MasterData.components().select(["component_name", "subcomponent_name"]).unique()
    cc_df = mutate_component_changeouts.clone()
    so_report_df = mutate_so_report.clone()
    component_changeouts_filtered_df = (
        cc_df.clone()
        .join(components_df, how="inner", on=["component_name", "subcomponent_name"])
        .select(
            [
                "cc_index",
                "equipment_name",
                "component_name",
                "subcomponent_name",
                "position_name",
                "changeout_date",
                "customer_work_order",
                "sap_equipment_name",
                "component_serial",
                "component_hours",
                "component_usage",
            ]
        )
        .drop_nulls(subset=["position_name"])
        .filter(~(pl.col("subcomponent_name").is_in(["motor", "radiador", "subframe"])))
        .sort(["sap_equipment_name", "changeout_date"])
    )

    component_changeouts_initial_join_df = component_changeouts_initial_join(
        context, component_changeouts_filtered_df, so_report_df
    )
    changeouts_matched_direct_df = changeouts_matched_direct(context, component_changeouts_initial_join_df)

    changeouts_unmatched_initial_df = changeouts_unmatched_initial(
        context, component_changeouts_initial_join_df, component_changeouts_filtered_df
    )

    reso_available_for_asof_df = reso_available_for_asof(context, so_report_df, changeouts_matched_direct_df)

    component_changeouts_asof_join_results_df = component_changeouts_asof_join_results(
        context,
        changeouts_unmatched_initial_df,
        reso_available_for_asof_df,
    )

    changeouts_matched_asof_df = changeouts_matched_asof(
        context,
        component_changeouts_asof_join_results_df,
        changeouts_matched_direct_df,
    )

    changeouts_unmatched_df = changeouts_unmatched(
        context,
        component_changeouts_asof_join_results_df,
        component_changeouts_filtered_df,
    )

    # Get row counts
    total_raw_rows = cc_df.height
    matched_direct_rows = changeouts_matched_direct_df.height
    matched_asof_rows = changeouts_matched_asof_df.height
    unmatched_rows = changeouts_unmatched_df.height

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

    df = pl.concat([changeouts_matched_direct_df, changeouts_matched_asof_df]).drop(
        [
            "cc_index",
            "component_serial",
            "component_hours",
            "component_usage",
            "customer_work_order",
            "sap_equipment_name",
        ]
    )
    df = (
        component_changeouts_filtered_df.select(
            [
                "cc_index",
                *MERGE_COLUMNS,
                "component_serial",
                "component_hours",
                "component_usage",
                "customer_work_order",
                "sap_equipment_name",
            ]
        )
        .join(
            df,
            how="full",
            on=MERGE_COLUMNS,
            coalesce=True,
            nulls_equal=True,
        )
        .select(
            [
                *MERGE_COLUMNS,
                "service_order",
                "reception_date",
                "component_serial",
                "customer_work_order",
                "sap_equipment_name",
                "component_hours",
                "component_usage",
                "cc_index",
            ]
        )
        .sort(
            [
                *MERGE_COLUMNS,
                "reception_date",
            ]
        )
        .unique(subset=MERGE_COLUMNS, keep="last")
        .sort(["changeout_date", "equipment_name", "component_name", "subcomponent_name", "position_name"])
    )

    dl = DataLake(context=context)

    dl.upload_tibble(tibble=df, az_path=DATA_CATALOG["component_reparations"]["analytics_path"])

    return df


@dg.asset(
    group_name="readr",
    description="Reads the consolidated oil analysis data from the ADLS analytics layer.",
)
def component_reparations(context: dg.AssetExecutionContext) -> pl.DataFrame:
    dl = DataLake(context=context)
    df = dl.read_tibble(az_path=DATA_CATALOG["component_reparations"]["analytics_path"])
    return df
