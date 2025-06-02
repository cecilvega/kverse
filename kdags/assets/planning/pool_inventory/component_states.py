import dagster as dg
from kdags.resources.tidyr import MasterData, DataLake
import polars as pl
from kdags.config import DATA_CATALOG
from datetime import datetime


state_transitions = [
    ("mounted_date", "mounted"),
    ("reso_closing_date", "subida"),
    ("load_final_report_date", "repaired"),
    ("latest_quotation_publication", "assembly"),
    ("load_preliminary_report_date", "awaiting_approval"),
    ("reception_date", "evaluation"),
    ("changeout_date", "bajada"),
]


def build_state_logic(transitions, snapshot_col):
    # Start with retirement check (highest priority)
    condition = pl.when((pl.col("retired_date").is_not_null()) & (snapshot_col >= pl.col("retired_date"))).then(
        pl.lit("retired")
    )

    # # Before changeout = mounted (base state)
    # condition = condition.when(snapshot_col < pl.col("changeout_date")).then(
    #     pl.lit("mounted")
    # )

    # After changeout, find the furthest state reached by snapshot_date
    for date_col, state in transitions:
        condition = condition.when((pl.col(date_col).is_not_null()) & (snapshot_col >= pl.col(date_col))).then(
            pl.lit(state)
        )

    # Fallback: after changeout but no other dates = mounted (component hasn't been unmounted)
    condition = condition.when(snapshot_col >= pl.col("changeout_date")).then(pl.lit("mounted"))

    return condition.otherwise(pl.lit("unknown"))


@dg.asset
def mutate_component_snapshots(
    context: dg.AssetExecutionContext,
    mutate_component_lifeline: pl.DataFrame,
    so_report: pl.DataFrame,
    component_serials: pl.DataFrame,
):
    dl = DataLake(context)
    merge_columns = ["service_order", "component_serial", "sap_equipment_name"]
    df = mutate_component_lifeline.join(
        so_report.with_columns(
            reso_closing_date=pl.col("reso_closing_date").fill_null(pl.col("load_final_report_date"))
        ).select(
            [
                *merge_columns,
                "load_preliminary_report_date",
                "latest_quotation_publication",
                "load_final_report_date",
                "reso_closing_date",
            ]
        ),
        on=merge_columns,
        how="left",
    ).with_columns(
        changeout_week=pl.concat_str(
            [
                pl.col("changeout_date").dt.iso_year().cast(pl.String),
                pl.lit("-W"),
                pl.col("changeout_date").dt.week().cast(pl.String).str.pad_start(2, "0"),
            ]
        )
    )

    # Cross join changeouts with snapshots
    # Este representa el estado de cada serie del componente en todo momento
    snapshots_df = (
        pl.date_range(
            start=datetime(2014, 1, 6),  # First Monday of 2014
            end=datetime.now(),
            interval="1w",
            eager=True,
        )
        .alias("snapshot_date")
        .to_frame()
        .with_columns(
            iso_year=pl.col("snapshot_date").dt.iso_year(),
            iso_week=pl.col("snapshot_date").dt.week(),
        )
        .with_columns(
            snapshot_week=pl.concat_str(
                [
                    pl.col("iso_year").cast(pl.String),
                    pl.lit("-W"),
                    pl.col("iso_week").cast(pl.String).str.pad_start(2, "0"),
                ]
            )
        )
        .drop(["iso_year", "iso_week"])
    )
    by_columns = [
        "component_name",
        "subcomponent_name",
        "component_serial",
        "sap_equipment_name",
    ]
    cross_df = component_serials.select([*by_columns]).join(snapshots_df, how="cross")

    cross_df = cross_df.join_asof(
        df.sort([*by_columns, "changeout_date"]).select([*by_columns, "changeout_date"]),
        by=by_columns,
        left_on="snapshot_date",
        right_on="changeout_date",
        strategy="backward",  # Get most recent changeout BEFORE snapshot
    )
    cross_df = cross_df.drop_nulls(subset=["changeout_date"])
    cross_df = cross_df.join(df, on=[*by_columns, "changeout_date"], how="left").join(
        component_serials, on=by_columns, how="left"
    )
    cross_df = cross_df.with_columns(
        [build_state_logic(state_transitions, pl.col("snapshot_date")).alias("component_state")]
    )

    return cross_df


@dg.asset
def mutate_component_states(context: dg.AssetExecutionContext, mutate_component_snapshots):
    """
    Create state periods by compressing consecutive identical states
    """
    # Get unique components
    dl = DataLake(context)
    df = mutate_component_snapshots.clone()
    component_serials = df.select("component_serial").unique().to_series().to_list()

    all_periods = []

    for component in component_serials:
        # Get data for this component, sorted by date
        comp_data = (
            df.filter(pl.col("component_serial") == component)
            .sort("snapshot_date")
            .select(["snapshot_date", "component_state"])
        )

        if comp_data.height == 0:
            continue

        # Create state change indicator (when state changes from previous row)
        comp_data = comp_data.with_columns(
            [
                (pl.col("component_state") != pl.col("component_state").shift(1))
                .fill_null(True)  # First row is always a change
                .cum_sum()
                .alias("state_group")
            ]
        )

        # Group by state and state_group to get periods
        periods = (
            comp_data.group_by(["component_state", "state_group"])
            .agg(
                [
                    pl.col("snapshot_date").min().alias("start_date"),
                    pl.col("snapshot_date").max().alias("end_date"),
                ]
            )
            .with_columns(
                [
                    pl.lit(component).alias("component_serial"),
                    # Add 7 days to end_date to complete the period
                    (pl.col("end_date") + pl.duration(days=7)).alias("end_date"),
                ]
            )
            .select(["component_serial", "component_state", "start_date", "end_date"])
            .rename({"component_state": "state"})
        )

        all_periods.append(periods)
        # Combine all periods and convert to pandas for matplotlib
    df = pl.concat(all_periods).sort(["component_serial", "start_date"])
    dl.upload_tibble(df, DATA_CATALOG["component_states"]["analytics_path"])
    return df


# @dg.asset
# def component_snapshots(context: dg.AssetExecutionContext):
#     dl = DataLake(context)
#     df = dl.read_tibble(DATA_CATALOG["component_snapshots"]["analytics_path"])
#     return df


@dg.asset
def component_states(context: dg.AssetExecutionContext):
    dl = DataLake(context)
    df = dl.read_tibble(DATA_CATALOG["component_states"]["analytics_path"])
    return df
