# --- Default imports ---
# --- Selenium imports ---
from datetime import date

import dagster as dg
import polars as pl


# Define update windows
COMPLETED_UPDATE_WINDOW_DAYS = 90  # Update completed SOs for 90 days after closing
MIN_UPDATE_INTERVAL_DAYS = 10  # Don't update same SO more than once per 10 days


@dg.asset()
def select_so_to_update(raw_so_quotations, so_report: pl.DataFrame):

    so_df = so_report.clone()

    df = (
        so_df.filter(pl.col("first_quotation_publication").is_not_null())
        .sort("reception_date", descending=True)
        .select(
            [
                "service_order",
                "component_serial",
                "reception_date",
                "load_final_report_date",
                "update_date",
                "reso_closing_date",
                "reso_repair_reason",
                "quotation_status",
                "component_status",
                "site_name",
            ]
        )
        .with_columns(reso_closing_date=pl.col("reso_closing_date").fill_null(pl.col("load_final_report_date")))
        .with_columns(days_diff=(pl.col("update_date") - pl.col("reso_closing_date")).dt.total_days())
        .drop(["load_final_report_date"])
    )

    df = df.join(
        raw_so_quotations.select(["service_order", "update_timestamp"]),
        on="service_order",
        how="left",
    )
    df = df.with_columns(
        [
            # Days since last update (NULL if never updated)
            pl.when(pl.col("update_timestamp").is_not_null())
            .then((pl.lit(date.today()) - pl.col("update_timestamp")).dt.total_days())
            .otherwise(None)
            .alias("days_since_update"),
            # Is this SO new (never processed)?
            pl.col("update_timestamp").is_null().alias("is_new_so"),
        ]
    )
    # Select SOs based on logical rules
    df = df.filter(
        # Rule 1: Always select new SOs that have never been processed
        pl.col("is_new_so")
        |
        # Rule 2: Select active/incomplete SOs that haven't been updated recently
        (
            ~pl.col("component_status").is_in(["Delivered", "Repaired"])
            & (pl.col("days_since_update") > MIN_UPDATE_INTERVAL_DAYS)
        )
        |
        # Rule 3: Select recently completed SOs (within update window) that haven't been updated recently
        (
            pl.col("component_status").is_in(["Delivered", "Repaired"])
            & pl.col("reso_closing_date").is_not_null()
            & (pl.col("days_diff") < COMPLETED_UPDATE_WINDOW_DAYS)
            & (pl.col("days_since_update") > MIN_UPDATE_INTERVAL_DAYS)
        )
    )

    # Apply site filter
    df = df.filter(pl.col("site_name").is_in(["Minera Spence", "MINERA ESCONDIDA"]))
    return df
