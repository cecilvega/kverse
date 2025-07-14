import dagster as dg
import polars as pl

from kdags.config import DATA_CATALOG, TIDY_NAMES, tidy_tibble
from kdags.resources.tidyr import DataLake, MasterData, MSGraph
import numpy as np
from datetime import date


@dg.asset(compute_kind="mutate")
def mutate_smr(context: dg.AssetExecutionContext, oil_analysis: pl.DataFrame):
    dl = DataLake(context)

    df = (
        oil_analysis.drop_nulls(["equipment_name", "equipment_hours", "sample_date"])
        .sort("equipment_name")
        .filter(~pl.col("is_microfiltered"))
        .sort(["equipment_name", "sample_date"])
        .select(["equipment_name", "sample_date", "equipment_hours"])
        # .drop_nulls()
        .with_columns(
            equipment_hours=pl.col("equipment_hours").cast(pl.Int64, strict=False),
            sample_date=pl.col("sample_date").cast(pl.Date),
        )
        .drop_nulls()
    )
    df = (
        MasterData.equipments()
        .filter(pl.col("equipment_model") == "960E")
        .select(["equipment_name", "equipment_model"])
        .join(
            df,
            how="left",
            on="equipment_name",
        )
    )

    df = clean_equipment_hours(df)
    df = create_daily_interpolated_df(df, end_date=date.today())
    df = df.with_columns(equipment_hours=pl.col("equipment_hours").fill_null(pl.col("interpolated_equipment_hours")))

    df = df.rename({"equipment_hours": "smr", "interpolated_equipment_hours": "interpolated_smr"})

    dl.upload_tibble(df, DATA_CATALOG["smr"]["analytics_path"])
    return df


def detect_outliers_by_regression(
    group_df: pl.DataFrame,
    outlier_threshold: float,
) -> pl.DataFrame:
    """Detect outliers using linear regression residuals"""

    # Convert dates to numeric (days since first date)
    min_date = group_df["sample_date"].min()
    group_df = group_df.with_columns([(pl.col("sample_date") - min_date).dt.total_days().alias("days_since_start")])

    # Handle null values in flags
    group_df = group_df.with_columns(
        [
            pl.col("flag_negative_progression").fill_null(False),
            pl.col("flag_excessive_rate").fill_null(False),
            pl.col("flag_low_rate").fill_null(False),
        ]
    )

    # Remove obvious outliers first (for better regression)
    initial_clean = group_df.filter(~(pl.col("flag_negative_progression")) & ~(pl.col("flag_excessive_rate")))

    if len(initial_clean) < 3:  # Need at least 3 points for meaningful regression
        return group_df.with_columns(pl.lit(True).alias("flag_outlier"))

    # Simple linear regression
    x = initial_clean["days_since_start"].to_numpy()
    y = initial_clean["equipment_hours"].to_numpy()

    # Calculate slope and intercept
    n = len(x)
    x_mean = np.mean(x)
    y_mean = np.mean(y)

    numerator = np.sum((x - x_mean) * (y - y_mean))
    denominator = np.sum((x - x_mean) ** 2)

    if denominator == 0:  # All x values are the same
        return group_df.with_columns(pl.lit(True).alias("flag_outlier"))

    slope = numerator / denominator
    intercept = y_mean - slope * x_mean

    # Calculate predicted values and residuals for all points
    all_x = group_df["days_since_start"].to_numpy()
    all_y = group_df["equipment_hours"].to_numpy()
    predicted = slope * all_x + intercept
    residuals = all_y - predicted

    # Calculate standard deviation of residuals
    flag_values = group_df["flag_negative_progression"].to_numpy()
    valid_residuals = residuals[~flag_values]

    if len(valid_residuals) > 0:
        residual_std = np.std(valid_residuals)
    else:
        residual_std = np.std(residuals)

    # Flag outliers based on residuals
    outlier_flags = np.abs(residuals) > (outlier_threshold * residual_std)

    return group_df.with_columns(
        [
            pl.Series("predicted_hours", predicted),
            pl.Series("residual", residuals),
            pl.Series("flag_outlier", outlier_flags),
        ]
    )


def clean_equipment_hours(
    df: pl.DataFrame,
    equipment_name: str = None,
    max_monthly_hours: int = 720,  # 24 * 30
    expected_monthly_hours: int = 510,
    outlier_threshold: float = 2.0,
) -> pl.DataFrame:
    """
    Clean equipment hours data by removing invalid entries and outliers.

    Parameters:
    - df: Polars DataFrame with columns ['equipment_name', 'sample_date', 'equipment_hours']
    - equipment_name: Optional filter for specific equipment
    - max_monthly_hours: Maximum possible hours in a month (default 720)
    - expected_monthly_hours: Expected average monthly hours (default 510)
    - outlier_threshold: Standard deviations for outlier detection
    """

    # Filter by equipment if specified
    if equipment_name:
        df = df.filter(pl.col("equipment_name") == equipment_name)

    # Step 1: Remove duplicates (keep first occurrence)
    df = df.unique(subset=["equipment_name", "sample_date"], keep="first")

    # Convert equipment_hours to numeric (handle non-numeric values)

    # Remove rows where we couldn't extract numeric hours
    df = df.drop_nulls(subset=["equipment_hours"])

    # Step 3: Sort by equipment and date
    df = df.sort(["equipment_name", "sample_date"])

    # Step 4: Calculate time differences and hour differences
    df = df.with_columns(
        days_diff=(pl.col("sample_date").diff().over("equipment_name")).dt.total_days(),
        hours_diff=pl.col("equipment_hours").diff().over("equipment_name"),
    )

    # Step 5: Calculate hourly rate (hours per day)
    df = df.with_columns(
        [
            pl.when(pl.col("days_diff").is_not_null() & (pl.col("days_diff") > 0))
            .then(pl.col("hours_diff") / pl.col("days_diff"))
            .otherwise(None)
            .alias("hourly_rate")
        ]
    )

    # Step 6: Flag invalid entries
    df = df.with_columns(
        [
            # Flag: Negative or zero progression
            pl.when(pl.col("hours_diff").is_not_null())
            .then(pl.col("hours_diff") <= 0)
            .otherwise(False)
            .alias("flag_negative_progression"),
            # Flag: Excessive hourly rate (more than 24 hours/day)
            pl.when(pl.col("hourly_rate").is_not_null())
            .then(pl.col("hourly_rate") > 24)
            .otherwise(False)
            .alias("flag_excessive_rate"),
            # Flag: Too low hourly rate (less than 10 hours/day as example)
            pl.when(pl.col("hourly_rate").is_not_null())
            .then(pl.col("hourly_rate") < 10)
            .otherwise(False)
            .alias("flag_low_rate"),
        ]
    )

    # Apply outlier detection by equipment
    df = df.group_by("equipment_name").map_groups(lambda group: detect_outliers_by_regression(group, outlier_threshold))

    # Step 8: Create a validity score
    df = df.with_columns(
        [
            (
                pl.col("flag_negative_progression").cast(pl.Int8)
                + pl.col("flag_excessive_rate").cast(pl.Int8)
                + pl.col("flag_low_rate").cast(pl.Int8)
                + pl.col("flag_outlier").cast(pl.Int8)
            ).alias("invalidity_score")
        ]
    )

    # Step 9: Mark valid entries
    df = df.with_columns([(pl.col("invalidity_score") == 0).alias("is_valid")])

    return df


def create_daily_interpolated_df(cleaned_df: pl.DataFrame, end_date: date = None) -> pl.DataFrame:
    """
    Create a daily dataframe with interpolated equipment hours.
    Now includes forward extrapolation based on the trend.
    """

    if end_date is None:
        end_date = date.today()

    # Get only valid data points
    valid_df = cleaned_df.filter(pl.col("is_valid")).select(["equipment_name", "sample_date", "equipment_hours"])

    # Get the regression slope from cleaned_df (better than average)
    regression_info = (
        cleaned_df.filter(pl.col("is_valid"))
        .group_by("equipment_name")
        .agg(
            [
                # Get the regression slope (hours per day) from predicted values
                (
                    (pl.col("predicted_hours").last() - pl.col("predicted_hours").first())
                    / (pl.col("sample_date").max() - pl.col("sample_date").min()).dt.total_days()
                ).alias("daily_rate")
            ]
        )
    )

    # Get first and last valid info for each equipment
    equipment_bounds = (
        valid_df.sort(["equipment_name", "sample_date"])
        .group_by("equipment_name")
        .agg(
            [
                pl.col("sample_date").first().alias("first_valid_date"),
                pl.col("equipment_hours").first().alias("first_valid_hours"),
                pl.col("sample_date").last().alias("last_valid_date"),
                pl.col("equipment_hours").last().alias("last_valid_hours"),
            ]
        )
    )

    # Get start_date for each equipment
    date_ranges = (
        valid_df.select(["equipment_name"])
        .unique()
        .with_columns(start_date=pl.lit(date(2019, 1, 1)), end_date=pl.lit(end_date))
    )

    # Create daily date range
    daily_dates = (
        date_ranges.with_columns(
            [
                pl.date_ranges(
                    start=pl.col("start_date"),
                    end=pl.col("end_date"),
                    interval="1d",
                    eager=False,
                ).alias("date_range")
            ]
        )
        .explode("date_range")
        .rename({"date_range": "smr_date"})
        .drop(["start_date", "end_date"])
    )

    # Join with actual data
    daily_df = daily_dates.join(
        valid_df.rename({"sample_date": "smr_date"}), on=["equipment_name", "smr_date"], how="left"
    )

    # Join regression info and bounds
    daily_df = daily_df.join(regression_info, on="equipment_name")
    daily_df = daily_df.join(equipment_bounds, on="equipment_name")

    # Sort by equipment and date
    daily_df = daily_df.sort(["equipment_name", "smr_date"])

    # First interpolate between known values
    daily_df = daily_df.with_columns(
        [pl.col("equipment_hours").interpolate().over("equipment_name").alias("interpolated_equipment_hours")]
    )

    # Handle backward and forward extrapolation
    daily_df = daily_df.with_columns(
        [
            pl.when(pl.col("smr_date") < pl.col("first_valid_date"))
            # Backward extrapolation
            .then(
                pl.col("first_valid_hours")
                - (pl.col("first_valid_date") - pl.col("smr_date")).dt.total_days() * pl.col("daily_rate")
            )
            .when(pl.col("interpolated_equipment_hours").is_null())
            # Forward extrapolation
            .then(
                pl.col("last_valid_hours")
                + (pl.col("smr_date") - pl.col("last_valid_date")).dt.total_days() * pl.col("daily_rate")
            )
            .otherwise(pl.col("interpolated_equipment_hours"))
            .alias("interpolated_equipment_hours")
        ]
    )

    # Clean up and add metadata
    daily_df = daily_df.select(
        [
            "equipment_name",
            "smr_date",
            "equipment_hours",  # Original value (null if interpolated/extrapolated)
            "interpolated_equipment_hours",  # Always has a value
            pl.col("equipment_hours").is_not_null().alias("is_actual_data"),
            pl.col("interpolated_equipment_hours").diff().over("equipment_name").alias("daily_hours_increment"),
        ]
    )

    return daily_df
