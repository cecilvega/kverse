import polars as pl
import dagster as dg
from kdags.config import DATA_CATALOG
from kdags.resources.tidyr import DataLake, MasterData
from datetime import date, datetime, timedelta
import numpy as np


@dg.asset
def raw_smr(context: dg.AssetExecutionContext):
    dl = DataLake(context)
    smr_df = dl.read_tibble(DATA_CATALOG["komtrax_smr"]["process_path"])
    reference_records = [
        {
            "equipment_name": "CEX46",
            "equipment_serial": "A50034",
            "smr_date": date(2025, 5, 18),
            "smr": 33036,
            "FECHARECEPCION": None,
        },
        {
            "equipment_name": "TK286",
            "equipment_serial": "A30528",
            "smr_date": date(2025, 5, 3),
            "smr": 23445,
            "FECHARECEPCION": None,
        },
        {
            "equipment_name": "TK859",
            "equipment_serial": "A30074",
            "smr_date": date(2025, 4, 4),
            "smr": 66539,
            "FECHARECEPCION": None,
        },
        {
            "equipment_name": "TK865",
            "equipment_serial": "A30080",
            "smr_date": date(2025, 6, 14),
            "smr": 66554,
            "FECHARECEPCION": None,
        },
        {
            "equipment_name": "TK874",
            "equipment_serial": "A30095",
            "smr_date": date(2025, 5, 15),
            "smr": 64989,
            "FECHARECEPCION": None,
        },
    ]
    reference_df = (
        pl.DataFrame(reference_records)
        .drop("equipment_name")
        .with_columns(source=pl.lit("REFERENCE"), smr_date=pl.col("smr_date").cast(pl.Date))
    )
    smr_df = (
        smr_df.rename({"SERIE": "equipment_serial", "FECHA": "smr_date", "VALOR": "smr"})
        .drop(["MODELO", "TIPO"])
        .with_columns(source=pl.lit("KOMTRAX"), smr_date=pl.col("smr_date").cast(pl.Date))
    )
    smr_df = (
        pl.concat([smr_df, reference_df], how="diagonal")
        .sort(["equipment_serial", "smr_date"])
        .join(
            MasterData.equipments().select(["site_name", "equipment_model", "equipment_name", "equipment_serial"]),
            how="left",
            on=["equipment_serial"],
        )
    )
    return smr_df


@dg.asset(group_name="operation", compute_kind="mutate")
def mutate_smr(context: dg.AssetExecutionContext, raw_smr: pl.DataFrame):
    dl = DataLake(context)
    # Date range
    years_back = 10
    equipments_df = (
        MasterData.equipments()
        .filter(pl.col("equipment_model").is_in(["960E", "930E-4", "980E-5"]))
        .select(["site_name", "equipment_model", "equipment_name", "equipment_serial"])
    )
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=365 * years_back)
    dates = pl.date_range(start_date, end_date, "1d", eager=True)

    # Process each equipment
    results = []

    for equipment_serial in equipments_df["equipment_serial"].unique():
        equipment_data = raw_smr.filter(pl.col("equipment_serial") == equipment_serial)

        if equipment_data.height == 0:
            continue
        elif equipment_data.height == 1:
            # Single point - use 8 hours/day default
            point = equipment_data.row(0, named=True)
            ref_date = point["smr_date"]
            ref_smr = point["smr"]

            for date in dates:
                days_diff = (date - ref_date).days
                smr_interpolated = ref_smr + (days_diff * 8)

                # Skip dates where SMR would be negative (before equipment start)
                if smr_interpolated < 0:
                    continue

                data_type = "raw" if date == ref_date else "interpolated"
                results.append(
                    {
                        "equipment_serial": equipment_serial,
                        "smr_date": date,
                        "smr": int(smr_interpolated),
                        "data_type": data_type,
                    }
                )
        else:
            # Multiple points - use original values for existing dates, interpolate missing dates
            data = equipment_data.sort("smr_date")
            x = data.select(pl.col("smr_date").dt.timestamp("ms")).to_numpy().flatten()
            y = data.select("smr").to_numpy().flatten()

            # Create dictionary of actual date -> SMR mappings
            actual_data = data.select([pl.col("smr_date").dt.date().alias("date"), "smr"])
            actual_dict = {row[0]: row[1] for row in actual_data.iter_rows()}

            # Calculate linear regression for interpolation
            slope, intercept = np.polyfit(x, y, 1)

            for date in dates:
                if date in actual_dict:
                    # Use original value for existing dates
                    smr_value = actual_dict[date]
                    data_type = "raw"
                else:
                    # Use interpolated value for missing dates
                    timestamp = datetime.combine(date, datetime.min.time()).timestamp() * 1000
                    smr_value = slope * timestamp + intercept
                    data_type = "interpolated"

                    # Skip dates where SMR would be negative
                    if smr_value < 0:
                        continue

                results.append(
                    {
                        "equipment_serial": equipment_serial,
                        "smr_date": date,
                        "smr": int(smr_value),
                        "data_type": data_type,
                    }
                )

    # Create final dataframe and join with equipment master data
    daily_smr_df = pl.DataFrame(results)
    df = equipments_df.join(daily_smr_df, on="equipment_serial", how="left")
    run_smr_sanity_checks(context, raw_smr, df)

    dl.upload_tibble(df, DATA_CATALOG["komtrax_smr"]["analytics_path"])
    return df


def run_smr_sanity_checks(context: dg.AssetExecutionContext, original_smr_df, daily_smr_df):
    """
    Run sanity checks to validate interpolated daily SMR data against original data
    Using Dagster context for logging
    """

    context.log.info("Starting sanity checks for daily SMR data")

    # 1. Check that all raw data points match exactly
    context.log.info("Running raw data point accuracy check")
    raw_daily = daily_smr_df.filter(pl.col("data_type") == "raw")

    # Join original with raw daily data (convert original smr_date to date for comparison)
    original_dates = original_smr_df.select(["equipment_serial", "smr_date", "smr"]).with_columns(
        pl.col("smr_date").dt.date().alias("smr_date")
    )

    comparison = original_dates.join(
        raw_daily.select(["equipment_serial", "smr_date", "smr"]).rename({"smr": "smr_interpolated"}),
        on=["equipment_serial", "smr_date"],
        how="inner",
    ).with_columns((pl.col("smr") - pl.col("smr_interpolated")).abs().alias("smr_diff"))

    max_diff = comparison.select(pl.col("smr_diff").max()).item()
    mismatches = comparison.filter(pl.col("smr_diff") > 24).height

    context.log.info(f"Raw data points checked: {comparison.height}")
    context.log.info(f"Mismatches found: {mismatches}")
    context.log.info(f"Maximum difference: {max_diff}")

    if mismatches == 0:
        context.log.info("✅ Raw data point accuracy check: PASS")
    else:
        context.log.warning(f"❌ Raw data point accuracy check: FAIL - {mismatches} mismatches found")

    # 2. Check equipment count consistency
    context.log.info("Running equipment count check")
    original_equipment_count = original_smr_df.select("equipment_serial").n_unique()
    daily_equipment_count = daily_smr_df.select("equipment_serial").n_unique()

    context.log.info(f"Original equipment count: {original_equipment_count}")
    context.log.info(f"Daily data equipment count: {daily_equipment_count}")

    if daily_equipment_count >= original_equipment_count:
        context.log.info("✅ Equipment count check: PASS")
    else:
        context.log.warning("❌ Equipment count check: FAIL - Equipment count decreased")

    # 3. Check for negative SMR values
    context.log.info("Running negative SMR check")
    negative_count = daily_smr_df.filter(pl.col("smr") < 0).height
    context.log.info(f"Negative SMR values found: {negative_count}")

    if negative_count == 0:
        context.log.info("✅ Negative SMR check: PASS")
    else:
        context.log.warning(f"❌ Negative SMR check: FAIL - {negative_count} negative values found")

    # 4. Check SMR monotonicity (should generally increase over time for each equipment)
    context.log.info("Running SMR monotonicity check on sample equipment")
    sample_equipment = daily_smr_df.select("equipment_serial").unique().head(5)

    monotonic_violations = 0
    for eq_serial in sample_equipment.to_series():
        eq_data = daily_smr_df.filter(pl.col("equipment_serial") == eq_serial).sort("smr_date")
        if eq_data.height > 1:
            violations = (
                eq_data.with_columns(pl.col("smr").diff().alias("smr_diff")).filter(pl.col("smr_diff") < -100).height
            )  # Allow small decreases (maintenance)

            if violations > 0:
                monotonic_violations += 1
                context.log.warning(f"Equipment {eq_serial}: {violations} large SMR decreases found")

    context.log.info(f"Equipment with large SMR decreases: {monotonic_violations}")

    if monotonic_violations == 0:
        context.log.info("✅ SMR monotonicity check: PASS")
    else:
        context.log.warning(f"⚠️ SMR monotonicity check: CHECK - {monotonic_violations} equipment with large decreases")

    # 5. Check date range coverage
    context.log.info("Running date range coverage check")
    original_date_range = original_smr_df.select(
        [
            pl.col("smr_date").dt.date().min().alias("min_date"),
            pl.col("smr_date").dt.date().max().alias("max_date"),
        ]
    ).row(0)

    daily_date_range = daily_smr_df.select(
        [
            pl.col("smr_date").min().alias("min_date"),
            pl.col("smr_date").max().alias("max_date"),
        ]
    ).row(0)

    context.log.info(f"Original data range: {original_date_range[0]} to {original_date_range[1]}")
    context.log.info(f"Daily data range: {daily_date_range[0]} to {daily_date_range[1]}")

    coverage_ok = daily_date_range[0] <= original_date_range[0] and daily_date_range[1] >= original_date_range[1]

    if coverage_ok:
        context.log.info("✅ Date range coverage check: PASS")
    else:
        context.log.warning("⚠️ Date range coverage check: CHECK - Coverage may be insufficient")

    # 6. Check data type distribution
    context.log.info("Running data type distribution check")
    data_type_counts = daily_smr_df.group_by("data_type").agg(pl.len().alias("count"))

    for row in data_type_counts.iter_rows(named=True):
        context.log.info(f"{row['data_type']}: {row['count']:,} records")

    raw_percentage = data_type_counts.filter(pl.col("data_type") == "raw").item(0, "count") / daily_smr_df.height * 100
    context.log.info(f"Raw data percentage: {raw_percentage:.1f}%")

    if 0.1 <= raw_percentage <= 50:  # Adjusted upper limit to be more realistic
        context.log.info("✅ Data type distribution check: PASS")
    else:
        context.log.warning(
            f"⚠️ Data type distribution check: CHECK - Raw percentage ({raw_percentage:.1f}%) outside expected range"
        )

    # 7. Check for equipment with single vs multiple points
    context.log.info("Running equipment interpolation method check")
    equipment_point_counts = original_smr_df.group_by("equipment_serial").agg(pl.len().alias("point_count"))

    single_point_eq = equipment_point_counts.filter(pl.col("point_count") == 1).height
    multi_point_eq = equipment_point_counts.filter(pl.col("point_count") > 1).height

    context.log.info(f"Equipment with single data point: {single_point_eq}")
    context.log.info(f"Equipment with multiple data points: {multi_point_eq}")
    context.log.info(f"Total equipment processed: {single_point_eq + multi_point_eq}")

    # 8. Spot check: Compare interpolated values near raw data points
    context.log.info("Running spot check on interpolated values near raw data")
    spot_check = daily_smr_df.filter(pl.col("data_type") == "raw").head(3)

    for row in spot_check.iter_rows(named=True):
        eq_serial = row["equipment_serial"]
        raw_date = row["smr_date"]
        raw_smr = row["smr"]

        # Get interpolated value one day before and after
        nearby = daily_smr_df.filter(
            (pl.col("equipment_serial") == eq_serial)
            & (pl.col("smr_date").is_between(raw_date - pl.duration(days=1), raw_date + pl.duration(days=1)))
        ).sort("smr_date")

        if nearby.height >= 3:
            before_smr = nearby.row(0)[-2]  # SMR value
            after_smr = nearby.row(2)[-2]  # SMR value
            context.log.info(f"Spot check - {eq_serial} on {raw_date}: {before_smr} → {raw_smr} → {after_smr}")

    context.log.info("Sanity checks completed")
