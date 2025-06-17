import polars as pl


def calculate_prorrata_sale(df):
    """
    Calculate prorrata_sale (Costo Prorrata) using vectorized Polars operations

    Formula:
    - If component_hours in range [mtbo, mtbo*1.05]: gfa_overhaul_rate * (1 + ((effective_hours - mtbo) / mtbo))
    - Otherwise: gfa_overhaul_rate * (effective_hours / mtbo)

    Args:
        df: DataFrame with required columns from schema

    Returns:
        DataFrame with updated 'prorrata_sale' column
    """

    result_df = df.with_columns(
        [
            # Calculate effective hours: M = MIN(component_hours, mtbo_105)
            pl.min_horizontal([pl.col("component_hours"), pl.col("mtbo") * 1.05]).alias("effective_hours"),
            # Apply prorrata formula
            pl.when(
                # Condition: effective_hours >= mtbo AND effective_hours <= mtbo*1.05
                (pl.min_horizontal([pl.col("component_hours"), pl.col("mtbo") * 1.05]) >= pl.col("mtbo"))
                & (pl.min_horizontal([pl.col("component_hours"), pl.col("mtbo") * 1.05]) <= pl.col("mtbo") * 1.05)
            )
            .then(
                # Case 1: Hours in normal range (100% to 105%)
                pl.col("gfa_overhaul_rate")
                * (
                    1
                    + (
                        (pl.min_horizontal([pl.col("component_hours"), pl.col("mtbo") * 1.05]) - pl.col("mtbo"))
                        / pl.col("mtbo")
                    )
                )
            )
            .otherwise(
                # Case 2: Hours outside normal range
                pl.col("gfa_overhaul_rate")
                * (pl.min_horizontal([pl.col("component_hours"), pl.col("mtbo") * 1.05]) / pl.col("mtbo"))
            )
            .fill_null(0.0)
            .round(0)
            .alias("prorrata_sale"),  # Final column name
        ]
    ).drop(
        ["effective_hours"]
    )  # Clean up intermediate column

    return result_df
