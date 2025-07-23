import polars as pl
from typing import Optional, List


def apply_manual_overrides(
    pivot_parts_df: pl.DataFrame,
    overrides_df: pl.DataFrame,
    key_cols: Optional[List[str]] = None,
) -> pl.DataFrame:
    """
    Applies manual overrides to the pivot_parts DataFrame with flexible key matching.

    Args:
        pivot_parts_df: Original DataFrame
        overrides_df: DataFrame with override values
        key_cols: List of columns to use as keys. If None, uses all available key columns
                 from the overrides_df that exist in the original DataFrame

    Returns:
        Updated DataFrame with overrides applied
    """

    # Default key columns
    all_key_cols = ["component_serial", "service_order", "subpart_name", "part_name"]

    if key_cols is None:
        # Use only the key columns that are present in overrides_df and not null
        key_cols = []
        for col in all_key_cols:
            if col in overrides_df.columns:
                # Check if the column has any non-null values
                if overrides_df[col].null_count() < len(overrides_df):
                    key_cols.append(col)

    # Ensure all columns from the original df are present in the overrides df
    for col in pivot_parts_df.columns:
        if col not in overrides_df.columns:
            overrides_df = overrides_df.with_columns(pl.lit(None).alias(col))

    # Ensure data types match for join keys
    for col in key_cols:
        if col in pivot_parts_df.columns and col in overrides_df.columns:
            if pivot_parts_df[col].dtype != overrides_df[col].dtype:
                overrides_df = overrides_df.with_columns(overrides_df[col].cast(pivot_parts_df[col].dtype))

    # Remove rows from original DataFrame that match the override keys
    df_without_overridden_rows = pivot_parts_df.join(overrides_df.select(key_cols), on=key_cols, how="anti")

    # For each override record, we need to merge it with existing data
    # This handles the case where you only specify some fields
    updated_records = []

    for override_row in overrides_df.iter_rows(named=True):
        # Create filter conditions for matching
        filter_conditions = []
        for col in key_cols:
            if override_row[col] is not None:
                filter_conditions.append(pl.col(col) == override_row[col])

        if filter_conditions:
            # Find matching rows in original data
            matching_rows = pivot_parts_df.filter(pl.all_horizontal(filter_conditions))

            if len(matching_rows) > 0:
                # Update each matching row with override values
                for orig_row in matching_rows.iter_rows(named=True):
                    updated_row = orig_row.copy()
                    # Override only the non-null values from override_row
                    for col, value in override_row.items():
                        if value is not None:
                            updated_row[col] = value
                    updated_records.append(updated_row)
            else:
                # If no matching rows found, add the override as a new record
                updated_records.append(override_row)

    # Convert updated records back to DataFrame
    if updated_records:
        overrides_updated_df = pl.DataFrame(updated_records)
        # Ensure column order matches original
        overrides_updated_df = overrides_updated_df.select(pivot_parts_df.columns)
    else:
        overrides_updated_df = pl.DataFrame(schema=pivot_parts_df.schema)

    # Concatenate the original data (minus replaced rows) with updated override data
    df_updated = pl.concat([df_without_overridden_rows, overrides_updated_df])

    # context.log.info(f"Manual overrides applied using keys: {key_cols}")
    return df_updated
