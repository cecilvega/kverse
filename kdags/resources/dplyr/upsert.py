import polars as pl
from typing import List


def upsert_tibbles(
    incoming_df: pl.DataFrame,
    consolidated_df: pl.DataFrame,
    key_columns: List[str],
) -> pl.DataFrame:
    """
    Performs an upsert operation (update + insert) between two dataframes.

    Args:
        incoming_df: DataFrame containing new or updated records
        consolidated_df: DataFrame containing existing records
        key_columns: List of column names that form the composite key for matching

    Returns:
        pl.DataFrame: Updated consolidated dataframe with upserted records
    """
    # Check that all key columns exist in both dataframes
    assert all(
        col in incoming_df.columns for col in key_columns
    ), f"Key columns {key_columns} not found in incoming dataframe"

    assert all(
        col in consolidated_df.columns for col in key_columns
    ), f"Key columns {key_columns} not found in consolidated dataframe"

    # If incoming dataframe is empty, just return the consolidated dataframe
    if incoming_df.shape[0] == 0:
        return consolidated_df

    # If consolidated dataframe is empty, just return the deduplicated incoming dataframe
    if consolidated_df.shape[0] == 0:
        return incoming_df.unique(subset=key_columns, keep="last")

    # Ensure incoming dataframe has no duplicates, keeping the last occurrence
    unique_incoming_df = incoming_df.unique(subset=key_columns, keep="last")

    # 1. Identify records to update (matching records between both dataframes)
    updates = unique_incoming_df.join(consolidated_df.select(key_columns), on=key_columns, how="inner")

    # 2. Identify new records to insert (records in incoming_df not in consolidated_df)
    inserts = unique_incoming_df.join(consolidated_df.select(key_columns), on=key_columns, how="anti")

    # 3. Remove outdated records from consolidated_df
    preserved = consolidated_df.join(unique_incoming_df.select(key_columns), on=key_columns, how="anti")

    # 4. Combine preserved records with updates and inserts
    result_df = pl.concat([preserved, updates, inserts])

    return result_df
