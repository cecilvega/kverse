import polars as pl
import re


def get_documents(
    component_reparations: pl.DataFrame, so_documents: pl.DataFrame, subcomponent_tag: str, file_type: str
):
    cr_df = component_reparations.filter(pl.col("subcomponent_tag") == subcomponent_tag)
    so_docs_df = so_documents.drop_nulls("file_size")
    records_list = (
        cr_df.join(
            so_docs_df.filter(pl.col("file_type") == file_type)
            .with_columns(pl.col("az_path").str.split(".").list.last().alias("suffix"))
            .filter(pl.col("suffix").str.to_lowercase() == "pdf")
            # .select(["service_order", "component_serial", "az_path"])
            .unique(subset=["service_order", "component_serial"]),
            how="inner",
            on=["service_order", "component_serial"],
        )
        .sort("reception_date")
        .select(["service_order", "component_serial", "az_path"])
        .to_dicts()
    )
    return records_list


def clean_part_serial(df: pl.DataFrame, column_name: str = "part_serial") -> pl.DataFrame:
    """
    Clean serial numbers in a specified column by removing unwanted text.

    Args:
        df: Polars DataFrame
        column_name: Name of the column to clean (default: "serial_number")

    Returns:
        DataFrame with cleaned serial numbers
    """

    def clean_serial_number(serial_str):
        if serial_str is None:
            return None

        # Convert to string and strip whitespace
        serial = str(serial_str).strip()

        # Handle common "unreadable" cases
        if serial.lower() in ["ilegible.", "ilegible", "illegible", "nuevo", "new"]:
            return None

        # Remove newlines and extra whitespace
        serial = re.sub(r"\s+", " ", serial)

        # Extract serial number before parentheses or common suffixes
        pattern = r"^([A-Za-z0-9]+)(?:\s*\([^)]*\)|\s*NUEVO\.?|\s*NEW\.?)*"
        match = re.match(pattern, serial, re.IGNORECASE)

        if match:
            cleaned = match.group(1).lower()
            # Return None if it's just "nuevo" or similar
            if cleaned.lower() in ["nuevo", "new", "ilegible"]:
                return None
            return cleaned

        # If no pattern matches, return the original lowercased (unless it's a known invalid)
        if serial.lower() not in ["nuevo", "new", "ilegible", "ilegible."]:
            return serial.lower()

        return None

    # Check if column exists
    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' not found in DataFrame. Available columns: {df.columns}")

    # Apply cleaning to the specified column
    return df.with_columns(
        pl.col(column_name).map_elements(clean_serial_number, return_dtype=pl.Utf8).alias(column_name)
    )
