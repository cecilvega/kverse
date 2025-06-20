import polars as pl

from kdags.config import DATA_CATALOG
from kdags.resources.tidyr import DataLake

BATCH_SIZE = 5  # Adjust as needed

# Schemas remain the same
QUOTATION_SCHEMA = {
    "service_order": pl.Int64,
    "version": pl.Int64,
    "purchase_order_status": pl.Utf8,
    "date_time_str": pl.Utf8,
    "user": pl.Utf8,
    "amount": pl.Utf8,
    "remarks": pl.Utf8,
    "update_timestamp": pl.Datetime(time_unit="us"),
    "download_url": pl.Utf8,
}
DOCUMENTS_LIST_SCHEMA = {
    "service_order": pl.Int64,
    "file_name": pl.Utf8,
    "file_title": pl.Utf8,
    "file_subtitle": pl.Utf8,
    "update_timestamp": pl.Datetime(time_unit="us"),
}


def ensure_schema_and_defaults(data_list, schema, service_order, timestamp):
    """Ensures records have all schema keys and converts basic types."""
    processed_list = []
    for record in data_list:
        # Add missing keys with None default
        for key in schema.keys():
            record.setdefault(key, None)
        # Ensure service_order and timestamp are set
        record["service_order"] = service_order  # Already int
        record["update_timestamp"] = timestamp
        # Basic Type handling (can be enhanced)
        for key, polars_type in schema.items():
            if key in record and record[key] is not None:
                try:
                    current_value = record[key]
                    if polars_type == pl.Int64 and not isinstance(current_value, int):
                        record[key] = int(current_value)
                    elif polars_type == pl.Utf8 and not isinstance(current_value, str):
                        record[key] = str(current_value)
                    # Add more type checks/conversions as needed (e.g., float, date)
                except (ValueError, TypeError):
                    print(
                        f"Warning: Could not convert '{key}' value '{record[key]}' for SO {service_order}. Setting to None."
                    )
                    record[key] = None  # Set to None on conversion error
        processed_list.append(record)
    return processed_list


def create_default_record(schema, service_order, timestamp):
    """Creates a default record with None values for a given schema."""
    default_data = {key: None for key in schema.keys()}
    default_data["service_order"] = service_order
    default_data["update_timestamp"] = timestamp
    return default_data


def process_and_save_batch(
    batch_quotations,
    batch_documents,
    quotations_df: pl.DataFrame,
    documents_list_df: pl.DataFrame,
    processed_sos_in_batch,
):
    dl = DataLake()

    # Schemas remain the same
    quotation_schema_with_ts = QUOTATION_SCHEMA.copy()
    document_schema_with_ts = DOCUMENTS_LIST_SCHEMA.copy()
    quotation_cols_order = list(quotation_schema_with_ts.keys())
    document_cols_order = list(document_schema_with_ts.keys())

    # Create DataFrames from batch data
    new_quotations_df = (
        pl.DataFrame(batch_quotations, schema_overrides=quotation_schema_with_ts)
        if batch_quotations
        else pl.DataFrame(schema=quotation_schema_with_ts)
    )
    new_documents_df = (
        pl.DataFrame(batch_documents, schema_overrides=document_schema_with_ts)
        if batch_documents
        else pl.DataFrame(schema=document_schema_with_ts)
    )

    # Ensure main DFs have correct columns before concatenation
    if not quotations_df.is_empty():
        # Ensure all expected columns exist, adding missing ones as null
        for col, dtype in quotation_schema_with_ts.items():
            if col not in quotations_df.columns:
                quotations_df = quotations_df.with_columns(pl.lit(None).cast(dtype).alias(col))
        quotations_df = quotations_df.select(quotation_cols_order)  # Reorder
    elif not new_quotations_df.is_empty():
        # Start with an empty DF with the correct schema if main DF was empty but new data exists
        quotations_df = pl.DataFrame(schema=quotation_schema_with_ts).select(quotation_cols_order)

    if not documents_list_df.is_empty():
        # Ensure all expected columns exist, adding missing ones as null
        for col, dtype in document_schema_with_ts.items():
            if col not in documents_list_df.columns:
                documents_list_df = documents_list_df.with_columns(pl.lit(None).cast(dtype).alias(col))
        documents_list_df = documents_list_df.select(document_cols_order)  # Reorder
    elif not new_documents_df.is_empty():
        documents_list_df = pl.DataFrame(schema=document_schema_with_ts).select(document_cols_order)

    # Handle 'overwrite' mode: remove existing SOs from the main DF that are in this batch
    so_list = list(processed_sos_in_batch)
    if not quotations_df.is_empty():
        quotations_df = quotations_df.filter(~pl.col("service_order").is_in(so_list))
    if not documents_list_df.is_empty():
        documents_list_df = documents_list_df.filter(~pl.col("service_order").is_in(so_list))

    # Concatenate new data
    # Use how="diagonal" if schemas might slightly differ (safer) or vertical_relaxed
    if not new_quotations_df.is_empty():
        quotations_df = pl.concat([quotations_df, new_quotations_df], how="diagonal")  # Or vertical_relaxed
    if not new_documents_df.is_empty():
        documents_list_df = pl.concat([documents_list_df, new_documents_df], how="diagonal")  # Or vertical_relaxed

    # Apply uniqueness constraints
    if not quotations_df.is_empty():
        quotations_df = quotations_df.unique(subset=["service_order"], keep="last")
    if not documents_list_df.is_empty():
        documents_list_df = documents_list_df.unique(subset=["service_order", "file_name"], keep="last")

    dl.upload_tibble(quotations_df, DATA_CATALOG["so_quotations"]["raw_path"])
    dl.upload_tibble(documents_list_df, DATA_CATALOG["so_documents"]["raw_path"])

    return quotations_df, documents_list_df
