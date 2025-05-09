from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time
import re
import json
from pathlib import Path
from urllib.parse import urlparse
from selenium.common.exceptions import (
    NoSuchElementException,
)  # Keep this import for potential future use or other parts of your code
from selenium.webdriver.support.ui import WebDriverWait

from ..reso.web_driver import initialize_driver, login_to_reso, DEFAULT_WAIT, retry_on_interception
from ..reso.main_navigation import click_presupuesto

from ..reso.presupuesto_navigation import (
    search_service_order,
    click_see_service_order,
    navigate_to_quotation_tab,
    navigate_to_documents_tab,
    close_service_order_view,
)
from ..reso.so_utils import extract_quotation_details, extract_document_links, has_quotation, has_documents

# from .service_order_harvester import
import polars as pl
import sys
from datetime import datetime
import dagster as dg
from kdags.resources.tidyr import DataLake

# Assume these imports exist elsewhere or need to be added
# from selenium.webdriver.remote.webdriver import WebDriver
# from selenium.webdriver.support.wait import WebDriverWait
# from your_selenium_utils import (
#     search_service_order, click_see_service_order, navigate_to_quotation_tab,
#     extract_quotation_details, navigate_to_documents_tab, extract_document_links,
#     close_service_order_view
# )

# --- Constants ---
QUOTATIONS_RAW_PATH = "az://bhp-raw-data/RESO/SERVICE_ORDER_DETAILS/quotations.parquet"
DOCUMENTS_LIST_RAW_PATH = f"az://bhp-raw-data/RESO/SERVICE_ORDER_DETAILS/documents_list.parquet"

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
}
DOCUMENTS_LIST_SCHEMA = {
    "service_order": pl.Int64,
    "file_name": pl.Utf8,
    "file_title": pl.Utf8,
    "file_subtitle": pl.Utf8,
    "update_timestamp": pl.Datetime(time_unit="us"),
}

# --- Helper Functions ---


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

    dl.upload_tibble(quotations_df, QUOTATIONS_RAW_PATH)
    dl.upload_tibble(documents_list_df, DOCUMENTS_LIST_RAW_PATH)

    return quotations_df, documents_list_df


def run_service_orders_extraction(driver, wait: WebDriverWait, service_orders: list, update_mode="skip"):

    if update_mode not in ["skip", "overwrite"]:
        raise ValueError("update_mode must be 'skip' or 'overwrite'")

    # --- Validate/Convert Input Service Orders ---

    dl = DataLake()
    quotations_df = dl.read_tibble(QUOTATIONS_RAW_PATH)
    documents_list_df = dl.read_tibble(DOCUMENTS_LIST_RAW_PATH)

    # --- Main Extraction Loop ---
    batch_quotations = []
    batch_documents = []
    processed_sos_in_batch = set()
    total_orders = len(service_orders)
    print(f"--- Starting Data Extraction for {total_orders} Service Orders ---")

    for i, service_order in enumerate(service_orders):
        progress_message = f"Processing SO {i+1}/{total_orders}: {service_order}"
        print(progress_message, end="\r")

        # --- Process Single Service Order ---
        now = datetime.now()
        quotation_data_extracted = None
        document_data_extracted = []

        search_service_order(wait, service_order)
        click_see_service_order(wait)

        # --- Quotation Extraction ---
        try:
            navigate_to_quotation_tab(driver, wait)
            quotation_data_extracted = extract_quotation_details(driver, wait, service_order)
        except Exception as e_quot:
            print(f"\nWarning: Error during quotation extraction for SO {service_order}: {e_quot}")
            # Continue to document extraction, default quotation will be added later

        # --- Document Extraction ---
        try:
            navigate_to_documents_tab(wait)
            document_data_extracted = extract_document_links(driver, wait)  # Should return a list of dicts
        except Exception as e_doc:
            print(f"\nWarning: Error during document extraction for SO {service_order}: {e_doc}")
            # Continue, default document record might be added later if needed

        if quotation_data_extracted:
            # Remove transient keys like download_url if present
            quotation_data_extracted.pop("download_url", None)
            processed_records = ensure_schema_and_defaults(
                [quotation_data_extracted], QUOTATION_SCHEMA, service_order, now
            )
            batch_quotations.extend(processed_records)

        else:
            # Add a default quotation record if none was extracted or if a major error occurred
            batch_quotations.append(create_default_record(QUOTATION_SCHEMA, service_order, now))

        if document_data_extracted:  # Check if list is not empty
            # Remove transient keys like url if present
            for doc in document_data_extracted:
                doc.pop("url", None)
            processed_records = ensure_schema_and_defaults(
                document_data_extracted, DOCUMENTS_LIST_SCHEMA, service_order, now
            )
            batch_documents.extend(processed_records)

        else:
            # Add a single default document record if none were extracted or major error
            batch_documents.append(create_default_record(DOCUMENTS_LIST_SCHEMA, service_order, now))

        processed_sos_in_batch.add(service_order)  # Add SO to batch set regardless of success/failure

        # --- Cleanup: Close Selenium View ---
        close_service_order_view(wait)

        # --- Batch Processing Trigger ---
        is_last_item = i == total_orders - 1
        if len(processed_sos_in_batch) >= BATCH_SIZE or (is_last_item and processed_sos_in_batch):
            print(
                f"\nProcessing and saving batch of {len(processed_sos_in_batch)} service orders to ADLS (ending with SO {service_order})..."
            )
            quotations_df, documents_list_df = process_and_save_batch(
                batch_quotations,
                batch_documents,
                quotations_df,
                documents_list_df,
                processed_sos_in_batch,
            )
            # Clear batches only after successful save
            batch_quotations = []
            batch_documents = []
            processed_sos_in_batch = set()

        print(" " * (len(progress_message) + 5), end="\r")  # Clear last progress line
    print("\n--- Data Extraction Complete ---")
    print(f"Final Quotations DF shape: {quotations_df.shape}")
    print(f"Final Documents DF shape: {documents_list_df.shape}")

    return quotations_df, documents_list_df


# @dg.asset
# def scrape_service_orders(context: dg.AssetExecutionContext, read_component_reparations: pl.DataFrame) -> dict:
#     # --- Initialize service orders to harvest
#     cr_df = read_component_reparations.clone()
#     service_orders_df = cr_df.filter(pl.col("service_order") != -1).select(["service_order"]).unique()
#     dl = DataLake()
#     quotations_df = dl.read_tibble(QUOTATIONS_RAW_PATH)
#     quotations_df = service_orders_df.join(quotations_df, on=["service_order"], how="outer")
#     dl.upload_tibble(quotations_df, az_path=QUOTATIONS_RAW_PATH)
#
#     documents_list_df = dl.read_tibble(DOCUMENTS_LIST_RAW_PATH)
#     documents_list_df = service_orders_df.join(documents_list_df, on=["service_order"], how="outer")
#     dl.upload_tibble(documents_list_df, az_path=DOCUMENTS_LIST_RAW_PATH)
#
#     quot_sos = set(quotations_df.drop_nulls(subset=["update_timestamp"])["service_order"].to_list())
#     doc_sos = set(documents_list_df.drop_nulls(subset=["update_timestamp"])["service_order"].to_list())
#     service_orders = list(quot_sos.union(doc_sos))  # service_orders to update
#
#     # --- Initialize Driver ---
#     driver = initialize_driver()
#     wait = WebDriverWait(driver, DEFAULT_WAIT)
#     context.log.info("WebDriver initialized.")
#
#     # --- Login ---
#     login_to_reso(driver, wait)
#     context.log.info("Login RESO+ successful.")
#     click_presupuesto(driver, wait)
#
#     run_service_orders_extraction(
#         driver=driver,
#         wait=wait,
#         service_orders=service_orders,
#         update_mode="skip",
#     )
#
#     return summary_data
