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

from .web_driver import initialize_driver, login_to_reso, DEFAULT_WAIT
from .main_navigation import click_presupuesto
import polars as pl
import sys
from datetime import datetime
import dagster as dg


# Assume these imports exist elsewhere or need to be added
# from selenium.webdriver.remote.webdriver import WebDriver
# from selenium.webdriver.support.wait import WebDriverWait
# from your_selenium_utils import (
#     search_service_order, click_see_service_order, navigate_to_quotation_tab,
#     extract_quotation_details, navigate_to_documents_tab, extract_document_links,
#     close_service_order_view
# )

# --- Constants ---
QUOTATIONS_RAW_PATH = f"az://bhp-raw-data/RESO/QUOTATIONS.parquet"
DOCUMENTS_LIST_RAW_PATH = f"az://bhp-raw-data/RESO/DOCUMENTS_LIST.parquet"

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
DOCUMENT_SCHEMA = {
    "service_order": pl.Int64,
    "file_name": pl.Utf8,
    "file_title": pl.Utf8,
    "file_subtitle": pl.Utf8,
    "update_timestamp": pl.Datetime(time_unit="us"),
}

# --- Helper Functions ---


def _ensure_schema_and_defaults(data_list, schema, service_order, timestamp):
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
                    # print(f"Warning: Could not convert '{key}' value '{record[key]}' for SO {service_order}. Setting to None.")
                    record[key] = None  # Set to None on conversion error
        processed_list.append(record)
    return processed_list


def _create_default_record(schema, service_order, timestamp):
    """Creates a default record with None values for a given schema."""
    default_data = {key: None for key in schema.keys()}
    default_data["service_order"] = service_order
    default_data["update_timestamp"] = timestamp
    return default_data


# --- Batch Processing Function (Updated for ADLS) ---
def process_and_save_batch_adls(
    batch_quotations,
    batch_documents,
    main_quotations_df,
    main_documents_df,
    update_mode,
    quotation_adls_path,  # ADLS path string
    document_adls_path,  # ADLS path string
    processed_sos_in_batch,
    storage_options=None,  # Optional: Pass Azure credentials if needed
):
    """Updates main DFs with batch data and saves to ADLS Parquet."""
    if not batch_quotations and not batch_documents:
        return main_quotations_df, main_documents_df

    # Schemas remain the same
    quotation_schema_with_ts = QUOTATION_SCHEMA.copy()
    document_schema_with_ts = DOCUMENT_SCHEMA.copy()
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

    # Ensure column order consistency
    if not new_quotations_df.is_empty():
        new_quotations_df = new_quotations_df.select(quotation_cols_order)
    if not new_documents_df.is_empty():
        new_documents_df = new_documents_df.select(document_cols_order)

    # Ensure main DFs have correct columns before concatenation
    if not main_quotations_df.is_empty():
        # Ensure all expected columns exist, adding missing ones as null
        for col, dtype in quotation_schema_with_ts.items():
            if col not in main_quotations_df.columns:
                main_quotations_df = main_quotations_df.with_columns(pl.lit(None).cast(dtype).alias(col))
        main_quotations_df = main_quotations_df.select(quotation_cols_order)  # Reorder
    elif not new_quotations_df.is_empty():
        # Start with an empty DF with the correct schema if main DF was empty but new data exists
        main_quotations_df = pl.DataFrame(schema=quotation_schema_with_ts).select(quotation_cols_order)

    if not main_documents_df.is_empty():
        # Ensure all expected columns exist, adding missing ones as null
        for col, dtype in document_schema_with_ts.items():
            if col not in main_documents_df.columns:
                main_documents_df = main_documents_df.with_columns(pl.lit(None).cast(dtype).alias(col))
        main_documents_df = main_documents_df.select(document_cols_order)  # Reorder
    elif not new_documents_df.is_empty():
        main_documents_df = pl.DataFrame(schema=document_schema_with_ts).select(document_cols_order)

    # Handle 'overwrite' mode: remove existing SOs from the main DF that are in this batch
    if update_mode == "overwrite" and processed_sos_in_batch:
        so_list = list(processed_sos_in_batch)
        if not main_quotations_df.is_empty():
            main_quotations_df = main_quotations_df.filter(~pl.col("service_order").is_in(so_list))
        if not main_documents_df.is_empty():
            main_documents_df = main_documents_df.filter(~pl.col("service_order").is_in(so_list))

    # Concatenate new data
    # Use how="diagonal" if schemas might slightly differ (safer) or vertical_relaxed
    if not new_quotations_df.is_empty():
        main_quotations_df = pl.concat([main_quotations_df, new_quotations_df], how="diagonal")  # Or vertical_relaxed
    if not new_documents_df.is_empty():
        main_documents_df = pl.concat([main_documents_df, new_documents_df], how="diagonal")  # Or vertical_relaxed

    # Apply uniqueness constraints
    if not main_quotations_df.is_empty():
        main_quotations_df = main_quotations_df.unique(
            subset=["service_order"], keep="last", maintain_order=False  # maintain_order=False for performance
        )
    if not main_documents_df.is_empty():
        main_documents_df = main_documents_df.unique(
            subset=["service_order", "file_name"], keep="last", maintain_order=False
        )

    # --- Save to ADLS ---
    try:
        # No need for mkdir with ADLS URIs
        if not main_quotations_df.is_empty():
            main_quotations_df.write_parquet(
                quotation_adls_path,
                use_pyarrow=True,  # Recommended for cloud storage
                pyarrow_options={"storage_options": storage_options} if storage_options else None,
            )
        else:
            # Consider deleting the file if the DF becomes empty, or just don't write.
            # For simplicity, we don't write if empty.
            print(f"Info: Quotations DataFrame is empty. Skipping write to {quotation_adls_path}")
            pass

        if not main_documents_df.is_empty():
            main_documents_df.write_parquet(
                document_adls_path,
                use_pyarrow=True,
                pyarrow_options={"storage_options": storage_options} if storage_options else None,
            )
        else:
            print(f"Info: Documents DataFrame is empty. Skipping write to {document_adls_path}")
            pass

    except Exception as e:
        # More specific error handling for ADLS might be needed (e.g., credential errors)
        print(
            f"\nCRITICAL: Failed to save batch data to ADLS! Path: {quotation_adls_path} / {document_adls_path}. Error: {e}"
        )
        # Depending on the error, you might want to retry or raise it

    return main_quotations_df, main_documents_df


# --- Main Processing Function (Refactored for ADLS) ---
def run_service_orders_extraction(
    driver,  # Selenium WebDriver instance
    wait,  # Selenium WebDriverWait instance
    service_orders_input,  # List of service orders (will be converted to int)
    update_mode="skip",  # 'skip' or 'overwrite'
    storage_options=None,  # Optional dict for ADLS credentials if not in env
    # Add adls_resource=None if using a custom resource object
):
    """
    Extracts data using Selenium, saves results in batches to ADLS Parquet.
    Handles 'skip'/'overwrite' logic and ensures input SOs are integers.

    Args:
        driver: Selenium WebDriver instance.
        wait: Selenium WebDriverWait instance.
        service_orders_input: List of service orders (strings or numbers).
        update_mode (str): 'skip' (default) or 'overwrite'.
        storage_options (dict, optional): Credentials for ADLS access passed to Polars/PyArrow.
                                         Example: {"account_name": "your_account", "account_key": "your_key"}
                                         or {"account_name": "your_account", "sas_token": "your_sas"}
                                         Leave as None if using environment variables or managed identity.
        # adls_resource: Your custom ADLS resource object, if applicable.
    """
    if update_mode not in ["skip", "overwrite"]:
        raise ValueError("update_mode must be 'skip' or 'overwrite'")

    # --- Validate/Convert Input Service Orders ---
    service_orders = []
    try:
        service_orders = [int(so) for so in service_orders_input]
        print(f"Successfully validated/converted {len(service_orders)} input service orders to integers.")
    except (ValueError, TypeError) as e:
        print(f"CRITICAL ERROR: Could not convert all input service orders to integers: {e}")
        print("Ensure 'service_orders_input' contains only numbers or strings convertible to numbers.")
        # Consider raising an error instead of sys.exit in a library function
        raise ValueError("Invalid service order input") from e
        # sys.exit(1) # Avoid sys.exit in reusable functions

    # --- Load existing data from ADLS ---

    try:
        # If using a custom resource: main_quotations_df = adls_resource.read_parquet(QUOTATION_FILE_ADLS, ...)
        main_quotations_df = pl.read_parquet(
            QUOTATIONS_RAW_PATH,
            use_pyarrow=True,
            pyarrow_options={"storage_options": storage_options} if storage_options else None,
        )
        # Ensure update_timestamp column exists and has the right type
        if "update_timestamp" not in main_quotations_df.columns:
            main_quotations_df = main_quotations_df.with_columns(
                pl.lit(None).cast(pl.Datetime(time_unit="us")).alias("update_timestamp")
            )
        else:
            main_quotations_df = main_quotations_df.with_columns(
                pl.col("update_timestamp").cast(pl.Datetime(time_unit="us"), strict=False)  # Cast existing
            )
        print(f"Loaded existing quotations from ADLS: {main_quotations_df.shape}")
    except Exception as e:  # Catch more specific exceptions if possible (FileNotFoundError, auth errors)
        print(f"Info: Quotations file not found in ADLS or error loading ({e}). Starting fresh.")
        main_quotations_df = pl.DataFrame(schema=QUOTATION_SCHEMA)

    try:
        # If using a custom resource: main_documents_df = adls_resource.read_parquet(DOCUMENTS_FILE_ADLS, ...)
        main_documents_df = pl.read_parquet(
            DOCUMENTS_LIST_RAW_PATH,
            use_pyarrow=True,
            pyarrow_options={"storage_options": storage_options} if storage_options else None,
        )
        # Ensure update_timestamp column exists and has the right type
        if "update_timestamp" not in main_documents_df.columns:
            main_documents_df = main_documents_df.with_columns(
                pl.lit(None).cast(pl.Datetime(time_unit="us")).alias("update_timestamp")
            )
        else:
            main_documents_df = main_documents_df.with_columns(
                pl.col("update_timestamp").cast(pl.Datetime(time_unit="us"), strict=False)  # Cast existing
            )
        print(f"Loaded existing documents from ADLS: {main_documents_df.shape}")
    except Exception as e:
        print(f"Info: Documents file not found in ADLS or error loading ({e}). Starting fresh.")
        main_documents_df = pl.DataFrame(schema=DOCUMENT_SCHEMA)

    # --- Prepare for 'skip' mode ---
    existing_sos = set()
    if update_mode == "skip":
        print("Update mode is 'skip'. Identifying existing service orders from loaded data...")
        quot_sos = set()
        doc_sos = set()
        if not main_quotations_df.is_empty() and "service_order" in main_quotations_df.columns:
            try:
                quot_sos = set(main_quotations_df["service_order"].drop_nulls().to_list())
                print(f"Found {len(quot_sos)} non-null SOs in loaded quotations.")
            except Exception as e:
                print(f"Warning: Could not extract service orders from quotations data: {e}")

        if not main_documents_df.is_empty() and "service_order" in main_documents_df.columns:
            try:
                doc_sos = set(main_documents_df["service_order"].drop_nulls().to_list())
                print(f"Found {len(doc_sos)} non-null SOs in loaded documents.")
            except Exception as e:
                print(f"Warning: Could not extract service orders from documents data: {e}")

        existing_sos = quot_sos.union(doc_sos)
        print(f"--> Total unique existing service orders identified for skipping: {len(existing_sos)}")

    # --- Main Extraction Loop ---
    batch_quotations = []
    batch_documents = []
    processed_sos_in_batch = set()
    total_orders = len(service_orders)
    print(f"--- Starting Data Extraction for {total_orders} Service Orders ---")
    skipped_count = 0

    for i, service_order in enumerate(service_orders):  # Use validated integer list
        progress_message = f"Processing SO {i+1}/{total_orders}: {service_order} (Skipped: {skipped_count})"
        print(progress_message, end="\r")

        if update_mode == "skip" and service_order in existing_sos:
            skipped_count += 1
            continue

        # --- Process Single Service Order ---
        now = datetime.now()
        quotation_data_extracted = None
        document_data_extracted = []
        process_error = False
        error_message = ""

        try:
            # --- Placeholder for Selenium Actions ---
            # Replace with your actual Selenium function calls
            # search_service_order(driver, wait, service_order)
            # click_see_service_order(wait)

            # --- Quotation Extraction ---
            try:
                # navigate_to_quotation_tab(driver, wait)
                # quotation_data_extracted = extract_quotation_details(driver, wait, service_order)
                # Mock data for testing without Selenium:
                # if service_order % 5 != 0: # Simulate finding data
                #     quotation_data_extracted = {
                #         "version": 1, "purchase_order_status": "Approved", "date_time_str": now.isoformat(),
                #         "user": "script", "amount": str(service_order * 100), "remarks": f"Remarks for {service_order}"
                #     }
                pass  # Remove pass when using real extraction

            except Exception as e_quot:
                print(f"\nWarning: Error during quotation extraction for SO {service_order}: {e_quot}")
                # Continue to document extraction, default quotation will be added later

            # --- Document Extraction ---
            try:
                # navigate_to_documents_tab(wait)
                # document_data_extracted = extract_document_links(driver, wait) # Should return a list of dicts
                # Mock data for testing without Selenium:
                # if service_order % 3 != 0: # Simulate finding docs
                #      document_data_extracted = [
                #          {"file_name": f"doc1_{service_order}.pdf", "file_title": "Doc 1", "file_subtitle": "Sub 1"},
                #          {"file_name": f"doc2_{service_order}.xlsx", "file_title": "Doc 2", "file_subtitle": "Sub 2"}
                #      ]
                pass  # Remove pass when using real extraction

            except Exception as e_doc:
                print(f"\nWarning: Error during document extraction for SO {service_order}: {e_doc}")
                # Continue, default document record might be added later if needed

        except Exception as outer_e:
            print(f"\nMajor error processing Service Order {service_order}: {outer_e}")
            process_error = True
            error_message = str(outer_e)
            # Decide if you want to add default records even on major errors

        finally:
            # --- Add records to batch (including defaults for failures/no data) ---
            quotation_record_added = False
            if quotation_data_extracted:
                # Remove transient keys like download_url if present
                quotation_data_extracted.pop("download_url", None)
                processed_records = _ensure_schema_and_defaults(
                    [quotation_data_extracted], QUOTATION_SCHEMA, service_order, now
                )
                batch_quotations.extend(processed_records)
                quotation_record_added = True
            else:
                # Add a default quotation record if none was extracted or if a major error occurred
                # (You might adjust this logic based on whether partial data is acceptable)
                # If process_error: print(f"\nAdding default quotation record for SO {service_order} due to error.")
                # else: print(f"\nAdding default quotation record for SO {service_order} (no data found).")
                batch_quotations.append(_create_default_record(QUOTATION_SCHEMA, service_order, now))
                quotation_record_added = True  # Mark default added

            document_records_added = False
            if document_data_extracted:  # Check if list is not empty
                # Remove transient keys like url if present
                for doc in document_data_extracted:
                    doc.pop("url", None)
                processed_records = _ensure_schema_and_defaults(
                    document_data_extracted, DOCUMENT_SCHEMA, service_order, now
                )
                batch_documents.extend(processed_records)
                document_records_added = True
            else:
                # Add a single default document record if none were extracted or major error
                # You might want to add a default only if the SO processing didn't fail entirely,
                # or always add one to signify the SO was attempted.
                # if process_error: print(f"\nAdding default document record for SO {service_order} due to error.")
                # else: print(f"\nAdding default document record for SO {service_order} (no data found).")
                batch_documents.append(_create_default_record(DOCUMENT_SCHEMA, service_order, now))
                document_records_added = True  # Mark default added

            processed_sos_in_batch.add(service_order)  # Add SO to batch set regardless of success/failure

            # --- Cleanup: Close Selenium View ---
            try:
                # Add check if driver is still valid if outer_e involved WebDriverException
                # if driver and driver.window_handles: # Basic check
                #    close_service_order_view(wait)
                pass  # Remove pass when using real cleanup
            except Exception as cleanup_e:
                # Avoid printing cleanup errors if the main error was a driver crash
                if not process_error or "WebDriverException" not in error_message:
                    print(f"\nWarning: Failed to close view for SO {service_order} during cleanup: {cleanup_e}")

        # --- Batch Processing Trigger ---
        is_last_item = i == total_orders - 1
        if len(processed_sos_in_batch) >= BATCH_SIZE or (is_last_item and processed_sos_in_batch):
            print(
                f"\nProcessing and saving batch of {len(processed_sos_in_batch)} service orders to ADLS (ending with SO {service_order})..."
            )
            try:
                main_quotations_df, main_documents_df = process_and_save_batch_adls(
                    batch_quotations,
                    batch_documents,
                    main_quotations_df,
                    main_documents_df,
                    update_mode,
                    QUOTATIONS_RAW_PATH,
                    DOCUMENTS_LIST_RAW_PATH,
                    processed_sos_in_batch,
                    storage_options=storage_options,  # Pass storage options
                    # If using custom resource: pass adls_resource=adls_resource
                )
                # Clear batches only after successful save
                batch_quotations = []
                batch_documents = []
                processed_sos_in_batch = set()
            except Exception as batch_save_e:
                print(f"\nCRITICAL ERROR during batch save: {batch_save_e}. Batch data might be lost for this cycle.")
                # Implement retry logic or persistent queue if needed
                # For now, we clear the batch to avoid reprocessing potentially problematic data indefinitely
                batch_quotations = []
                batch_documents = []
                processed_sos_in_batch = set()
                # Potentially re-raise or handle more gracefully

    print(" " * (len(progress_message) + 5), end="\r")  # Clear last progress line
    print("\n--- Data Extraction Complete ---")
    print(
        f"Final Quotations DF shape (in memory): {main_quotations_df.shape if not main_quotations_df.is_empty() else (0, len(QUOTATION_SCHEMA))}"
    )
    print(
        f"Final Documents DF shape (in memory): {main_documents_df.shape if not main_documents_df.is_empty() else (0, len(DOCUMENT_SCHEMA))}"
    )
    print(f"Total Service Orders processed/attempted: {total_orders - skipped_count}")
    print(f"Total Service Orders skipped: {skipped_count}")

    # Return the final in-memory dataframes if needed by downstream tasks
    return main_quotations_df, main_documents_df


@dg.asset
def scrape_service_orders(context: dg.AssetExecutionContext) -> dict:

    # --- Initialize Driver ---
    context.log.info("Initializing WebDriver...")
    driver = initialize_driver()
    wait = WebDriverWait(driver, DEFAULT_WAIT)
    context.log.info("WebDriver initialized.")

    # --- Login ---
    context.log.info("Logging into RESO+...")
    login_to_reso(driver, wait)
    context.log.info("Login successful.")

    click_presupuesto(driver, wait)

    run_service_orders_extraction(
        driver=driver,
        wait=wait,
        service_orders_input=some_service_orders,
        update_mode="skip",|
        storage_options=storage_opts,
    )
    # print("\nSkip run finished.")
    return summary_data
