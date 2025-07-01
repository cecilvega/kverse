import time
from datetime import datetime
from pathlib import Path

# --- Default imports ---
import dagster as dg
import pandas as pd
import polars as pl

# --- Selenium imports ---
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from datetime import date

# --- Relative module imports
from kdags.resources.tidyr import DataLake
from kdags.config import DATA_CATALOG
from ..reso import *
from .so_details_utils import (
    ensure_schema_and_defaults,
    DOCUMENTS_LIST_SCHEMA,
    QUOTATION_SCHEMA,
    create_default_record,
    process_and_save_batch,
    BATCH_SIZE,
)


@dg.asset(compute_kind="harvest")
def harvest_so_details(
    context: dg.AssetExecutionContext, select_so_to_update: pl.DataFrame, raw_so_quotations, raw_so_documents
) -> list:
    service_orders_data = select_so_to_update.select(["service_order", "component_serial"]).to_dicts()

    driver = initialize_driver()
    wait = WebDriverWait(driver, DEFAULT_WAIT)
    context.log.info("WebDriver initialized.")

    # --- Login ---
    login_to_reso(driver, wait)
    context.log.info("Login RESO+ successful.")
    click_presupuesto(driver, wait)

    # dl = DataLake()
    quotations_df = raw_so_quotations.clone()
    documents_list_df = raw_so_documents.clone()  # dl.read_tibble(DATA_CATALOG["so_documents"]["raw_path"])

    # --- Main Extraction Loop ---
    batch_quotations = []
    batch_documents = []
    processed_sos_in_batch = []
    processed_sos = []
    total_orders = len(service_orders_data)
    context.log.info(f"--- Starting Data Extraction for {total_orders} Service Orders ---")

    MAX_SO_ATTEMPTS = 2  # Total attempts: 1 initial + 1 retry

    for i, row_data in enumerate(service_orders_data):
        so_number = row_data["service_order"]
        component_serial = row_data["component_serial"]
        # if i > 6:
        #     break
        now = datetime.now()
        context.log.info(f"Processing SO {i + 1}/{total_orders}: {so_number}")

        current_attempt = 0
        successfully_processed_this_so = False

        while current_attempt < MAX_SO_ATTEMPTS and not successfully_processed_this_so:
            current_attempt += 1
            context.log.info(f"Attempt {current_attempt}/{MAX_SO_ATTEMPTS} for SO: {so_number}")

            try:

                # --- Main SO processing block ---
                context.log.info(f"Searching SO {so_number} (Attempt {current_attempt})")  #
                search_service_order(driver, wait, so_number)  #

                context.log.info(f"Accessing details for SO: {so_number} (Attempt {current_attempt})")
                click_see_service_order(driver, wait)  #

                context.log.info(f"Navigating to documents tab for SO: {so_number} (Attempt {current_attempt})")
                retry_on_interception(  #
                    context=context,
                    action_function=navigate_to_documents_tab,  #
                    max_retries=1,  # Internal retries for this specific action
                    delay_seconds=5,
                    wait=wait,
                    driver=driver,
                )

                context.log.info(f"Extracting document links for SO: {so_number} (Attempt {current_attempt})")
                # time.sleep(5)
                check_has_documents = has_documents(context, wait)
                if check_has_documents:
                    document_data_extracted = extract_document_links(driver, wait)
                    for doc in document_data_extracted:
                        doc.pop("url", None)
                    processed_documents_records = ensure_schema_and_defaults(
                        document_data_extracted, DOCUMENTS_LIST_SCHEMA, so_number, component_serial, now
                    )
                    batch_documents.extend(processed_documents_records)

                    navigate_to_quotation_tab(wait)

                    # time.sleep(5)

                    check_has_quotation = has_quotation(context, wait)
                    if check_has_quotation:
                        quotation_data_extracted = extract_quotation_details(driver, wait, so_number)
                        processed_quotations_records = ensure_schema_and_defaults(
                            [quotation_data_extracted], QUOTATION_SCHEMA, so_number, component_serial, now
                        )
                        batch_quotations.extend(processed_quotations_records)
                    else:
                        batch_quotations.append(
                            create_default_record(QUOTATION_SCHEMA, so_number, component_serial, now)
                        )

                else:
                    batch_quotations.append(create_default_record(QUOTATION_SCHEMA, so_number, component_serial, now))
                successfully_processed_this_so = True  # Mark as successful for this attempt
            except Exception as e:
                context.log.warning(
                    f"Attempt {current_attempt}/{MAX_SO_ATTEMPTS} for SO {so_number} failed: {type(e).__name__} - {str(e)}"
                )
                if current_attempt >= MAX_SO_ATTEMPTS:
                    context.log.error(
                        f"All {MAX_SO_ATTEMPTS} attempts failed for SO {so_number}. Last error: {type(e).__name__} - {str(e)}. This SO will be skipped."
                    )
                    break  # Exit while loop for this SO, it failed

                context.log.info(
                    f"Waiting 5 seconds before checking for popup for SO {so_number} (after attempt {current_attempt} failed)..."
                )
                time.sleep(1)
                popup_handled = check_and_close_error_popup(driver, wait)  # This function uses print for its logs
                if popup_handled:
                    context.log.info(
                        f"Error popup was detected and an attempt to close it was made after failed attempt {current_attempt} for SO {so_number}."
                    )
                else:
                    raise
                context.log.info(f"Attempting to reset to 'Presupuesto' page before retrying SO {so_number}...")

        # After all attempts for the current SO
        if successfully_processed_this_so:

            context.log.info(f"Successfully processed SO: {so_number}. Closing its view.")
            try:
                close_service_order_view(wait)  #
                context.log.info(f"View for SO {so_number} closed.")
                processed_sos_in_batch.append({"service_order": so_number, "component_serial": component_serial})

            except Exception as e_close:
                # Log error but still consider SO processed if data extraction was successful
                context.log.error(
                    f"SO {so_number} was processed, but an error occurred while closing its view: {e_close}"
                )

                processed_sos_in_batch.append({"service_order": so_number, "component_serial": component_serial})
        else:
            context.log.error(f"SO {so_number} could NOT be processed after {MAX_SO_ATTEMPTS} attempts.")
            # SO is not added to processed_sos_successfully

        # --- Batch Processing Trigger ---
        is_last_item = i == total_orders - 1
        if len(processed_sos_in_batch) >= BATCH_SIZE or (is_last_item and processed_sos_in_batch):
            context.log.info(f"\nProcessing and saving batch of {len(processed_sos_in_batch)} service orders")
            quotations_df, documents_list_df = process_and_save_batch(
                batch_quotations,
                batch_documents,
                quotations_df,
                documents_list_df,
                processed_sos_in_batch,
            )
            processed_sos.extend(processed_sos_in_batch)
            # # Clear batches only after successful save
            batch_quotations = []
            batch_documents = []
            processed_sos_in_batch = []
    context.log.info(f"Finished processing all service orders. Successfully processed: {total_orders}.")

    context.log.info("\n--- Data Extraction Complete ---")
    context.log.info(f"Final Quotations DF shape: {quotations_df.shape}")
    context.log.info(f"Final Documents DF shape: {documents_list_df.shape}")
    driver.quit()
    return processed_sos
