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

# Define update windows
COMPLETED_UPDATE_WINDOW_DAYS = 90  # Update completed SOs for 90 days after closing
MIN_UPDATE_INTERVAL_DAYS = 10  # Don't update same SO more than once per 10 days


@dg.asset(group_name="reparation")
def select_so_to_update(raw_so_quotations, so_report: pl.DataFrame):

    so_df = so_report.clone()

    df = (
        so_df.select(
            [
                "service_order",
                "component_serial",
                "reception_date",
                "load_final_report_date",
                "update_date",
                "reso_closing_date",
                "reso_repair_reason",
                "quotation_status",
                "component_status",
                "site_name",
            ]
        )
        .with_columns(reso_closing_date=pl.col("reso_closing_date").fill_null(pl.col("load_final_report_date")))
        .with_columns(days_diff=(pl.col("update_date") - pl.col("reso_closing_date")).dt.total_days())
        .drop(["load_final_report_date"])
    )

    df = df.join(
        raw_so_quotations.select(["service_order", "update_timestamp"]),
        on="service_order",
        how="left",
    )
    df = df.with_columns(
        [
            # Days since last update (NULL if never updated)
            pl.when(pl.col("update_timestamp").is_not_null())
            .then((pl.lit(date.today()) - pl.col("update_timestamp")).dt.total_days())
            .otherwise(None)
            .alias("days_since_update"),
            # Is this SO new (never processed)?
            pl.col("update_timestamp").is_null().alias("is_new_so"),
        ]
    )
    # Select SOs based on logical rules
    df = df.filter(
        # Rule 1: Always select new SOs that have never been processed
        pl.col("is_new_so")
        |
        # Rule 2: Select active/incomplete SOs that haven't been updated recently
        (
            ~pl.col("component_status").is_in(["Delivered", "Repaired"])
            & (pl.col("days_since_update") > MIN_UPDATE_INTERVAL_DAYS)
        )
        |
        # Rule 3: Select recently completed SOs (within update window) that haven't been updated recently
        (
            pl.col("component_status").is_in(["Delivered", "Repaired"])
            & pl.col("reso_closing_date").is_not_null()
            & (pl.col("days_diff") < COMPLETED_UPDATE_WINDOW_DAYS)
            & (pl.col("days_since_update") > MIN_UPDATE_INTERVAL_DAYS)
        )
    )

    # Apply site filter
    df = df.filter(pl.col("site_name").is_in(["Minera Spence", "MINERA ESCONDIDA"]))
    return df


@dg.asset
def raw_so_documents(context: dg.AssetExecutionContext):
    dl = DataLake(context)
    try:
        df = dl.read_tibble(DATA_CATALOG["so_documents"]["raw_path"])
    except Exception as e:
        df = pl.DataFrame(schema=DOCUMENTS_LIST_SCHEMA)

    return df


@dg.asset(group_name="reparation")
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
    processed_sos_in_batch = set()
    processed_sos = []
    total_orders = len(service_orders_data)
    context.log.info(f"--- Starting Data Extraction for {total_orders} Service Orders ---")

    processed_sos_successfully = []
    MAX_SO_ATTEMPTS = 2  # Total attempts: 1 initial + 1 retry

    for i, row_data in enumerate(service_orders_data):
        so_number = row_data["service_order"]
        component_serial = row_data["component_serial"]
        if i > 6:
            break
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
                time.sleep(5)
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

                    time.sleep(5)

                    check_has_quotation = has_quotation(context, wait)
                    if check_has_quotation:
                        quotation_data_extracted = extract_quotation_details(driver, wait, so_number)
                        processed_quotations_records = ensure_schema_and_defaults(
                            [quotation_data_extracted], QUOTATION_SCHEMA, so_number, component_serial, now
                        )
                        batch_quotations.extend(processed_quotations_records)
                        time.sleep(5)
                else:
                    batch_quotations.append(create_default_record(QUOTATION_SCHEMA, so_number, now))
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
                time.sleep(5)
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
                processed_sos_successfully.append(so_number)
                processed_sos_in_batch.add(so_number)

            except Exception as e_close:
                # Log error but still consider SO processed if data extraction was successful
                context.log.error(
                    f"SO {so_number} was processed, but an error occurred while closing its view: {e_close}"
                )
                processed_sos_successfully.append(so_number)  # Still count as processed for data
                processed_sos_in_batch.add(so_number)
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
            processed_sos_in_batch = set()
    context.log.info(
        f"Finished processing all service orders. Successfully processed: {len(processed_sos_successfully)} out of {total_orders}."
    )

    context.log.info("\n--- Data Extraction Complete ---")
    context.log.info(f"Final Quotations DF shape: {quotations_df.shape}")
    context.log.info(f"Final Documents DF shape: {documents_list_df.shape}")

    return processed_sos
