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


@dg.asset(group_name="reparation")
def select_so_to_update(component_reparations: pl.DataFrame, so_report: pl.DataFrame):
    cr_df = component_reparations.clone()
    so_df = so_report.clone()
    merge_columns = [
        "equipment_name",
        "component_name",
        "subcomponent_name",
        "position_name",
        "changeout_date",
    ]
    so_df = (
        so_df.select(
            [
                "service_order",
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

    cr_df = (
        cr_df.select([*merge_columns, "service_order"])
        .filter(pl.col("service_order").is_not_null())
        .unique("service_order")
    )
    df = cr_df.join(
        so_df,
        on="service_order",
        how="left",
        # validate="1:1",
    )
    df = pl.concat(
        [
            df.filter(
                ~(pl.col("component_status").is_in(["Delivered", "Repaired"])) | (pl.col("reso_closing_date").is_null())
            ),
            df.filter(
                (pl.col("component_status").is_in(["Delivered", "Repaired"]))
                & (pl.col("reso_closing_date").is_not_null())
                & (pl.col("days_diff") < pl.lit(365 * 3))
            ),
        ]
    )

    return df


@dg.asset(group_name="reparation")
def harvest_so_details(context: dg.AssetExecutionContext, select_so_to_update: pl.DataFrame) -> list:
    service_orders = select_so_to_update["service_order"].to_list()

    driver = initialize_driver()
    wait = WebDriverWait(driver, DEFAULT_WAIT)
    context.log.info("WebDriver initialized.")

    # --- Login ---
    login_to_reso(driver, wait)
    context.log.info("Login RESO+ successful.")
    click_presupuesto(driver, wait)

    dl = DataLake()
    quotations_df = dl.read_tibble(DATA_CATALOG["so_quotations"]["raw_path"])
    documents_list_df = dl.read_tibble(DATA_CATALOG["so_documents"]["raw_path"])

    # --- Main Extraction Loop ---
    batch_quotations = []
    batch_documents = []
    processed_sos_in_batch = set()
    processed_sos = []
    total_orders = len(service_orders)
    context.log.info(f"--- Starting Data Extraction for {total_orders} Service Orders ---")

    for i, service_order in enumerate(service_orders):
        context.log.info(f"Processing SO {i + 1}/{total_orders}: {service_order}")

        # --- Process Single Service Order ---
        now = datetime.now()
        quotation_data_extracted = None
        document_data_extracted = []

        search_service_order(driver, wait, service_order)
        click_see_service_order(driver, wait)
        retry_on_interception(
            context=context,
            action_function=navigate_to_quotation_tab,  # The function to call
            max_retries=2,  # Try the initial call + 2 retries (total 3 attempts)
            delay_seconds=5,  # Wait 5 seconds between retries
            wait=wait,
        )
        navigate_to_quotation_tab(wait)
        check_has_quotation = has_quotation(context, wait)
        batch_quotations.append(create_default_record(QUOTATION_SCHEMA, service_order, now))
        if check_has_quotation:
            quotation_data_extracted = extract_quotation_details(wait, service_order)

            navigate_to_documents_tab(wait)
            check_has_documents = has_documents(context, wait)
            if check_has_documents:
                document_data_extracted = extract_document_links(driver, wait)

                quotation_data_extracted.pop("download_url", None)
                processed_records = ensure_schema_and_defaults(
                    [quotation_data_extracted], QUOTATION_SCHEMA, service_order, now
                )
                batch_quotations.extend(processed_records)

                for doc in document_data_extracted:
                    doc.pop("url", None)
                processed_records = ensure_schema_and_defaults(
                    document_data_extracted, DOCUMENTS_LIST_SCHEMA, service_order, now
                )
                batch_documents.extend(processed_records)

        processed_sos_in_batch.add(service_order)

        # --- Cleanup: Close Selenium View ---
        close_service_order_view(wait)

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

    context.log.info("\n--- Data Extraction Complete ---")
    context.log.info(f"Final Quotations DF shape: {quotations_df.shape}")
    context.log.info(f"Final Documents DF shape: {documents_list_df.shape}")

    return processed_sos
