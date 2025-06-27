# --- Default imports ---
import dagster as dg
import polars as pl
import requests

# --- Selenium imports ---
from selenium.webdriver.support.ui import WebDriverWait

from kdags.config import DATA_CATALOG

# --- Relative module imports
from kdags.resources.tidyr import DataLake
from ..reso import *


@dg.asset(group_name="reparation")
def harvest_so_documents(context: dg.AssetExecutionContext, mutate_so_documents: pl.DataFrame) -> list:
    dl = DataLake(context)
    driver = initialize_driver()
    wait = WebDriverWait(driver, DEFAULT_WAIT)
    context.log.info("WebDriver initialized.")

    # --- Login ---
    login_to_reso(driver, wait)
    context.log.info("Login RESO+ successful.")
    click_presupuesto(driver, wait)

    service_orders_data = (
        mutate_so_documents.filter(pl.col("last_modified").is_null())
        .select(["service_order", "component_serial"])
        .to_dicts()
    )
    processed_sos_successfully = []
    MAX_SO_ATTEMPTS = 2  # Total attempts: 1 initial + 1 retry

    for i, row_data in enumerate(service_orders_data):
        so_number = row_data["service_order"]
        component_serial = row_data["component_serial"]
        context.log.info(f"Starting processing for Service Order: {so_number}")

        current_attempt = 0
        successfully_processed_this_so = False

        while current_attempt < MAX_SO_ATTEMPTS and not successfully_processed_this_so:
            current_attempt += 1
            context.log.info(f"Attempt {current_attempt}/{MAX_SO_ATTEMPTS} for SO: {so_number}")

            try:
                # Filter data for the current SO for this attempt
                filter_docs_df = mutate_so_documents.filter(
                    (pl.col("service_order") == so_number) & (pl.col("component_serial") == component_serial)
                )

                # --- Main SO processing block ---
                context.log.info(f"Searching SO {so_number} (Attempt {current_attempt})")  #
                search_service_order(driver, wait, so_number)  #

                context.log.info(f"Accessing details for SO: {so_number} (Attempt {current_attempt})")
                click_see_service_order(driver, wait)  #

                context.log.info(f"Navigating to documents tab for SO: {so_number} (Attempt {current_attempt})")
                retry_on_interception(  #
                    context=context,
                    action_function=navigate_to_documents_tab,  #
                    max_retries=2,
                    delay_seconds=5,
                    wait=wait,
                    driver=driver,
                )

                context.log.info(f"Extracting document links for SO: {so_number} (Attempt {current_attempt})")
                document_data_extracted = extract_document_links(driver, wait)  #

                so_docs_df = (
                    pl.DataFrame(document_data_extracted)  #
                    .join(filter_docs_df.select(["file_name", "az_path"]), how="left", on="file_name")  #
                    .drop_nulls("az_path")  #
                )

                if so_docs_df.is_empty():
                    if not document_data_extracted:
                        context.log.info(f"No documents found on page for SO: {so_number} (Attempt {current_attempt}).")
                    else:
                        context.log.warning(
                            f"Documents extracted for SO: {so_number}, but none mapped to an az_path for upload (Attempt {current_attempt}). Check 'file_name' matching."
                        )
                else:
                    context.log.info(
                        f"Found {len(so_docs_df)} documents to upload for SO: {so_number} (Attempt {current_attempt})."
                    )

                for idx, row_data in enumerate(so_docs_df.to_dicts()):  #
                    source_url = row_data["url"]  #
                    destination_az_path = row_data["az_path"]  #
                    file_name_log = row_data.get("file_name", "Unknown Filename")
                    context.log.info(
                        f"Uploading doc {idx + 1}/{len(so_docs_df)} ('{file_name_log}') for SO {so_number} to {destination_az_path} (Attempt {current_attempt})"
                    )
                    try:
                        dl.upload_file(source_url, destination_az_path)  #
                    except requests.exceptions.HTTPError as e_http:  #
                        if e_http.response is not None and e_http.response.status_code == 403:  #
                            context.log.warning(
                                f"Skipping upload for {file_name_log} (SO: {so_number}) due to 403 Client Error: Server failed to authenticate."
                            )
                        else:
                            # Log other HTTP errors but continue processing other files for this SO
                            context.log.error(
                                f"HTTPError during upload for {file_name_log} (SO: {so_number}): {e_http}"
                            )
                    except Exception as e_upload:
                        context.log.error(
                            f"Non-HTTP error during upload for {file_name_log} (SO: {so_number}): {e_upload}"
                        )

                successfully_processed_this_so = True  # Mark as successful for this attempt
                context.log.info(f"SO: {so_number} processing completed for attempt {current_attempt}.")
                # --- End of Main SO processing block for this attempt ---

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
                try:
                    # Navigate to a known good state before retrying the SO processing.
                    # This ensures we are not stuck in an unexpected part of the website.
                    click_presupuesto(driver, wait)  #
                    context.log.info("'Presupuesto' page reached. Ready for next attempt.")
                except Exception as nav_e:
                    context.log.error(
                        f"Failed to navigate to 'Presupuesto' page before retrying SO {so_number}: {nav_e}. Will proceed with next attempt regardless."
                    )

        # After all attempts for the current SO
        if successfully_processed_this_so:
            context.log.info(f"Successfully processed SO: {so_number}. Closing its view.")
            try:
                close_service_order_view(wait)  #
                context.log.info(f"View for SO {so_number} closed.")
                processed_sos_successfully.append(so_number)
            except Exception as e_close:
                # Log error but still consider SO processed if data extraction was successful
                context.log.error(
                    f"SO {so_number} was processed, but an error occurred while closing its view: {e_close}"
                )
                processed_sos_successfully.append(so_number)  # Still count as processed for data
        else:
            context.log.error(f"SO {so_number} could NOT be processed after {MAX_SO_ATTEMPTS} attempts.")
            # SO is not added to processed_sos_successfully

    context.log.info(
        f"Finished processing all service orders. Successfully processed: {len(processed_sos_successfully)} out of {len(service_orders_data)}."
    )
    return service_orders_data
