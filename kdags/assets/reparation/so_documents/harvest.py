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
def select_documents_to_update(
    context: dg.AssetExecutionContext, component_reparations: pl.DataFrame, so_report: pl.DataFrame
):
    dl = DataLake(context)
    downloaded_documents = dl.list_paths("az://bhp-raw-data/RESO/DOCUMENTS").select(["az_path", "last_modified"])
    df = dl.read_tibble(DATA_CATALOG["so_documents"]["raw_path"])
    df = (
        df.with_columns(_file_title=pl.col("file_title").str.to_lowercase())
        .with_columns(
            file_type=pl.when(pl.col("_file_title").str.contains("carta presupuesto"))
            .then(pl.lit("quotation"))
            .when(pl.col("_file_title").str.contains("informe tecnico preliminar"))
            .then(pl.lit("preliminary_report"))
            .when(pl.col("_file_title").str.contains("informe tecnico final"))
            .then(pl.lit("final_report"))
            .otherwise(pl.lit("other"))
        )
        .drop("_file_title")
        .with_columns(
            pl.when(pl.col("file_type") == "quotation")
            .then(
                pl.concat_str(
                    [
                        pl.lit("az://bhp-raw-data/RESO/DOCUMENTS/QUOTATIONS/"),
                        pl.col("service_order"),
                        pl.lit("/"),
                        pl.col("file_name"),
                    ]
                )
            )
            .when(pl.col("file_type") == "preliminary_report")
            .then(
                pl.concat_str(
                    [
                        pl.lit("az://bhp-raw-data/RESO/DOCUMENTS/PRELIMINARY_REPORT/"),
                        pl.col("service_order"),
                        pl.lit("/"),
                        pl.col("file_name"),
                    ]
                )
            )
            .when(pl.col("file_type") == "final_report")
            .then(
                pl.concat_str(
                    [
                        pl.lit("az://bhp-raw-data/RESO/DOCUMENTS/FINAL_REPORT/"),
                        pl.col("service_order"),
                        pl.lit("/"),
                        pl.col("file_name"),
                    ]
                )
            )
            .otherwise(
                pl.concat_str(
                    [
                        pl.lit("az://bhp-raw-data/RESO/DOCUMENTS/OTHER/"),
                        pl.col("service_order"),
                        pl.lit("/"),
                        pl.col("file_name"),
                    ]
                )
            )
            .alias("az_path")
        )
    )

    df = df.join(downloaded_documents, on="az_path", how="left")
    return df


@dg.asset(group_name="reparation")
def harvest_so_documents(context: dg.AssetExecutionContext, select_documents_to_update: pl.DataFrame) -> list:
    dl = DataLake(context)
    driver = initialize_driver()
    wait = WebDriverWait(driver, DEFAULT_WAIT)
    context.log.info("WebDriver initialized.")

    # --- Login ---
    login_to_reso(driver, wait)
    context.log.info("Login RESO+ successful.")
    click_presupuesto(driver, wait)

    sos = select_documents_to_update.filter(pl.col("last_modified").is_null())["service_order"].unique()
    sos = sos.to_list()
    processed_sos = []
    for so in sos:
        context.log.info(f"Searching SO {so}")
        filter_docs_df = select_documents_to_update.filter(pl.col("service_order") == so)
        search_service_order(driver, wait, so)
        context.log.info(f"Accessing details SO: {so}")
        click_see_service_order(driver, wait)
        retry_on_interception(
            context=context,
            action_function=navigate_to_documents_tab,
            max_retries=2,
            delay_seconds=5,
            wait=wait,
        )
        document_data_extracted = extract_document_links(driver, wait)
        so_docs_df = (
            pl.DataFrame(document_data_extracted)
            .join(filter_docs_df.select(["file_name", "az_path"]), how="left", on="file_name")
            .drop_nulls("az_path")
        )  # fix this should not remove non mapped files
        for row in so_docs_df.to_dicts():
            source_url = row["url"]
            destination_az_path = row["az_path"]
            try:
                dl.upload_file(source_url, destination_az_path)
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 403:
                    context.log.warning(
                        f"Skipping upload for {row['file_name']} due to a 403 Client Error: Server failed to authenticate."
                    )

            context.log.info(f"File {row['file_name']} uploaded to {destination_az_path}")

        processed_sos.append(so)
        context.log.info(f"Closing SO: {so}")
        close_service_order_view(wait)
    return processed_sos
