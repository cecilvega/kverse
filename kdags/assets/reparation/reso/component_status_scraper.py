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
from .main_navigation import click_reportabilidad, click_component_status
from .web_driver import initialize_driver, login_to_reso, DEFAULT_WAIT, RESO_URL


def set_default_component_status_filters(driver, wait: WebDriverWait):
    """Sets the default filters: Toggles 'See All', selects all workshops."""

    # Wait for the 'Select All Workshops' checkbox to be present in the DOM.
    select_all_locator = (By.ID, "cbxTodosTalleres")
    wait.until(EC.presence_of_element_located(select_all_locator))
    time.sleep(0.5)  # Small pause after click might help stability

    # 1. Click "See All" switch
    see_all_switch_locator = (By.CSS_SELECTOR, "input#verTodosOS + span.switchery")
    # Use presence first, then check clickability after scroll
    see_all_switch = wait.until(EC.presence_of_element_located(see_all_switch_locator))
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", see_all_switch)
    # Now wait for clickability
    clickable_switch = wait.until(EC.element_to_be_clickable(see_all_switch_locator))
    clickable_switch.click()

    # 2. Open Workshop dropdown (using original overlay locator)
    overlay_locator = (
        By.XPATH,
        "//select[@id='cbxModeloDTaller']/following-sibling::div[contains(@class, 'overSelect')]",
    )
    select_element_locator = (By.ID, "cbxModeloDTaller")  # For scrolling reference

    # Ensure the underlying select element exists before scrolling
    select_element = wait.until(EC.presence_of_element_located(select_element_locator))

    # Scrolling Workshop dropdown area into view...
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", select_element)

    # Wait for the overlay element itself to be present before trying to click
    overlay_div = wait.until(EC.presence_of_element_located(overlay_locator))

    # Wait for the overlay to be clickable ---
    wait.until(EC.element_to_be_clickable(overlay_locator))

    overlay_div.click()  # Click the overlay div

    # 3. Click "SELECCIONAR TODOS" checkbox inside the dropdown
    # Wait for the checkbox to become VISIBLE now that the dropdown is open
    wait.until(EC.visibility_of_element_located(select_all_locator))
    select_all_checkbox = wait.until(EC.element_to_be_clickable(select_all_locator))
    select_all_checkbox.click()
    time.sleep(0.5)  # Small pause after click might help stability

    # 4. Close Workshop dropdown (using original overlay locator)
    overlay_div_to_close = wait.until(EC.element_to_be_clickable(overlay_locator))
    overlay_div_to_close.click()

    # Wait for the dropdown content (the 'Select All' checkbox) to become invisible
    wait.until(EC.invisibility_of_element_located(select_all_locator))


# --- Helper function to handle date setting logic ---
def set_date(
    driver,
    wait,
    date_str: str,
    input_locator: tuple,
    icon_locator: tuple,
    date_label: str,
    datepicker_widget_locator,
):
    print(f"Setting '{date_label}': {date_str}")

    # Parse day - Script will raise ValueError here if format is wrong
    target_day_dt = datetime.strptime(date_str, "%d-%m-%Y")
    target_day_number = str(target_day_dt.day)
    print(f"  Target Day: {target_day_number}")

    # --- Open and Interact with Calendar Widget ---
    calendar_icon: WebElement = wait.until(EC.element_to_be_clickable(icon_locator))
    # -- Start Change --
    # Replace the standard click with a JavaScript click
    print("  Clicking calendar icon using JavaScript...")
    driver.execute_script("arguments[0].click();", calendar_icon)
    # -- End Change --

    # --- Interact with Input Field ---
    date_input: WebElement = wait.until(EC.element_to_be_clickable(input_locator))
    driver.execute_script("arguments[0].scrollIntoViewIfNeeded(true);", date_input)
    date_input.clear()
    date_input.send_keys(date_str)
    print(f"  Sent keys '{date_str}' to input field.")
    wait.until(EC.text_to_be_present_in_element_value(input_locator, date_str))
    print("  Input value confirmed.")

    # Wait explicitly for the calendar widget container to be visible (no change here yet)
    datepicker_widget: WebElement = wait.until(EC.visibility_of_element_located(datepicker_widget_locator))
    print("  Calendar widget is visible.")

    # --- Locate and Click the Target Day ---
    # Define the relative locator for searching WITHIN the datepicker widget
    day_link_locator_relative = (
        By.XPATH,
        f".//td[not(contains(@class, 'ui-datepicker-other-month'))]/a[normalize-space(text())='{target_day_number}']",
    )
    print(f"  Looking for day '{target_day_number}' link within the calendar widget...")

    # Step 1: Find the day link element *within the datepicker widget*.
    # We use a wait with a lambda here just to ensure it's present before checking clickability.
    # This lambda correctly calls find_element and returns the WebElement.
    day_link_element: WebElement = wait.until(lambda d: datepicker_widget.find_element(*day_link_locator_relative))
    print(f"  Found day '{target_day_number}' element. Now waiting for it to be clickable...")

    # Step 2: Wait for the *found WebElement* to be clickable.
    # Pass the actual WebElement variable here, NOT the locator or lambda.
    wait.until(EC.element_to_be_clickable(day_link_element))
    print(f"  Day '{target_day_number}' is clickable.")

    # Step 3: Click the now confirmed clickable element.
    day_link_element.click()
    print(f"  Clicked day '{target_day_number}' in calendar for '{date_label}'.")

    # --- Wait for Calendar to Close ---
    wait.until(EC.invisibility_of_element_located(datepicker_widget_locator))
    print("  Calendar widget closed.")


def set_component_status_dates(driver, wait: WebDriverWait, start_date: str, end_date: str):
    print(f"--- Setting Dates: {start_date} to {end_date} ---")

    # --- Locators ---
    date_from_input_locator = (By.ID, "dfechaInicial")
    calendar_icon_from_locator = (By.CSS_SELECTOR, "span.controlFechaInicial")
    date_up_input_locator = (By.ID, "dfechaFinal")
    calendar_icon_up_locator = (By.CSS_SELECTOR, "span.controlFechaFinal")
    datepicker_widget_locator = (By.ID, "ui-datepicker-div")  # The main calendar pop-up

    # --- Call helper function for both dates ---
    set_date(
        driver,
        wait,
        start_date,
        date_from_input_locator,
        calendar_icon_from_locator,
        "Date From",
        datepicker_widget_locator,
    )
    time.sleep(1)
    set_date(
        driver,
        wait,
        end_date,
        date_up_input_locator,
        calendar_icon_up_locator,
        "Date Up",
        datepicker_widget_locator,
    )

    print(f"\nFinished setting dates: {start_date} to {end_date}")


def export_component_status_results(driver, wait):
    """Clicks Search and then Export, waiting for results."""
    search_button_locator = (By.CSS_SELECTOR, "span.btn.btnVerde.btnBuscar")
    search_button = wait.until(EC.presence_of_element_located(search_button_locator))
    wait.until(EC.element_to_be_clickable(search_button_locator))
    search_button.click()

    # Wait for loading indicator to disappear
    loading_indicator_locator = (By.CSS_SELECTOR, "div.updateProgress")
    # Use a longer timeout if search is slow
    results_wait = WebDriverWait(driver, 120)  # 60 second timeout for results
    results_wait.until(EC.invisibility_of_element_located(loading_indicator_locator))

    # Click Export to Excel
    export_button_locator = (By.ID, "ContentPlaceHolder1_btnExportar")
    export_button = wait.until(EC.presence_of_element_located(export_button_locator))
    wait.until(EC.element_to_be_clickable(export_button_locator))
    export_button.click()
    time.sleep(5)  # Pause to allow download to initiate/complete


def upload_component_status_results(context: dg.AssetExecutionContext, az_path: str, year: int):
    """Finds latest download, reads as HTML table, uploads to ADLS, deletes local file."""
    print(f"--- Uploading results for year {year} to {az_path} ---")

    download_dir = Path.home() / "Downloads"
    context.log.info(f"Checking for downloads in: {download_dir}")

    if not download_dir.is_dir():
        context.log.error(f"Download directory not found: {download_dir}")
        return None  # Indicate failure

    time.sleep(1)  # Pause for file system

    excel_files = list(download_dir.glob("*.xlsx")) + list(download_dir.glob("*.xls"))

    excel_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    downloaded_file_path = excel_files[0]
    context.log.info(f"Identified most recent Excel file: {downloaded_file_path}")

    pandas_df = pd.read_html(downloaded_file_path)[0]
    all_string_converters = {col_name: str for col_name in pandas_df.columns}
    pandas_df = pd.read_html(
        downloaded_file_path,
        converters=all_string_converters,
        dtype_backend="pyarrow",
    )[0]

    context.log.info(f"Successfully read data from {downloaded_file_path.name}")

    df = pl.from_pandas(pandas_df, schema_overrides={c: pl.String for c in pandas_df.columns.to_list()})

    # Upload to Data Lake
    datalake = DataLake()
    datalake.upload_tibble(az_path=az_path, tibble=df, format="parquet")
    context.log.info(f"Successfully uploaded data for year {year} to {az_path}")

    # Delete local file
    context.log.info(f"Deleting local file: {downloaded_file_path}")
    downloaded_file_path.unlink(missing_ok=True)  # missing_ok=True ignores error if already gone
    context.log.info(f"Deleted local file: {downloaded_file_path.name}")

    return az_path


@dg.asset
def scrape_component_status(context: dg.AssetExecutionContext) -> dict:

    uploaded_uris = []
    summary_data = {
        "years_processed": [],
        "uploaded_files": [],
        "failed_years": [],
        "num_files_uploaded": 0,
        "status": "incomplete",
    }

    num_years_back = 2  # Define how many years back to process
    # --- Process Past Years ---
    current_date = datetime.now()
    current_year = current_date.year
    start_loop_year = current_year
    end_loop_year = start_loop_year - num_years_back

    # --- Initialize Driver ---
    driver = initialize_driver()
    wait = WebDriverWait(driver, DEFAULT_WAIT)
    context.log.info("WebDriver initialized.")

    # --- Login ---

    login_to_reso(driver, wait)
    context.log.info("Login RESO+ successful.")

    context.log.info(f"Starting year loop from {start_loop_year} down to {end_loop_year}...")

    for year in range(start_loop_year, end_loop_year - 1, -1):
        context.log.info(f"Processing Year: {year}")
        summary_data["years_processed"].append(year)
        start_date_str = f"01-01-{year}"
        end_date_str = f"31-12-{year}"

        # --- Navigate ---
        driver.get(RESO_URL)
        click_reportabilidad(driver, wait)
        click_component_status(driver, wait)
        context.log.info("Navigated to Estatus Componente page.")

        # --- Set Default Filters ---
        set_default_component_status_filters(driver, wait)
        context.log.info("Default filters applied.")

        # Set dates for the current year
        set_component_status_dates(driver, wait, start_date_str, end_date_str)

        # Export the results for the current year
        export_component_status_results(driver, wait)

        # Define upload URI for the current year
        # Using current_date for partition, but year for filename seems intended
        partition_path = current_date.strftime("y=%Y/m=%m/d=%d")  # Partition based on run date
        az_path = f"az://bhp-raw-data/RESO/COMPONENT_STATUS/{partition_path}/component_status_{year}.parquet"

        # Upload results and cleanup
        uploaded_uri = upload_component_status_results(context, az_path, year)
        if uploaded_uri:
            uploaded_uris.append(uploaded_uri)
            # Optionally add more details per file if needed
            summary_data["uploaded_files"].append({"year": year, "az_path": uploaded_uri})
        else:
            context.log.error(f"Upload failed for year {year}")
            summary_data["failed_years"].append(year)
            # Decide if you want to stop or continue if one year fails

    context.log.info("Year loop finished.")
    summary_data["status"] = "completed" if not summary_data["failed_years"] else "completed_with_errors"
    summary_data["num_files_uploaded"] = len(uploaded_uris)

    context.log.info("Closing WebDriver...")
    driver.quit()
    context.log.info("WebDriver closed.")

    # Return the summary dictionary
    context.add_output_metadata(metadata=summary_data)
    return summary_data
