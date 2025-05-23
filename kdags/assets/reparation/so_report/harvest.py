# --- Default imports ---
from datetime import datetime

import dagster as dg

# --- Selenium imports ---
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# --- Relative module imports
from ..reso import *


@dg.asset(group_name="reparation")
def harvest_so_report(context: dg.AssetExecutionContext):
    driver = initialize_driver()
    context.log.info("WebDriver initialized.")

    # --- Login ---
    login_to_reso(driver, WebDriverWait(driver, 30))  # Pass wait instance if function expects it
    context.log.info("Login to RESO+ successful.")
    click_presupuesto(driver, WebDriverWait(driver, 30))  # Pass wait instance if function expects it
    context.log.info("Navigated to 'Presupuesto' section.")

    # --- Determine years for the loop ---
    current_year = datetime.now().year
    years_to_process = [current_year, current_year - 1, current_year - 2, current_year - 3]
    context.log.info(f"Starting report harvest for {len(years_to_process)} years: {years_to_process}")

    for year_to_filter in years_to_process:
        context.log.info(f"--- Processing year: {year_to_filter} ---")

        date_from_value = f"01-01-{year_to_filter}"
        date_up_value = f"31-12-{year_to_filter}"
        # context.log.info(f"Target dates: FROM '{date_from_value}', UP TO '{date_up_value}'") # Optional, if dates themselves are critical for log

        # Open filter dialog
        filters_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "popupFiltrar")))
        filters_button.click()
        WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.ID, "tboxFechaInicial")))
        # context.log.info("Filter dialog opened.") # Reduced

        # Set DATE FROM
        date_from_input_field = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "tboxFechaInicial"))
        )
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "tboxFechaInicial")))
        date_from_input_field.clear()
        date_from_input_field.send_keys(date_from_value)

        overlay_locator = (By.ID, "updateProgressActividades")
        WebDriverWait(driver, 15).until(EC.invisibility_of_element_located(overlay_locator))

        calendar_icon_from_locator = (By.CSS_SELECTOR, "span.controlFechaInicial")
        calendar_icon_from_first = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(calendar_icon_from_locator)
        )
        calendar_icon_from_first.click()
        time.sleep(0.5)
        calendar_icon_from_second = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(calendar_icon_from_locator)
        )
        calendar_icon_from_second.click()

        # Set DATE UP
        date_up_input_field = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "tboxFechaFinal")))
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "tboxFechaFinal")))
        date_up_input_field.clear()
        date_up_input_field.send_keys(date_up_value)

        WebDriverWait(driver, 15).until(EC.invisibility_of_element_located(overlay_locator))

        calendar_icon_up_locator = (By.CSS_SELECTOR, "span.controlFechaFinal")
        calendar_icon_up_first = WebDriverWait(driver, 10).until(EC.element_to_be_clickable(calendar_icon_up_locator))
        calendar_icon_up_first.click()
        time.sleep(0.5)
        calendar_icon_up_second = WebDriverWait(driver, 10).until(EC.element_to_be_clickable(calendar_icon_up_locator))
        calendar_icon_up_second.click()
        context.log.info(f"Date range for {year_to_filter} ({date_from_value} to {date_up_value}) set.")

        # Apply filters within the dialog
        filter_apply_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "controlBotonFiltrar"))
        )
        filter_apply_button.click()
        # context.log.info(f"Filters applied for {year_to_filter}.") # Reduced

        # Wait for main data load
        WebDriverWait(driver, 60).until(EC.invisibility_of_element_located(overlay_locator))
        context.log.info(f"Data loaded for {year_to_filter} after applying filters.")

        # Export
        export_button = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.ID, "descargarExcelBusqueda")))
        export_button.click()
        context.log.info(f"Export to Excel initiated for {year_to_filter}.")

        time.sleep(3)  # Allow time for download to start
        context.log.info(f"--- Finished processing for year: {year_to_filter} ---")

    context.log.info("All years processed. Closing WebDriver.")
    driver.quit()
    context.log.info("WebDriver closed. Asset 'harvest_so_report' finished.")
