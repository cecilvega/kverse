from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time

__all__ = ["search_service_order", "click_see_service_order", "navigate_to_quotation_tab", "navigate_to_documents_tab"]


def search_service_order(wait, service_order):

    # --- Find the input field, clear it, and enter the service order ---
    # print(f"Locating the service order input field (codigoBuscar)...")
    search_input_locator = (By.ID, "codigoBuscar")
    search_input = wait.until(EC.presence_of_element_located(search_input_locator))
    # print(f"Entering service order: {service_order}")
    search_input.clear()
    search_input.send_keys(str(service_order))  # Ensure input is string
    # ---------------------------------------------------------------------

    # --- Find and click the search button ---
    # print("Locating the search button (buttonBuscar)...")
    search_button_locator = (By.ID, "buttonBuscar")
    search_button = wait.until(EC.element_to_be_clickable(search_button_locator))
    # print("Clicking the search button...")
    search_button.click()
    # -----------------------------------------

    # --- Wait for the filtering results (assuming the overlay reappears) ---
    # print("Waiting for search results filtering overlay to disappear...")
    overlay_locator = (By.CSS_SELECTOR, ".overlay")
    # IMPORTANT: This assumes the *same* overlay with class 'overlay' is used for filtering.
    wait.until(EC.invisibility_of_element_located(overlay_locator))
    # print("Search results filtering complete.")
    # --------------------------------------------------------------------


def click_see_service_order(wait):
    """
    Waits for the results table and clicks the 'See service order' button
    for the first row.

    Args:
        driver: The Selenium WebDriver instance.
        wait: The WebDriverWait instance.
    """
    # --- Wait for the results table ---
    # print("Waiting for the results table (WebGrid)...")
    webgrid_locator = (By.ID, "WebGrid")
    wait.until(EC.presence_of_element_located(webgrid_locator))
    # print("Results table found.")

    # --- Wait directly for the button to be clickable and click it ---
    # Locator targets the button with class 'verOrdenServicio' in the first row (implicitly)
    see_order_button_locator = (By.CSS_SELECTOR, "button.verOrdenServicio")
    # print("Waiting for the 'See service order' button to be clickable...")
    # This wait implicitly handles presence and visibility before checking clickability
    see_order_button = wait.until(EC.element_to_be_clickable(see_order_button_locator))
    # print("'See service order' button is clickable.")

    # print("Clicking the 'See service order' button...")
    see_order_button.click()  # Using standard click
    # print("Clicked 'See service order' button.")
    # ------------------------------------------------------------------


def navigate_to_quotation_tab(driver, wait):
    """
    Waits for and clicks the 'Quotation' tab, then waits for its content to load.

    Args:
        driver: The Selenium WebDriver instance.
        wait: The WebDriverWait instance.
    """
    # --- Wait for the Quotation tab button to be ready and click it ---
    # print("Waiting for the 'Quotation' tab button (v-pills-presupuesto-tab) to be clickable...")
    quotation_tab_locator = (By.ID, "v-pills-presupuesto-tab")

    # Wait for the tab button to be clickable
    quotation_tab_button = wait.until(EC.element_to_be_clickable(quotation_tab_locator))
    # print("'Quotation' tab button is clickable.")

    # print("Clicking the 'Quotation' tab button...")
    quotation_tab_button.click()
    # print("Clicked 'Quotation' tab button.")
    # ---------------------------------------------------------------------

    # --- Wait for content within the Quotation tab to load (if necessary) ---
    # Assumes the overlay appears again. Adjust if needed.
    # print("Waiting for Quotation tab content to load (overlay to disappear)...")
    overlay_locator = (By.CSS_SELECTOR, ".overlay")
    wait.until(EC.invisibility_of_element_located(overlay_locator))
    # print("Quotation tab content loaded (overlay disappeared).")
    # --------------------------------------------------------------------------


def navigate_to_documents_tab(wait):
    """
    Waits for and clicks the 'Documents' tab, then waits for its content to load.

    Args:
        driver: The Selenium WebDriver instance.
        wait: The WebDriverWait instance.
    """
    # --- Wait for the Documents tab button to be ready and click it ---
    # print("Waiting for the 'Documents' tab button (v-pills-documentos-tab) to be clickable...")
    documents_tab_locator = (By.ID, "v-pills-documentos-tab")

    # Wait for the tab button to be clickable
    documents_tab_button = wait.until(EC.element_to_be_clickable(documents_tab_locator))
    # print("'Documents' tab button is clickable.")

    # print("Clicking the 'Documents' tab button...")
    documents_tab_button.click()
    # print("Clicked 'Documents' tab button.")
    # ---------------------------------------------------------------------

    # --- Wait for content within the Documents tab to load (if necessary) ---
    # Assume the overlay appears again. Adjust if needed.
    # print("Waiting for Documents tab content to load (overlay to disappear)...")
    overlay_locator = (By.CSS_SELECTOR, ".overlay")
    wait.until(EC.invisibility_of_element_located(overlay_locator))
    # print("Documents tab content loaded (overlay disappeared).")
    # You might need to wait for a specific element *within* the Documents tab instead, e.g.:
    # wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#v-pills-documentos .some-element")))
    # --------------------------------------------------------------------------
