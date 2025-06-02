from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.ie.webdriver import WebDriver

__all__ = [
    "search_service_order",
    "click_see_service_order",
    "navigate_to_quotation_tab",
    "navigate_to_documents_tab",
    "close_service_order_view",
]


def close_service_order_view(wait: WebDriverWait):

    # --- Wait for the close button/div to be clickable ---
    close_button_locator = (By.ID, "closePCI")
    # Wait for the element to be clickable
    close_button = wait.until(EC.element_to_be_clickable(close_button_locator))
    close_button.click()

    # --- Wait for navigation back to the previous screen ---
    previous_screen_element_locator = (By.ID, "codigoBuscar")
    wait.until(EC.presence_of_element_located(previous_screen_element_locator))


def check_and_close_error_popup(driver: WebDriver, wait: WebDriverWait):
    error_popup_title_xpath = (
        "//span[@id='ui-id-1' and contains(text(), 'Please try again or contact your service administrator.')]"
    )
    # Check for the presence of the popup's title
    popup_title_element = driver.find_element(By.XPATH, error_popup_title_xpath)
    if popup_title_element.is_displayed():
        # 1. Generic close button in a ui-dialog:
        close_button_xpath = "//div[contains(@class,'ui-dialog') and .//span[@id='ui-id-1' and contains(text(), 'Please try again or contact your service administrator.')]]//button[contains(@class,'ui-dialog-titlebar-close')]"
        close_button = wait.until(EC.element_to_be_clickable((By.XPATH, close_button_xpath)))
        close_button.click()
        # Wait a moment for the popup to disappear
        wait.until(EC.invisibility_of_element_located((By.XPATH, error_popup_title_xpath)))


def search_service_order(driver: WebDriver, wait: WebDriverWait, service_order):

    # --- Find the input field, clear it, and enter the service order ---
    search_input_locator = (By.ID, "codigoBuscar")
    search_input = wait.until(EC.presence_of_element_located(search_input_locator))
    # Entering service order: {service_order}
    search_input.clear()
    search_input.send_keys(str(service_order))  # Ensure input is string

    # --- Find and click the search button ---
    search_button_locator = (By.ID, "buttonBuscar")
    search_button = wait.until(EC.element_to_be_clickable(search_button_locator))
    search_button.click()

    # --- Wait for the filtering results (assuming the overlay reappears) ---
    overlay_locator = (By.CSS_SELECTOR, ".overlay")
    wait.until(EC.invisibility_of_element_located(overlay_locator))


def click_see_service_order(driver, wait: WebDriverWait):
    try:
        check_and_close_error_popup(driver, wait)
    except Exception:
        pass
    # --- Wait for the results table ---
    webgrid_locator = (By.ID, "WebGrid")
    wait.until(EC.presence_of_element_located(webgrid_locator))

    # Locator targets the button with class 'verOrdenServicio' in the first row (implicitly)
    see_order_button_locator = (By.CSS_SELECTOR, "button.verOrdenServicio")
    # Waiting for the 'See service order' button to be clickable...
    see_order_button = wait.until(EC.element_to_be_clickable(see_order_button_locator))

    # Clicking the 'See service order' button...
    see_order_button.click()  # Using standard click

    # --- ADD WAIT HERE ---
    overlay_locator = (By.CSS_SELECTOR, ".overlayContent")  # Or relevant overlay
    try:
        # Waiting for overlay '{overlay_locator[1]}' after clicking 'See service order'...")
        wait.until(EC.invisibility_of_element_located(overlay_locator))
    except TimeoutException:
        raise "Timed out waiting for overlay after 'See service order' click."
    # --- END ADDED WAIT ---


def navigate_to_quotation_tab(wait: WebDriverWait):

    # --- Wait for the Quotation tab button to be ready and click it ---
    quotation_tab_locator = (By.ID, "v-pills-presupuesto-tab")

    # Wait for the tab button to be clickable
    quotation_tab_button = wait.until(EC.element_to_be_clickable(quotation_tab_locator))
    quotation_tab_button.click()

    # --- Wait for content within the Quotation tab to load (if necessary) ---
    overlay_locator = (By.CSS_SELECTOR, ".overlay")
    wait.until(EC.invisibility_of_element_located(overlay_locator))


def navigate_to_documents_tab(wait: WebDriverWait):

    # --- Wait for the Documents tab button to be ready and click it ---
    documents_tab_locator = (By.ID, "v-pills-documentos-tab")
    documents_tab_button = wait.until(EC.element_to_be_clickable(documents_tab_locator))
    documents_tab_button.click()

    # --- Wait for content within the Documents tab to load (if necessary) ---
    overlay_locator = (By.CSS_SELECTOR, ".overlay")
    wait.until(EC.invisibility_of_element_located(overlay_locator))
