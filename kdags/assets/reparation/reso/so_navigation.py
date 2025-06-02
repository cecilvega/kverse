from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.ie.webdriver import WebDriver
from selenium.common.exceptions import TimeoutException, NoSuchElementException

__all__ = [
    "search_service_order",
    "click_see_service_order",
    "navigate_to_quotation_tab",
    "navigate_to_documents_tab",
    "close_service_order_view",
    "check_and_close_error_popup",
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


# --- Helper function to check and close the error popup (User's preferred style, now with fallback) ---
def check_and_close_error_popup(driver: WebDriver, wait: WebDriverWait) -> bool:
    """
    Checks for the presence of the error popup and closes it if found.
    Attempts to close via the title bar button first, then via a footer close element.
    Returns True if the popup was found and successfully closed by either method, False otherwise.
    """
    error_popup_title_xpath = (
        "//span[@id='ui-id-1' and contains(text(), 'Please try again or contact your service administrator.')]"
    )
    # XPath for the primary (title bar) close button
    title_bar_close_button_xpath = "//div[contains(@class,'ui-dialog') and .//span[@id='ui-id-1' and contains(text(), 'Please try again or contact your service administrator.')]]//button[contains(@class,'ui-dialog-titlebar-close')]"

    # XPath for the fallback (footer) close button div
    # This targets the dialog with the specific title, then finds the 'cerrarModal' div in its footer.
    footer_cerrar_modal_xpath = "//div[contains(@class,'ui-dialog') and contains(@class,'alertError') and @aria-labelledby='ui-id-1']//div[contains(@class,'dialog-footer')]//div[contains(@class,'cerrarModal')]"

    try:
        # Check for the presence of the popup's title
        popup_title_element = driver.find_element(By.XPATH, error_popup_title_xpath)

        if popup_title_element.is_displayed():
            print("Info: Error popup detected. Attempting to close.")

            # Attempt 1: Title bar close button
            try:
                print("Info: Attempting to close popup via title bar button.")
                title_close_button = wait.until(EC.element_to_be_clickable((By.XPATH, title_bar_close_button_xpath)))
                title_close_button.click()
                print("Info: Clicked title bar popup close button.")
                # Wait for the popup to disappear by checking invisibility of its title
                wait.until(EC.invisibility_of_element_located((By.XPATH, error_popup_title_xpath)))
                print("Info: Error popup closed successfully via title bar button.")
                return True
            except Exception as e1:  # Catches TimeoutException or other errors for the first attempt
                print(f"Warning: Failed to close popup using title bar button: {type(e1).__name__} - {str(e1)}.")
                print("Info: Attempting to close popup via footer 'cerrarModal' div as a fallback.")

                # Attempt 2: Footer "cerrarModal" div
                try:
                    footer_close_button = wait.until(EC.element_to_be_clickable((By.XPATH, footer_cerrar_modal_xpath)))
                    footer_close_button.click()
                    print("Info: Clicked popup footer 'cerrarModal' div.")
                    # Wait for the popup to disappear by checking invisibility of its title
                    wait.until(EC.invisibility_of_element_located((By.XPATH, error_popup_title_xpath)))
                    print("Info: Error popup closed successfully via footer 'cerrarModal' div.")
                    return True
                except TimeoutException:
                    print(
                        "Warning: Timeout while trying to click footer 'cerrarModal' div or wait for popup to close after fallback."
                    )
                    return False
                except Exception as e2:
                    print(
                        f"Warning: An exception occurred while trying to close the popup via footer 'cerrarModal' div: {type(e2).__name__} - {str(e2)}"
                    )
                    return False
        else:
            # Popup title element is found but not displayed
            # print("Info: Error popup title found but not displayed.") # Optional logging
            return False

    except NoSuchElementException:
        # This is the normal case when the popup (specifically, its title) is not present
        # print("Info: No error popup title found.") # Optional: can be verbose if enabled
        return False
    except Exception as e:
        # Catch any other unexpected errors during the initial check for the popup title
        print(f"Warning: An unexpected exception occurred while checking for the popup: {type(e).__name__} - {str(e)}")
        return False


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
