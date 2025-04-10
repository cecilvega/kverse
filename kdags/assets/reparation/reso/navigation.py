from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time


def click_presupuesto(driver, wait):
    # ---  Clicking the SPAN containing "Presupuesto" ---
    # Use XPath to find the specific <span> element containing the text
    presupuesto_locator = (By.XPATH, "//span[normalize-space(text())='Presupuesto']")
    # Wait for the span element to be present on the page
    presupuesto_span = wait.until(EC.presence_of_element_located(presupuesto_locator))
    # Scroll the element into view to ensure it's not off-screen
    driver.execute_script("arguments[0].scrollIntoView(true);", presupuesto_span)
    time.sleep(0.5)  # Brief pause after scroll
    wait.until(EC.element_to_be_clickable(presupuesto_locator))
    presupuesto_span.click()  # Click the span
    print("Waiting for loading overlay to disappear...")
    wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, ".overlay")))
    print("Loading overlay disappeared.")
