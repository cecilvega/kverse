from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time
from selenium.webdriver.support.ui import WebDriverWait

__all__ = ["click_reportabilidad", "click_component_status", "click_presupuesto"]


def click_reportabilidad(driver, wait: WebDriverWait):
    """Clicks the main 'Reportabilidad' section."""
    print("--- Clicking Reportabilidad ---")
    reportabilidad_locator = (
        By.XPATH,
        "//span[normalize-space(text())='Reportabilidad']",
    )
    reportabilidad_span = wait.until(EC.presence_of_element_located(reportabilidad_locator))
    driver.execute_script("arguments[0].scrollIntoView(true);", reportabilidad_span)
    wait.until(EC.element_to_be_clickable(reportabilidad_locator))
    reportabilidad_span.click()
    print("Clicked 'Reportabilidad'.")


def click_component_status(driver, wait: WebDriverWait):
    """Clicks the 'Estatus Componente' sub-link."""
    estatus_componente_locator = (By.ID, "submodulo-369")
    estatus_componente_link = wait.until(EC.presence_of_element_located(estatus_componente_locator))
    driver.execute_script("arguments[0].scrollIntoView(true);", estatus_componente_link)
    wait.until(EC.element_to_be_clickable(estatus_componente_locator))
    estatus_componente_link.click()
    time.sleep(1)


def click_presupuesto(driver, wait: WebDriverWait):
    # ---  Clicking the SPAN containing "Presupuesto" ---
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
