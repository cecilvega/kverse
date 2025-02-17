from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from .config import CHROME_DRIVER_PATH, RESO_URL, RESO_CREDENTIALS
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time
from selenium.common.exceptions import (
    StaleElementReferenceException,
    TimeoutException,
)


def initialize_driver():
    """Initialize and return configured Chrome WebDriver"""
    options = webdriver.ChromeOptions()
    options.add_experimental_option("detach", True)
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--ignore-ssl-errors")

    service = Service(executable_path=CHROME_DRIVER_PATH)
    return webdriver.Chrome(service=service, options=options)


# Click Login Button (with retry)
def click_element(by, value, retries=3):
    for attempt in range(retries):
        try:
            element = wait.until(EC.element_to_be_clickable((by, value)))
            # Add a small pause to let the page stabilize
            time.sleep(0.5)
            element.click()
            return True
        except (StaleElementReferenceException, TimeoutException) as e:
            if attempt == retries - 1:
                raise e
            time.sleep(1)
    return False


def login_to_reso(driver, wait):
    """
    Handles the login process for Reso application with retries for stale elements.
    """

    def click_element(wait, by, value, retries=3):
        """Helper function to click elements with retry logic."""
        for attempt in range(retries):
            try:
                element = wait.until(EC.element_to_be_clickable((by, value)))
                # Add a small pause to let the page stabilize
                time.sleep(0.5)
                element.click()
                return True
            except (StaleElementReferenceException, TimeoutException) as e:
                if attempt == retries - 1:
                    raise e
                time.sleep(1)
        return False

    email = RESO_CREDENTIALS["email"]
    password = RESO_CREDENTIALS["password"]

    # Navigate to Reso
    driver.get(RESO_URL)

    # Login sequence with improved stability
    click_element(wait, By.ID, "btnLoginAzure")

    # Enter Email
    email_field = wait.until(EC.presence_of_element_located((By.ID, "i0116")))
    email_field.clear()
    email_field.send_keys(email)

    # Click Next after email
    click_element(wait, By.ID, "idSIButton9")

    # Enter Password
    password_field = wait.until(EC.presence_of_element_located((By.ID, "i0118")))
    password_field.clear()
    password_field.send_keys(password)

    # Click Next after password (with extra wait)
    time.sleep(1)  # Give the page time to process the password
    click_element(wait, By.ID, "idSIButton9")

    # Wait for Stay Signed In screen & Click "Yes" (with extra wait)
    time.sleep(1)  # Give the page time to load the Stay Signed In screen
    click_element(wait, By.ID, "idSIButton9")

    # Navigate to Presupuesto page
    time.sleep(2)  # Wait for login to complete
    driver.get(f"{RESO_URL}/Gestion/Presupuesto")

    return True
