from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time
from selenium.common.exceptions import (
    StaleElementReferenceException,
    TimeoutException,
)
from pathlib import Path
import os

__all__ = ["DEFAULT_WAIT", "RESO_URL", "initialize_driver", "login_to_reso"]

DEFAULT_WAIT = 60
# BASE_PATH = r"C:\Users\andmn\OneDrive - Komatsu Ltd\REPARACION"
RESO_URL = "https://resoplus.komatsu.cl/"
CHROME_DRIVER_PATH = os.path.abspath(os.path.join(Path(__file__).parents[5], "config", "chromedriver.exe"))


def initialize_driver():
    """Initialize and return configured Chrome WebDriver"""
    options = webdriver.ChromeOptions()
    options.add_experimental_option("detach", True)
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--ignore-ssl-errors")
    options.add_argument("--start-maximized")

    service = Service()
    return webdriver.Chrome(service=service, options=options)


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


def login_to_reso(driver, wait):
    """
    Handles the login process for Reso application with retries for stale elements.
    """

    # Navigate to Reso
    driver.get(RESO_URL)

    # Login sequence with improved stability
    click_element(wait, By.ID, "btnLoginAzure")

    # Enter Email
    email_field = wait.until(EC.presence_of_element_located((By.ID, "i0116")))
    email_field.clear()
    email_field.send_keys(os.environ["RESO_EMAIL"])

    # Click Next after email
    click_element(wait, By.ID, "idSIButton9")

    # Enter Password
    password_field = wait.until(EC.presence_of_element_located((By.ID, "i0118")))
    password_field.clear()
    password_field.send_keys(os.environ["RESO_PASSWORD"])

    # Click Next after password (with extra wait)
    time.sleep(1)  # Give the page time to process the password
    click_element(wait, By.ID, "idSIButton9")

    # Wait for Stay Signed In screen & Click "Yes" (with extra wait)
    time.sleep(1)  # Give the page time to load the Stay Signed In screen
    click_element(wait, By.ID, "idSIButton9")

    return True
