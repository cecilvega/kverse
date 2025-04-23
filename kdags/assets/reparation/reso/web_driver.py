from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.ie.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time
from selenium.common.exceptions import (
    StaleElementReferenceException,
    TimeoutException,
)
from pathlib import Path
import os
from selenium.common.exceptions import ElementClickInterceptedException
import dagster as dg

__all__ = ["DEFAULT_WAIT", "RESO_URL", "initialize_driver", "login_to_reso", "retry_on_interception"]

DEFAULT_WAIT = 60
# BASE_PATH = r"C:\Users\andmn\OneDrive - Komatsu Ltd\REPARACION"
RESO_URL = "https://resoplus.komatsu.cl/"
CHROME_DRIVER_PATH = os.path.abspath(os.path.join(Path(__file__).parents[5], "config", "chromedriver.exe"))


def retry_on_interception(
    context: dg.AssetExecutionContext,
    action_function,  # The function to call (e.g., navigate_to_quotation_tab)
    max_retries: int = 3,  # How many times to retry
    delay_seconds: int = 5,  # How long to wait between retries
    **kwargs,  # Keyword arguments to pass to action_function
):
    """
    Calls a given function and retries if ElementClickInterceptedException occurs.

    Args:
        action_function: The function to execute.
        max_retries: Maximum number of retry attempts.
        delay_seconds: Delay between retries in seconds.
        **kwargs: Keyword arguments for action_function.

    Returns:
        The result of the action_function if successful.

    Raises:
        ElementClickInterceptedException: If the action still fails after all retries.
        Exception: Any other exception raised by action_function.
    """
    last_exception = None
    for attempt in range(max_retries + 1):  # Initial attempt + max_retries
        try:
            result = action_function(**kwargs)
            if attempt > 0:
                context.log.info(f"Action '{action_function.__name__}' succeeded on attempt {attempt + 1}.")
            return result  # Success, return the result
        except ElementClickInterceptedException as e:
            last_exception = e
            if attempt < max_retries:
                context.log.warning(
                    f"Action '{action_function.__name__}' intercepted on attempt {attempt + 1}. "
                    f"Retrying in {delay_seconds}s..."
                )
                time.sleep(delay_seconds)
            else:
                context.log.error(
                    f"Action '{action_function.__name__}' failed after {max_retries + 1} attempts due to interception."
                )
                raise last_exception  # Re-raise the exception after final attempt
        # Let other exceptions propagate immediately
        except Exception as e:
            context.log.error(
                f"Action '{action_function.__name__}' failed with non-interception error: {e}", exc_info=True
            )
            raise e

    # This part should technically not be reached if logic is correct, but as a fallback:
    if last_exception:
        raise last_exception
    else:
        # Should not happen if max_retries >= 0
        raise RuntimeError(f"Retry loop completed without success or defined exception for {action_function.__name__}")


def initialize_driver() -> WebDriver:
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
