from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


def wait_for_page_load(wait):
    """Wait for all dynamic elements to load."""
    try:
        # Wait for jQuery to complete if it exists
        wait._driver.execute_script(
            """
            if (typeof jQuery != 'undefined') {
                return jQuery.active == 0;
            }
            return true;
        """
        )

        # Wait for document ready state
        wait.until(lambda driver: driver.execute_script("return document.readyState") == "complete")

        # Wait for any loading overlays to disappear
        wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, ".loading-overlay")))
    except Exception as e:
        print(f"Warning: Page load wait encountered an error: {str(e)}")


def ensure_clickable(driver, element):
    """Ensure an element is actually clickable."""
    driver.execute_script(
        """
        // Scroll element into view
        arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});
        
        // Remove any overlays
        document.querySelectorAll('.loading-overlay, .modal-backdrop').forEach(e => e.remove());
        
        // Ensure element is visible
        arguments[0].style.opacity = '1';
        arguments[0].style.visibility = 'visible';
        arguments[0].style.display = 'block';
    """,
        element,
    )
