from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
import time


def extract_docs_data(driver, wait, max_retries=3):
    """
    Extracts document information from the page and returns a list of dictionaries
    with keys "subtitle" and "blob_url". Returns None if no documents are found.
    This version re-finds each document element by its index if a stale element error occurs.
    """
    # First check if there are any error messages indicating no documents
    error_messages = driver.find_elements(By.CLASS_NAME, "error2")
    no_docs_found = True
    if error_messages:
        for msg in error_messages:
            if "No documents have been uploaded" not in msg.text:
                no_docs_found = False
                break
        if no_docs_found:
            print("No documents found in this service order - skipping")
            return None

    try:
        # Wait for the container and document elements to be present
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".panelComponenteDocumentos")))
        documents = wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".panelComponenteDocumentos a.documento"))
        )
    except TimeoutException:
        print("No document elements found - skipping this service order")
        return None

    # If we get here but find no documents, return None
    if not documents:
        print("No documents found in the panels - skipping this service order")
        return None

    docs_data = []
    # Loop by index so that we can re-find the element if it becomes stale
    for index in range(len(documents)):
        for attempt in range(max_retries):
            try:
                # Re-find the element by index every time
                doc = driver.find_elements(By.CSS_SELECTOR, ".panelComponenteDocumentos a.documento")[index]
                blob_url = doc.get_attribute("href").strip()

                spans = doc.find_elements(By.TAG_NAME, "span")
                display_title = ""
                file_title = ""
                skip_next = False
                for i, span in enumerate(spans):
                    try:
                        text = span.text.strip()
                    except Exception:
                        continue
                    if skip_next:
                        skip_next = False
                        continue
                    if not text:
                        continue
                    if text.lower() == "file title:":
                        if i + 1 < len(spans):
                            try:
                                file_title = spans[i + 1].text.strip()
                            except Exception:
                                file_title = ""
                        skip_next = True
                    elif not display_title:
                        display_title = text
                full_title = f"{display_title} - {file_title}" if file_title else display_title
                docs_data.append({"subtitle": full_title, "blob_url": blob_url})
                break  # If succeeded, exit retry loop for this element.
            except StaleElementReferenceException as e:
                if attempt == max_retries - 1:
                    print(f"Failed to process document at index {index}: {e}")
                time.sleep(1)  # Add a small delay before retry

            except Exception as e:
                print(f"Error processing document at index {index}: {e}")
                break

    # If we processed everything but found no valid documents, return None
    if not docs_data:
        print("No valid documents could be extracted - skipping this service order")
        return None

    return docs_data


def click_see_service_order(wait, max_retries=3):
    """Clicks the 'See service order' button in the first row of the WebGrid table."""
    # First ensure the table is present
    wait.until(EC.presence_of_element_located((By.ID, "WebGrid")))

    for attempt in range(max_retries):
        try:
            # Wait for any loading overlays to disappear first
            wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, ".loading-overlay")))

            # Now wait for the row and button
            first_row = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#WebGrid tbody tr")))

            see_order_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button.verOrdenServicio")))

            # Try to ensure the element is actually clickable
            wait._driver.execute_script("arguments[0].scrollIntoView(true);", see_order_button)

            # Add a small wait for any animations to complete
            wait.until(lambda x: see_order_button.is_displayed() and see_order_button.is_enabled())

            # Try to remove any overlays programmatically
            wait._driver.execute_script(
                """
                document.querySelectorAll('.loading-overlay').forEach(e => e.remove());
                document.querySelectorAll('.modal-backdrop').forEach(e => e.remove());
            """
            )

            # Finally attempt the click
            wait._driver.execute_script("arguments[0].click();", see_order_button)
            print("Clicked 'See service order' button successfully.")
            return

        except (StaleElementReferenceException, TimeoutException) as e:
            print(f"Retrying due to error: {str(e)} (attempt {attempt + 1}/{max_retries})...")

        except Exception as e:
            print(f"Unexpected error: {str(e)}")
            if attempt == max_retries - 1:
                raise

    print("Failed to click 'See service order' button after retries.")
