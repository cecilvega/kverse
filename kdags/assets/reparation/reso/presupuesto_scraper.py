from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time
import re
import json
from pathlib import Path
from urllib.parse import urlparse
from selenium.common.exceptions import (
    NoSuchElementException,
)  # Keep this import for potential future use or other parts of your code


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


def search_service_order(driver, wait, service_order):
    """
    Searches for a specific service order on the page and waits for results.

    Args:
        driver: The Selenium WebDriver instance.
        wait: The WebDriverWait instance.
        service_order: The service order number (int or str) to search for.
    """
    # --- Find the input field, clear it, and enter the service order ---
    print(f"Locating the service order input field (codigoBuscar)...")
    search_input_locator = (By.ID, "codigoBuscar")
    search_input = wait.until(EC.presence_of_element_located(search_input_locator))
    print(f"Entering service order: {service_order}")
    search_input.clear()
    search_input.send_keys(str(service_order))  # Ensure input is string
    # ---------------------------------------------------------------------

    # --- Find and click the search button ---
    print("Locating the search button (buttonBuscar)...")
    search_button_locator = (By.ID, "buttonBuscar")
    search_button = wait.until(EC.element_to_be_clickable(search_button_locator))
    print("Clicking the search button...")
    search_button.click()
    # -----------------------------------------

    # --- Wait for the filtering results (assuming the overlay reappears) ---
    print("Waiting for search results filtering overlay to disappear...")
    overlay_locator = (By.CSS_SELECTOR, ".overlay")
    # IMPORTANT: This assumes the *same* overlay with class 'overlay' is used for filtering.
    wait.until(EC.invisibility_of_element_located(overlay_locator))
    print("Search results filtering complete.")
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


def extract_quotation_details(driver, wait, service_order):
    """
    Extracts details from the most recent quotation entry displayed.

    Args:
        driver: The Selenium WebDriver instance.
        wait: The WebDriverWait instance.
        service_order: The service order number associated with this quotation.

    Returns:
        A dictionary containing the extracted quotation details,
        or None if the container is not found.
    """
    # --- Locate the main container for quotations ---
    # print("Waiting for Quotation container (divPres1)...")
    quotation_container_locator = (By.ID, "divPres1")
    quotation_container = wait.until(EC.visibility_of_element_located(quotation_container_locator))
    # print("Quotation container found and visible.")

    # --- Locate the first/most recent entry block within the container ---
    # print("Locating the first quotation entry block...")
    first_entry_locator = (By.CSS_SELECTOR, "#divPres1 > div:first-child > div")
    first_entry_element = wait.until(EC.visibility_of_element_located(first_entry_locator))
    # print("First quotation entry block found and visible.")

    # --- Extract data from the first entry block ---
    quotation_data = {}

    # Version and Purchase Order Status
    version_po_element = first_entry_element.find_element(By.CSS_SELECTOR, "div > span > i")
    version_po_text = version_po_element.text
    # print(f"Found Version/PO text block: {version_po_text}")
    version_match = re.search(r"Version (\d+):", version_po_text)
    quotation_data["version"] = int(version_match.group(1)) if version_match else None
    po_status_match = re.search(r"Purchase Order:\s*([^\n]+)", version_po_text, re.IGNORECASE)
    quotation_data["purchase_order_status"] = po_status_match.group(1).strip() if po_status_match else None

    # print(f"Extracted Version: {quotation_data['version']}")
    print(f"Extracted PO Status: {quotation_data['purchase_order_status']}")

    # Locate the block containing the most recent details
    details_container = first_entry_element.find_element(By.CSS_SELECTOR, "div > div")
    most_recent_detail_block = details_container.find_element(By.CSS_SELECTOR, "div:first-child > div:last-child")

    # Date/Time
    date_block = most_recent_detail_block.find_element(By.CSS_SELECTOR, "i.fecha")
    day = date_block.find_element(By.TAG_NAME, "dia").text
    month = date_block.find_element(By.TAG_NAME, "mes").text
    year_time = date_block.find_element(By.TAG_NAME, "año").text
    quotation_data["date_time_str"] = f"{day} {month} {year_time}".strip()

    # User
    user_element = most_recent_detail_block.find_element(By.XPATH, ".//span[contains(normalize-space(), 'User:')]")
    quotation_data["user"] = user_element.text.split("User:")[1].strip()

    # Amount
    amount_element = most_recent_detail_block.find_element(By.XPATH, ".//span[contains(normalize-space(), 'Amount:')]")
    quotation_data["amount"] = amount_element.text.split("Amount:")[1].strip()

    # Remarks
    remarks_element = most_recent_detail_block.find_element(By.CSS_SELECTOR, "span.overflowText")
    remarks_full_text = remarks_element.text
    quotation_data["remarks"] = (
        remarks_full_text.split("Remarks:")[1].strip() if "Remarks:" in remarks_full_text else remarks_full_text.strip()
    )

    # Add the service order
    quotation_data["service_order"] = service_order

    # Extract Download URL
    print("Extracting download URL...")
    download_link_element = first_entry_element.find_element(By.TAG_NAME, "a")
    download_url = download_link_element.get_attribute("href")
    quotation_data["download_url"] = download_url
    print(f"Found Download URL: {download_url}")

    print("\nExtracted Quotation Data (within function):")
    print(json.dumps(quotation_data, indent=4))

    return quotation_data


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


def extract_document_links(driver, wait):

    documents_list = []
    documents_container_locator = (By.ID, "v-pills-documentos")
    document_link_locator = (By.CSS_SELECTOR, "#v-pills-documentos a.documento")

    # print("Waiting for the documents container to be visible...")
    wait.until(EC.visibility_of_element_located(documents_container_locator))
    # print("Documents container is visible.")

    # print("Waiting for document links to be present...")
    wait.until(EC.presence_of_element_located(document_link_locator))
    # print("Document links are present.")

    document_elements = driver.find_elements(*document_link_locator)
    # print(f"Found {len(document_elements)} document links.")

    if not document_elements:
        # print("No documents found in the panel.")
        return documents_list

    # Extract info for each document
    for i, element in enumerate(document_elements):
        # print(f"Processing document {i+1}...")
        doc_info = {}
        url = element.get_attribute("href")
        doc_info["url"] = url

        # **** ADDED: Extract Filename from URL ****
        file_name = None
        if url:
            try:
                parsed_url = urlparse(url)
                url_path = parsed_url.path
                # Use pathlib to reliably get the last part of the path
                file_name = Path(url_path).name
            except Exception as e:
                print(f"  Error parsing URL to get filename for doc {i+1}: {e}")
                file_name = None  # Ensure it's None if parsing failed

        # **** ADDED: Assertion for filename ****
        assert file_name, f"Could not extract filename from URL for doc {i+1}: {url}"
        doc_info["file_name"] = file_name
        # *****************************************

        file_title = None
        file_subtitle = None

        # Find all direct child spans of the link element
        all_spans = element.find_elements(By.XPATH, "./span")  # Direct child spans

        if all_spans:
            # Title Logic (same as before)
            if all_spans[0].text and "File Title:" not in all_spans[0].text.strip():
                file_title = all_spans[0].text.strip()
            elif len(all_spans) > 1 and all_spans[1].text and "File Title:" not in all_spans[1].text.strip():
                file_title = all_spans[1].text.strip()

            # Subtitle Logic (same as before)
            for idx, span in enumerate(all_spans):
                span_text = span.text.strip()
                if "File Title:" in span_text and idx + 1 < len(all_spans):
                    file_subtitle = all_spans[idx + 1].text.strip()
                    if file_title is None and idx > 0:
                        file_title = all_spans[idx - 1].text.strip()
                    break

        # Fallback title (same as before)
        if file_title is None:
            full_text = element.text
            lines = [line.strip() for line in full_text.split("\n") if line.strip() and "(See more" not in line]
            file_title = " ".join(lines) if lines else "Unknown Title"

        # Assign to dictionary
        doc_info["file_title"] = file_title.replace("\n", " ").strip() if file_title else None
        doc_info["file_subtitle"] = file_subtitle.replace("\n", " ").strip() if file_subtitle else None

        # print(f"  Title: {doc_info['file_title']}")
        # print(f"  Subtitle: {doc_info['file_subtitle']}")
        # print(f"  Filename: {doc_info['file_name']}")  # Added print
        # print(f"  URL: {doc_info['url']}")

        # Only add if we got a URL (filename assertion already checked)
        documents_list.append(doc_info)

    # print(f"Extracted {len(documents_list)} document records.")
    return documents_list
