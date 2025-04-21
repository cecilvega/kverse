import json
import re
from pathlib import Path
from urllib.parse import urlparse

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC


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
