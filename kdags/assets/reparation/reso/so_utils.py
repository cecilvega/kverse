import re
from pathlib import Path
from urllib.parse import urlparse

import dagster as dg
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

__all__ = ["extract_quotation_details", "extract_document_links", "has_quotation", "has_documents"]


def find_and_extract_optional(
    search_context: WebElement,
    locator,
    extract_logic,
    default_value=None,
):
    """
    Tries to find an element and apply extraction logic.
    Returns default_value on ANY exception during find or extract.
    (Simplified version based on user request)
    """
    try:
        element = search_context.find_element(locator[0], locator[1])
        return extract_logic(element)
    except Exception as e:
        # logging.warning(f"Error extracting field with locator {locator}: {e}")
        return default_value


def extract_version_po(element: WebElement):
    """Extracts Version and PO Status from a given element's text."""
    version, po_status = None, None
    try:
        text = element.text
        version_match = re.search(r"Version (\d+):", text)
        if version_match:
            version = int(version_match.group(1))
        po_status_match = re.search(r"Purchase Order:\s*([^\n]+)", text, re.IGNORECASE)
        if po_status_match:
            po_status = po_status_match.group(1).strip()
    except Exception as e:
        # logging.warning(f"Error during extract_version_po: {e}")
        # Return None tuple on error within this specific logic
        return None, None
    return version, po_status


def extract_datetime(element: WebElement):
    """Extracts combined date and time string from child elements."""
    try:
        # Find child elements relative to the passed 'element' (which is i.fecha)
        day = element.find_element(By.TAG_NAME, "dia").text
        month = element.find_element(By.TAG_NAME, "mes").text
        year_time = element.find_element(By.TAG_NAME, "año").text
        return f"{day} {month} {year_time}".strip()
    except Exception as e:
        # logging.warning(f"Error during extract_datetime: {e}")
        return None  # Return None on error within this specific logic


def extract_remarks(element: WebElement):
    """Extracts remarks text, handling the 'Remarks:' prefix."""
    try:
        text = element.text
        if "Remarks:" in text:
            # Use maxsplit=1 to avoid issues if ':' appears in the remark itself
            return text.split("Remarks:", 1)[1].strip()
        else:
            # Return the full text if the prefix is missing
            return text.strip()
    except Exception as e:
        # logging.warning(f"Error during extract_remarks: {e}")
        return None  # Return None on error within this specific logic


def extract_quotation_details(wait: WebDriverWait, service_order: str) -> dict:

    quotation_data = {"service_order": service_order}

    # --- Locate the main container & entry block (critical steps) ---
    quotation_container_locator = (By.ID, "divPres1")
    wait.until(EC.visibility_of_element_located(quotation_container_locator))

    first_entry_locator = (By.CSS_SELECTOR, "#divPres1 > div:first-child > div")
    first_entry_element = wait.until(EC.visibility_of_element_located(first_entry_locator))

    details_container = first_entry_element.find_element(By.CSS_SELECTOR, "div > div")
    most_recent_detail_block = details_container.find_element(By.CSS_SELECTOR, "div:first-child > div:last-child")

    # --- Extract data using the simplified helper and module-level logic functions ---

    # Version and Purchase Order Status
    version_po_tuple = find_and_extract_optional(
        search_context=first_entry_element,
        locator=(By.CSS_SELECTOR, "div > span > i"),
        extract_logic=extract_version_po,  # Pass the module-level function
        default_value=(None, None),
    )
    # Ensure version_po_tuple is actually a tuple before unpacking
    version_po_tuple = version_po_tuple if isinstance(version_po_tuple, tuple) else (None, None)
    quotation_data["version"], quotation_data["purchase_order_status"] = version_po_tuple

    # Date/Time
    quotation_data["date_time_str"] = find_and_extract_optional(
        search_context=most_recent_detail_block,
        locator=(By.CSS_SELECTOR, "i.fecha"),
        extract_logic=extract_datetime,  # Pass the module-level function
    )

    # User (using lambda as logic is simple)
    quotation_data["user"] = find_and_extract_optional(
        search_context=most_recent_detail_block,
        locator=(By.XPATH, ".//span[contains(normalize-space(), 'User:')]"),
        extract_logic=lambda el: el.text.split("User:", 1)[1].strip(),
    )

    # Amount (using lambda)
    quotation_data["amount"] = find_and_extract_optional(
        search_context=most_recent_detail_block,
        locator=(By.XPATH, ".//span[contains(normalize-space(), 'Amount:')]"),
        extract_logic=lambda el: el.text.split("Amount:", 1)[1].strip(),
    )

    # Remarks
    quotation_data["remarks"] = find_and_extract_optional(
        search_context=most_recent_detail_block,
        locator=(By.CSS_SELECTOR, "span.overflowText"),
        extract_logic=extract_remarks,  # Pass the module-level function
    )

    # Download URL (using lambda)
    quotation_data["download_url"] = find_and_extract_optional(
        search_context=first_entry_element,
        locator=(By.TAG_NAME, "a"),
        extract_logic=lambda el: el.get_attribute("href"),
    )

    # logging.info(f"Finished extracting quotation details for SO {service_order}.")
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


def has_quotation(context: dg.AssetExecutionContext, wait: WebDriverWait) -> bool:

    # 1. Check for the explicit "No Quotation" message
    no_quotation_elements = wait._driver.find_elements(
        By.XPATH,
        "//div[@id='v-pills-presupuesto']//span[@class='error']/i[contains(normalize-space(), 'Does not have Quotation.')]",
    )
    if no_quotation_elements:
        context.log.info(f"Found explicit 'No Quotation'.")
        return False  # Explicit message means no quotation
    else:
        return True


def has_documents(context, wait: WebDriverWait) -> bool:  # Replace 'context' type if not dg.AssetExecutionContext
    """
    Checks if the 'v-pills-documentos' tab contains any actual document links.

    It looks for the presence of 'a' tags with the class 'documento' inside the
    div with id 'v-pills-documentos'. This matches the elements targeted by
    the extract_document_links function.

    Args:
        context: The execution context (used for logging). Replace with actual type if needed.
        wait: Selenium WebDriverWait instance.

    Returns:
        True if any document links ('a.documento') are found within the
        'v-pills-documentos' container, False otherwise.
    """
    # Locator for the container where documents (or the 'no documents' message) appear
    documents_container_locator = (By.ID, "v-pills-documentos")
    # Locator for the actual document links that extract_document_links targets
    document_link_locator = (By.CSS_SELECTOR, "#v-pills-documentos a.documento")
    # Optional: Locator for the specific "no documents" message if you want finer-grained logging
    no_docs_message_locator = (
        By.XPATH,
        "//div[@id='v-pills-documentos']//span[@class='error2']/i[normalize-space()='No documents have been uploaded.']",
    )

    try:
        # 1. Ensure the main container is at least present.
        #    If not, we can immediately say there are no documents.
        wait.until(EC.presence_of_element_located(documents_container_locator))
        context.log.debug("Documents container '#v-pills-documentos' is present.")

        # 2. Check for the presence of *any* document links.
        #    Using find_elements is non-blocking if elements aren't found immediately.
        document_elements = wait._driver.find_elements(*document_link_locator)

        if document_elements:
            # If the list is not empty, we found document links.
            context.log.info(f"Found {len(document_elements)} document links (a.documento). Returning True.")
            return True
        else:
            # If no document links are found, check for the explicit "no documents" message for confirmation/logging
            no_docs_elements = wait._driver.find_elements(*no_docs_message_locator)
            if no_docs_elements:
                context.log.info(
                    "No document links (a.documento) found. Explicit 'No documents have been uploaded' message detected. Returning False."
                )
            else:
                context.log.info(
                    "No document links (a.documento) found in '#v-pills-documentos', and no explicit 'no documents' message detected either. Returning False."
                )
            return False

    except Exception as e:
        # If the container itself times out finding, or another Selenium error occurs
        context.log.warning(
            f"Could not reliably determine presence of documents due to error: {e}. Assuming no documents. Returning False."
        )
        return False
