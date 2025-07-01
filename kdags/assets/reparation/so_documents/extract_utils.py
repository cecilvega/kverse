import re
import pdfplumber
import polars as pl
from io import BytesIO

from dagster import AssetExecutionContext
import unicodedata
import re
from kdags.resources.tidyr import DataLake


def extract_page_text(pdf_path_or_bytes, page_num=None, page_range=None):
    """
    Extract text from specific page(s) of a PDF for debugging purposes.

    Args:
        pdf_path_or_bytes: Path to PDF file, bytes, or BytesIO object
        page_num: Single page number (1-indexed)
        page_range: Tuple of (start_page, end_page) inclusive, 1-indexed

    Returns:
        Dict with page numbers as keys and extracted text as values
    """
    import pdfplumber
    from io import BytesIO

    # Handle different input types
    if isinstance(pdf_path_or_bytes, (bytes, BytesIO)):
        if isinstance(pdf_path_or_bytes, bytes):
            pdf_file = BytesIO(pdf_path_or_bytes)
        else:
            pdf_file = pdf_path_or_bytes
    else:
        pdf_file = pdf_path_or_bytes

    extracted_text = {}

    with pdfplumber.open(pdf_file) as pdf:
        total_pages = len(pdf.pages)

        # Determine which pages to extract
        if page_num:
            pages_to_extract = [page_num - 1]  # Convert to 0-indexed
        elif page_range:
            start, end = page_range
            pages_to_extract = range(start - 1, min(end, total_pages))
        else:
            pages_to_extract = range(total_pages)

        for page_idx in pages_to_extract:
            if 0 <= page_idx < total_pages:
                page = pdf.pages[page_idx]
                text = page.extract_text()
                tables = page.extract_tables()

                extracted_text[page_idx + 1] = {
                    "text": text,
                    "tables": tables,
                    "table_count": len(tables) if tables else 0,
                }

    return extracted_text


def extract_table_from_page(page, table_columns, expected_columns=None, section_pattern=None):
    """
    Extract table data from a single PDF page.
    This is the core logic that can be reused by _extract_table().

    Args:
        page: pdfplumber page object
        table_columns: List of expected column names
        expected_columns: Number of expected columns (defaults to len(table_columns))
        section_pattern: Compiled regex pattern for section headers (optional)

    Returns:
        Dict with:
            - found_header: bool indicating if header was found
            - rows: list of processed rows
            - is_section_start: bool indicating if this page starts a new section
            - section_number: section number if found
    """

    normalized_columns = [normalize_text(col).lower() for col in table_columns]
    expected_columns = expected_columns or len(table_columns)

    result = {
        "found_header": False,
        "rows": [],
        "is_section_start": False,
        "section_number": None,
        "should_stop": False,
    }

    # Extract tables and text
    tables = page.extract_tables()
    text = page.extract_text() or ""

    # Check for section start if pattern provided
    if section_pattern and text:
        normalized_text = normalize_text(text)
        match = section_pattern.search(normalized_text)
        if match:
            result["is_section_start"] = True
            # Try to extract section number
            num_match = re.search(r"(\d+)\.-", text)
            if num_match:
                result["section_number"] = num_match.group(1)

    # Add a flag to track if we're processing our target table
    processing_target_table = False

    # Process tables
    for table in tables:
        if not table:
            continue

        for row in table:
            if not row or len(row) < 2:
                continue

            # Skip section header rows
            row_text = " ".join(str(cell) if cell else "" for cell in row)
            if section_pattern:
                normalized_row_text = normalize_text(row_text)
                if section_pattern.search(normalized_row_text):
                    processing_target_table = True  # We found our table!
                    continue

            # Only check for section end AFTER we've started processing our table
            if processing_target_table and row[0] and re.match(r"^\d+\.-\s*", str(row[0])):
                result["should_stop"] = True
                return result

            # Check if this is the header row
            if not result["found_header"] and len(row) >= expected_columns:
                row_normalized = [normalize_text(str(cell)).lower() if cell else "" for cell in row]

                matches = 0
                for norm_col in normalized_columns:
                    for cell in row_normalized:
                        if norm_col in cell:
                            matches += 1
                            break

                if matches == len(normalized_columns):
                    result["found_header"] = True
                    cleaned_row = [str(cell).strip() if cell else "" for cell in row]
                    result["rows"].append({"type": "header", "data": cleaned_row})
                    continue

            # Capture data rows if header was found
            if result["found_header"]:
                non_empty_cells = [cell for cell in row if cell and str(cell).strip()]

                if len(non_empty_cells) >= 2:
                    cleaned_row = [str(cell).strip() if cell else "" for cell in row]
                    result["rows"].append({"type": "data", "data": cleaned_row})

    return result


def normalize_text(text):
    """Remove accents/tildes from text for comparison"""
    return "".join(c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn")


def _extract_table(pdf_bytes, table_title: str, table_columns: list):
    """
    Extract a specific table section from a PDF.
    """
    # Prepare pattern
    normalized_title = normalize_text(table_title)
    pattern = re.compile(rf"\d+\.-\s*{re.escape(normalized_title)}", re.IGNORECASE)

    section_data = {
        "title": table_title,
        "number": None,
        "content": [],
        "found": False,
        "start_page": None,
        "end_page": None,
    }

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        capturing = False
        found_header = False
        current_section_number = None

        for page_num, page in enumerate(pdf.pages, 1):
            tables = page.extract_tables()
            text = page.extract_text() or ""

            # Check each table on the page
            for table in tables:
                if not table:
                    continue

                # Check if this table starts with a section header
                if table[0] and table[0][0] and re.match(r"^\d+\.-", str(table[0][0])):
                    table_section_match = re.match(r"^(\d+)\.-", str(table[0][0]))
                    if table_section_match:
                        table_section_num = table_section_match.group(1)

                        # Check if this is our target section
                        normalized_first_cell = normalize_text(str(table[0][0]))
                        if pattern.search(normalized_first_cell):
                            # Start capturing
                            capturing = True
                            section_data["found"] = True
                            section_data["start_page"] = page_num
                            section_data["number"] = table_section_num
                            current_section_number = table_section_num
                            found_header = False
                        elif capturing and table_section_num != current_section_number:
                            # Different section number - stop capturing
                            section_data["end_page"] = page_num - 1 if page_num > 1 else page_num
                            return section_data

                # If we're capturing, process this table
                if capturing:
                    # Look for header row
                    if not found_header:
                        for i, row in enumerate(table):
                            if not row or len(row) < 2:
                                continue

                            # Skip the section title row
                            if i == 0 and re.match(r"^\d+\.-", str(row[0]) if row[0] else ""):
                                continue

                            # Check if this is the header row
                            row_normalized = [normalize_text(str(cell)).lower() if cell else "" for cell in row]
                            matches = sum(
                                1
                                for norm_col in [normalize_text(col).lower() for col in table_columns]
                                if any(norm_col in cell for cell in row_normalized if cell)
                            )

                            if matches == len(table_columns):
                                found_header = True
                                cleaned_row = [str(cell).strip() if cell else "" for cell in row]
                                section_data["content"].append({"type": "header", "data": cleaned_row})

                                # Process remaining rows in this table as data
                                for data_row in table[i + 1 :]:
                                    if data_row and any(cell and str(cell).strip() for cell in data_row):
                                        cleaned_data = [str(cell).strip() if cell else "" for cell in data_row]
                                        section_data["content"].append({"type": "data", "data": cleaned_data})
                                break
                    else:
                        # We already have a header, just add data rows
                        for row in table:
                            if row and any(cell and str(cell).strip() for cell in row):
                                # Skip if this looks like a new section
                                if row[0] and re.match(r"^\d+\.-", str(row[0])):
                                    continue
                                cleaned_row = [str(cell).strip() if cell else "" for cell in row]
                                section_data["content"].append({"type": "data", "data": cleaned_row})

            # Update end page if we're still capturing
            if capturing:
                section_data["end_page"] = page_num

    return section_data if section_data["found"] else None


def extract_tables(context, reports: list, table_title: str, table_columns: list) -> list:
    """
    Extract the conclusions section from a technical report.
    Returns only section_number, pages, and raw_content.
    """
    dl = DataLake(context)
    tables = []
    for record in reports:
        try:
            # Extract metadata
            service_order = record["service_order"]
            component_serial = record["component_serial"]
            az_path = record["az_path"]

            # Read PDF content
            content = dl.read_bytes(az_path)

            # Extract conclusions section
            table = _extract_table(
                content,
                table_title=table_title,
                table_columns=table_columns,
            )

            # FIX: Check if table is None before accessing properties
            if table and table.get("found"):
                data = {
                    "service_order": service_order,
                    "component_serial": component_serial,
                    "section_number": table.get("number", ""),
                    "pages": (
                        f"{table['start_page']}-{table['end_page']}"
                        if table.get("end_page") != table.get("start_page")
                        else str(table.get("start_page", ""))
                    ),
                    "raw_content": table.get("content", []),
                }
                tables.append(data)
            # If table not found, we just skip it (don't add to tables list)

        except Exception as ex:
            context.log.warning(f"Error processing {record.get('az_path', 'unknown')}: {ex}")

    return tables


def reports_to_tibble(records_list: list) -> pl.DataFrame:
    """Transform records with raw_content into a polars DataFrame."""
    all_rows = []

    for record in records_list:
        base = {k: record.get(k, "") for k in ["service_order", "component_serial", "section_number", "pages"]}

        # Extract header and data rows
        header_row = None
        data_rows = []

        for item in record.get("raw_content", []):
            if item["type"] == "header":
                header_row = item["data"]
            elif item["type"] == "data":
                data_rows.append(item["data"])

        if header_row and data_rows:
            # Process each data row
            offset = 1 if header_row[0] == "" else 0
            cols = header_row[offset:]

            for data_row in data_rows:
                row = {
                    **base,
                    "index": data_row[0],
                    **{
                        c: data_row[i + offset].strip() if i + offset < len(data_row) and data_row[i + offset] else ""
                        for i, c in enumerate(cols)
                        if c
                    },
                }

                all_rows.append(row)
        else:
            # No data, just add base info
            all_rows.append(base)

    if not all_rows:
        return pl.DataFrame({k: [] for k in ["service_order", "component_serial", "section_number", "pages"]})

    df = pl.DataFrame(all_rows)
    base_cols = ["service_order", "component_serial", "section_number", "pages"]
    return df.select([c for c in base_cols + sorted(set(df.columns) - set(base_cols)) if c in df.columns])


def extract_tibble_from_report(context, reports: list, table_title: str, table_columns: list):
    data = extract_tables(
        context,
        reports,
        table_title=table_title,
        table_columns=table_columns,
    )
    df = reports_to_tibble(data)

    # Find which reports were successfully extracted
    extracted_orders = set()
    if df.height > 0:
        extracted_orders = set(df["service_order"].to_list())
        df = df.with_columns(extraction_status=pl.lit("success"))

    # Find which reports had no data
    not_found_reports = [r for r in reports if r["service_order"] not in extracted_orders]

    # Create records for not found
    if not_found_reports:
        not_found_df = pl.DataFrame(
            [
                {
                    "service_order": r["service_order"],
                    "component_serial": r["component_serial"],
                    "section_number": "",
                    "pages": "",
                    "extraction_status": "not_found",
                }
                for r in not_found_reports
            ]
        )

        # Combine both DataFrames
        if df.height > 0:
            df = pl.concat([df, not_found_df], how="diagonal")
        else:
            df = not_found_df

    # Log summary
    found = df.filter(pl.col("extraction_status") == "success").height
    context.log.info(f"Extracted {found}/{len(reports)} tables")

    # TODO:
    # --- NEW LOGIC: Filter final columns ---
    if df.height > 0:
        base_cols = [
            "service_order",
            "component_serial",
            "section_number",
            "pages",
            "index",
            "extraction_status",
        ]

        # Find the actual DataFrame columns that match the requested table_columns
        normalized_target_cols = [normalize_text(c).lower() for c in table_columns]
        matched_data_cols = [
            df_col for df_col in df.columns if normalize_text(df_col).lower() in normalized_target_cols
        ]

        # Combine all columns to keep
        final_cols_to_keep = base_cols + matched_data_cols

        # Select only the columns that exist in the DataFrame
        df = df.select([col for col in final_cols_to_keep if col in df.columns])

    return df
