import re
import pdfplumber
import polars as pl
from io import BytesIO

from dagster import AssetExecutionContext

from kdags.resources.tidyr import DataLake


def extract_section(pdf_bytes, section_title: str):
    """
    Extract a specific section from a PDF technical report.
    Handles tables that span multiple pages and stops at the end of the table.

    Args:
        pdf_bytes: PDF file content as bytes
        section_title: Title of the section to extract (without number prefix)

    Returns:
        dict with section content or None if not found
    """
    # Pattern to match section headers like "24.- Conclusiones o comentarios"
    pattern = re.compile(rf"\d+\.-\s*{re.escape(section_title)}", re.IGNORECASE)

    section_data = {
        "title": section_title,
        "number": None,
        "content": [],
        "found": False,
        "start_page": None,
        "end_page": None,
    }

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        capturing = False
        found_header_row = False
        expected_columns = 0

        for page_num, page in enumerate(pdf.pages, 1):
            # Extract tables
            tables = page.extract_tables()

            # Extract text for section detection
            text = page.extract_text()
            if not text:
                continue

            # Check if we should start capturing
            if not capturing and pattern.search(text):
                capturing = True
                section_data["found"] = True
                section_data["start_page"] = page_num

                # Extract section number
                match = re.search(rf"(\d+)\.-\s*{re.escape(section_title)}", text)
                if match:
                    section_data["number"] = match.group(1)

            if capturing:
                # Process tables on this page
                table_found_on_page = False

                for table in tables:
                    if not table:
                        continue

                    for row in table:
                        if not row:
                            continue

                        # Convert row to string for pattern matching
                        row_text = " ".join(str(cell) if cell else "" for cell in row)

                        # Skip the section header row itself
                        if pattern.search(row_text):
                            continue

                        # Check if this is the header row (contains "Conclusiones" and "Serie del vástago")
                        if (
                            not found_header_row
                            and any("Conclusiones" in str(cell) for cell in row)
                            and any("Serie" in str(cell) for cell in row)
                        ):
                            found_header_row = True
                            expected_columns = len([cell for cell in row if cell])  # Count non-empty cells
                            cleaned_row = [str(cell).strip() if cell else "" for cell in row]
                            section_data["content"].append({"type": "table_row", "data": cleaned_row})
                            table_found_on_page = True
                            continue

                        # If we found the header, capture data rows
                        if found_header_row:
                            # Check if this row has the expected structure (right number of columns with data)
                            non_empty_cells = [cell for cell in row if cell and str(cell).strip()]

                            # Stop if we encounter:
                            # 1. A new numbered section
                            # 2. "Fotografías Reporte Técnico" which marks end of conclusions
                            # 3. Rows that don't match the table structure

                            if row[0] and re.match(r"^\d+\.-\s*", str(row[0])):
                                section_data["end_page"] = page_num
                                return section_data

                            if any("Fotografías Reporte Técnico" in str(cell) for cell in row if cell):
                                section_data["end_page"] = page_num
                                return section_data

                            # Only capture rows that look like they belong to the conclusions table
                            # They should either have the same column structure or be a continuation
                            if len(non_empty_cells) >= expected_columns - 1:  # Allow some flexibility
                                # Check if first cell contains "Conclusiones" (data row)
                                print(row)
                                if row[0] and "Conclusiones" in str(row[0]):
                                    cleaned_row = [str(cell).strip() if cell else "" for cell in row]
                                    section_data["content"].append({"type": "table_row", "data": cleaned_row})
                                    table_found_on_page = True
                            elif table_found_on_page:
                                # If we already found table data on this page and now hit different structure, stop
                                section_data["end_page"] = page_num
                                return section_data

                # Update end page
                section_data["end_page"] = page_num

                # If we've found the complete table (header + at least one data row), we can stop
                if found_header_row and len(section_data["content"]) >= 2:
                    # Check if the last row looks like a complete data row
                    last_row = section_data["content"][-1]["data"]
                    if len([cell for cell in last_row if cell]) >= expected_columns - 1:
                        return section_data

    return section_data if section_data["found"] else None


def extract_conclusions(pdf_bytes):
    """
    Extract the conclusions section from a technical report.
    Returns only section_number, pages, and raw_content.
    """
    result = extract_section(pdf_bytes, "Conclusiones o comentarios")

    if not result:
        return None

    return {
        "section_number": result["number"],
        "pages": (
            f"{result['start_page']}-{result['end_page']}"
            if result["end_page"] != result["start_page"]
            else str(result["start_page"])
        ),
        "raw_content": result["content"],
    }


def process_multiple_reports_expanded(context: AssetExecutionContext, records_list: list):
    """
    Process multiple PDF reports and create an expanded dataframe
    where each row from the extracted table becomes a row in the final dataframe.
    """
    dl = DataLake(context)
    all_results = []

    for record in records_list:
        try:
            # Extract metadata
            service_order = record["service_order"]
            component_serial = record["component_serial"]
            az_path = record["az_path"]

            # Read PDF content
            content = dl.read_bytes(az_path)

            # Extract conclusions section
            conclusions = extract_conclusions(content)

            if conclusions and conclusions["raw_content"]:
                # Get section info
                section_number = conclusions["section_number"]
                pages = conclusions["pages"]
                table_title = f"{section_number}.- Conclusiones o comentarios"

                # Find header row and data rows
                header_row = None
                data_rows = []

                for item in conclusions["raw_content"]:
                    if item["type"] == "table_row":
                        row_data = item["data"]

                        # Check if this is a header row (contains column names like "Conclusiones", "Serie del vástago")
                        if not header_row and any(
                            "Conclusiones" in str(cell) or "Serie" in str(cell) for cell in row_data
                        ):
                            header_row = row_data
                        elif header_row and any(row_data):  # This is a data row
                            data_rows.append(row_data)

                if header_row and data_rows:
                    # Clean up header row (remove empty first column if present)
                    if header_row[0] == "":
                        columns = header_row[1:]
                        col_offset = 1
                    else:
                        columns = header_row
                        col_offset = 0

                    # Create a row in the final dataframe for each data row
                    for data_row in data_rows:
                        row_dict = {
                            "service_order": service_order,
                            "component_serial": component_serial,
                            "az_path": az_path,
                            "table_title": table_title,
                            "pages": pages,
                        }

                        # Add columns from the extracted table
                        for i, col_name in enumerate(columns):
                            if col_name:  # Skip empty column names
                                data_idx = i + col_offset
                                if data_idx < len(data_row):
                                    # Clean the value
                                    value = data_row[data_idx].strip() if data_row[data_idx] else ""
                                    # Replace newlines with spaces for cleaner data
                                    value = value.replace("\n", " ")
                                    row_dict[col_name] = value
                                else:
                                    row_dict[col_name] = ""

                        all_results.append(row_dict)
                else:
                    # No proper table structure found, add a single row with empty values
                    print(f"No table structure found for {az_path}")
                    row_dict = {
                        "service_order": service_order,
                        "component_serial": component_serial,
                        "az_path": az_path,
                        "table_title": table_title,
                        "pages": pages,
                    }
                    all_results.append(row_dict)

        except Exception as e:
            print(f"Error processing {record.get('az_path', 'unknown')}: {str(e)}")
            # Add error record
            error_dict = {
                "service_order": record.get("service_order", ""),
                "component_serial": record.get("component_serial", ""),
                "az_path": record.get("az_path", ""),
                "table_title": "ERROR",
                "pages": "",
                "error_message": str(e),
            }
            all_results.append(error_dict)

    # Create final dataframe
    if all_results:
        # Convert to dataframe - Polars will handle different column sets
        final_df = pl.DataFrame(all_results)

        # Order columns consistently
        base_cols = [
            "service_order",
            "component_serial",
            "az_path",
            "table_title",
            "pages",
        ]
        other_cols = [col for col in final_df.columns if col not in base_cols]
        ordered_cols = base_cols + sorted(other_cols)

        # Select only columns that exist
        existing_cols = [col for col in ordered_cols if col in final_df.columns]
        final_df = final_df.select(existing_cols)

        return final_df
    else:
        # Return empty dataframe with expected schema
        return pl.DataFrame(
            {
                "service_order": [],
                "component_serial": [],
                "az_path": [],
                "table_title": [],
                "pages": [],
            }
        )
