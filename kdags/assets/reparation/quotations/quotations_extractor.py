import pdfplumber
import re
import pandas as pd
import io  # Required for BytesIO


def extract_tables_from_quotations(pdf_bytes):
    """
    Extracts tabular data from PDF bytes based on defined textual patterns,
    processing each line individually. Attempts to append simple wrapped
    lines to the ITEM or DESCRIPTION of the previous valid data row.
    """
    if not pdf_bytes:
        print("Error: pdf_bytes is None. Cannot process.")
        return []

    all_table_rows = []
    current_table_title = None
    capturing_data = False

    # --- REGULAR EXPRESSIONS ---
    title_pattern = re.compile(r"^\d+\.- .*")
    header_pattern_text = "SEC ITEM DESCRIPCIÓN ITEM Q UND P. UNIT. SUB-TOTAL MON"
    sub_total_pattern = re.compile(r"^Sub Total \d+\.- .*")
    # Tail pattern: Q UND, P. UNIT., SUB-TOTAL MON
    tail_pattern = re.compile(r"(\d+\s+[A-Za-z\/]+)\s+([\d.,]+)\s+([\d.,]+\s+(?:USD|CLP))$")

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page_num, page in enumerate(pdf.pages):
            lines = page.extract_text_lines(strip=True, return_chars=False)

            if not lines:
                continue

            for line_info in lines:
                line_text = line_info["text"].strip()

                # 1. Check for new table title
                if title_pattern.match(line_text) and not sub_total_pattern.match(line_text):
                    current_table_title = line_text
                    capturing_data = False
                    # print(f"DEBUG: New Title: {current_table_title}")
                    continue

                # 2. Check for header row
                if current_table_title and not capturing_data:
                    normalized_line_text = " ".join(line_text.replace("DESCRIPCION", "DESCRIPCIÓN").split())
                    if normalized_line_text == header_pattern_text:
                        capturing_data = True
                        # print(f"DEBUG: Header found for {current_table_title}")
                        continue

                # 3. Process data lines if capturing_data
                if capturing_data:
                    if sub_total_pattern.match(line_text):
                        current_table_title = None
                        capturing_data = False
                        # print(f"DEBUG: Sub Total found.")
                        continue

                    if not line_text:
                        continue  # Skip empty lines

                    # Initialize default row dictionary for the current line
                    row_data_dict = {
                        "table_title": current_table_title,
                        "SEC": "PARSE_ERROR",
                        "ITEM": "PARSE_ERROR",
                        "DESCRIPCIÓN ITEM": "PARSE_ERROR",
                        "Q UND": "PARSE_ERROR",
                        "P. UNIT.": "PARSE_ERROR",
                        "SUB-TOTAL MON": "PARSE_ERROR",
                        "original_line": line_text,
                    }

                    parse_line = line_text  # Work with a copy for parsing attempts

                    # Attempt to parse SEC from the beginning of the line
                    sec_match = re.match(r"^(\S+)\s+", parse_line)

                    if sec_match:
                        # This line looks like it starts with a SEC code
                        row_data_dict["SEC"] = sec_match.group(1).strip()
                        parse_line = parse_line[len(sec_match.group(0)) :].strip()  # Remainder after SEC

                        # Attempt to parse the tail (Q UND, P.UNIT, SUB-TOTAL MON)
                        tail_match_res = tail_pattern.search(parse_line)
                        middle_text_block = ""

                        if tail_match_res:
                            row_data_dict["Q UND"] = tail_match_res.group(1).strip()
                            row_data_dict["P. UNIT."] = tail_match_res.group(2).strip()
                            row_data_dict["SUB-TOTAL MON"] = tail_match_res.group(3).strip()
                            middle_text_block = parse_line[: tail_match_res.start()].strip()
                        else:
                            # SEC was found, but tail was not. The rest is middle_text_block.
                            middle_text_block = parse_line

                        # Parse ITEM and DESCRIPCIÓN ITEM from middle_text_block
                        item_val = "PARSE_ERROR"
                        desc_val = "PARSE_ERROR"

                        if middle_text_block:
                            parts = middle_text_block.split(maxsplit=1)
                            if len(parts) > 0:
                                item_val = parts[0].strip()
                            if len(parts) > 1:
                                desc_val = parts[1].strip()
                            elif item_val != "PARSE_ERROR" and item_val != "":
                                desc_val = ""
                            elif not item_val or item_val == "PARSE_ERROR":
                                item_val = ""
                                desc_val = ""
                        else:
                            item_val = ""  # If middle_text_block was empty
                            desc_val = ""

                        row_data_dict["ITEM"] = item_val
                        row_data_dict["DESCRIPCIÓN ITEM"] = desc_val

                        all_table_rows.append(row_data_dict)

                    else:
                        # SEC was NOT found at the beginning of this line.
                        # Check if this line could be a continuation of the previous row.
                        tail_match_for_continuation = tail_pattern.search(line_text)

                        if tail_match_for_continuation is None and all_table_rows:
                            last_row = all_table_rows[-1]
                            # Ensure last_row is part of the current table and was a valid data row (not an error row itself)
                            if (
                                last_row["table_title"] == current_table_title
                                and last_row["Q UND"] != "PARSE_ERROR"
                                and last_row["SEC"] != "PARSE_ERROR"
                            ):  # Added check for valid SEC on last_row

                                stripped_line_text = line_text.strip()

                                # Check if the PREVIOUS item ended with a hyphen
                                if last_row["ITEM"] != "PARSE_ERROR" and last_row["ITEM"].endswith("-"):
                                    # This line is a continuation of the ITEM code
                                    # print(f"DEBUG: Appending '{stripped_line_text}' to previous row's ITEM: '{last_row['ITEM']}'")
                                    last_row["ITEM"] += stripped_line_text  # Append directly to the item code
                                    last_row["original_line"] += (
                                        " " + stripped_line_text
                                    )  # Append to original line for traceability
                                    continue  # Skip adding this as a new row
                                else:
                                    # This line is a continuation of the DESCRIPTION
                                    # print(f"DEBUG: Appending '{stripped_line_text}' to previous row's DESCRIPTION: '{last_row['DESCRIPCIÓN ITEM']}'")
                                    continuation_text = " " + stripped_line_text
                                    if (
                                        last_row["DESCRIPCIÓN ITEM"] == "PARSE_ERROR"
                                        or not last_row["DESCRIPCIÓN ITEM"]
                                    ):
                                        last_row["DESCRIPCIÓN ITEM"] = stripped_line_text
                                    else:
                                        last_row["DESCRIPCIÓN ITEM"] += continuation_text
                                    last_row[
                                        "original_line"
                                    ] += continuation_text  # Append to original_line of the previous row
                                    continue

                        # If it's not a clear continuation or no valid previous row, add as a PARSE_ERROR row.
                        # This row_data_dict will still have SEC: "PARSE_ERROR"
                        all_table_rows.append(row_data_dict)
                        # print(f"DEBUG: Adding row with SEC PARSE_ERROR: {line_text}")

    return all_table_rows
