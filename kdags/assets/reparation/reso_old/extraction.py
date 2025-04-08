import pdfplumber
import pandas as pd
from pathlib import Path
import re

import json
import pickle


def read_all_tables(pdf_path):
    """Extract all tables from PDF and return a list of their basic info"""
    table_info = []

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, 1):
            tables = page.extract_tables()

            for table_num, table in enumerate(tables, 1):
                # Clean table data
                cleaned_table = []
                for row in table:
                    cleaned_row = [str(cell).strip() if cell is not None else "" for cell in row]
                    if any(cleaned_row):  # Only include non-empty rows
                        cleaned_table.append(cleaned_row)

                if cleaned_table:
                    # Get basic info about the table
                    info = {
                        "page": page_num,
                        "table_number": table_num,
                        "data": cleaned_table,
                    }
                    table_info.append(info)

    # table_info = merge_split_tables(table_info)
    return table_info


def merge_split_tables(tables):
    merged = []
    current_table = None

    for table in tables:
        data = table["data"]

        # Check if first row is a title (starts with number and dot)
        has_title = len(data) >= 1 and any(data[0]) and any(c.isdigit() for c in data[0][0])

        # Check if second row is a header (all columns have content except first)
        has_header = (
            len(data) >= 2
            and not data[1][0]  # First cell empty
            and all(cell for cell in data[1][1:])  # Rest have content
        )

        # It's a complete table if it has both title and header
        if has_title and has_header:
            if current_table:
                merged.append(current_table)
            current_table = table
        # It's a continuation if it matches column count and doesn't have title/header
        elif current_table and len(data[0]) == len(current_table["data"][0]) and not has_title and not has_header:
            current_table["data"].extend(data)

    if current_table:
        merged.append(current_table)

    return merged


def format_tables_for_df(merged_tables):
    formatted_tables = []

    for table in merged_tables:
        data = table["data"]
        # Extract table name from first row
        table_name = data[0][0].strip()
        # Get headers from second row (skip first empty cell)
        headers = data[1][1:]
        # Get actual data (skip title and header rows)
        rows_data = data[2:]

        # Create dict with data ready for DataFrame
        formatted_data = []
        for row in rows_data:
            # First column is usually the row identifier/name
            row_dict = {"item": row[0]}
            # Add rest of columns using headers as keys
            for header, value in zip(headers, row[1:]):
                row_dict[header] = value
            formatted_data.append(row_dict)

        formatted_tables.append(
            {
                "table_name": table_name,
                "page": table["page"],
                "table_number": table["table_number"],
                "tabular_data": formatted_data,
            }
        )

    return formatted_tables


def extract_all_tables(pdf_path):
    tables = read_all_tables(pdf_path)
    merged_tables = merge_split_tables(tables)  # [40:43]
    merged_tables = format_tables_for_df(merged_tables)
    return merged_tables
