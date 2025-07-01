import pdfplumber
import re
from datetime import datetime
import pandas as pd
import warnings
import polars as pl
from datetime import date

warnings.filterwarnings("ignore", message="CropBox missing from /Page, defaulting to MediaBox")


def get_shift_dates():
    # --- Configuration ---
    start_date = date(2020, 1, 1)
    end_date = date(2030, 12, 31)

    # Reference point: Wednesday, April 9, 2025 starts an 'M' shift week.
    ref_date_wednesday = date(2025, 4, 9)
    ref_shift_label = "M"
    other_shift_label = "N"

    # Get the absolute ordinal day number for the reference Wednesday
    ref_ordinal_wednesday = ref_date_wednesday.toordinal()
    # --- End Configuration ---

    # 1. Generate all dates
    df = pl.DataFrame({"date": pl.date_range(start_date, end_date, interval="1d", eager=True)})

    # 2. Calculate ISO Year and Week
    df = df.with_columns(iso_year=pl.col("date").dt.iso_year(), iso_week=pl.col("date").dt.week())

    # 3. Calculate shift label - Using Mon=1 convention for weekday()

    # 3a. Calculate offset days to subtract to get to previous Wednesday (assuming Wed=3)
    df = df.with_columns(days_to_subtract=(pl.col("date").dt.weekday() - 3 + 7) % 7)

    # 3b. Calculate the shift week start date
    df = df.with_columns(shift_week_start=pl.col("date") - pl.duration(days=pl.col("days_to_subtract")))

    # 3c. Calculate the absolute ordinal for the shift week start date
    df = df.with_columns(
        shift_start_ordinal=pl.col("shift_week_start").map_elements(lambda d: d.toordinal(), return_dtype=pl.Int64)
    )

    # 3d. Calculate week difference using absolute ordinals
    df = df.with_columns(week_difference_ord=(pl.col("shift_start_ordinal") - ref_ordinal_wednesday) // 7)

    # 3e. Assign shift based on ordinal week difference parity
    df = df.with_columns(
        icc_week=(pl.col("iso_year").cast(pl.String) + "-W" + pl.col("iso_week").cast(pl.String).str.zfill(2)),
        work_shift=pl.when(pl.col("week_difference_ord") % 2 == 0)
        .then(pl.lit(ref_shift_label))  # Even diff -> 'M'
        .otherwise(pl.lit(other_shift_label)),  # Odd diff -> 'N'
    )

    # 4. Select final columns
    final_df = df.select(["date", "icc_week", "work_shift"])
    return final_df


def parse_horometer_value(value_str):
    """
    Parse horometer values with mixed decimal/thousands separators.
    Examples: "65.416,8 HRS", "1,234.56 hrs", "100,000 h"

    Returns float value or -1.0 if parsing fails.
    """
    try:
        # Remove unit text and whitespace
        clean_str = value_str.split(" ")[0].lower().replace("hrs", "").replace("h", "").strip()

        # If there's no comma or period, just parse directly
        if "," not in clean_str and "." not in clean_str:
            return float(clean_str)

        # Check if comma is used as decimal separator (e.g., "65.416,8")
        if "," in clean_str:
            parts = clean_str.split(",")
            if len(parts) == 2 and len(parts[1]) <= 2:
                # Comma is decimal separator - remove thousands separators (periods)
                whole_part = parts[0].replace(".", "")
                return float(whole_part + "." + parts[1])

        # Otherwise assume period is decimal separator and remove thousands separators (commas)
        return float(clean_str.replace(",", ""))

    except (ValueError, IndexError, AttributeError):
        return -1.0


def extract_technical_report_data(pdf_path):
    """
    Extract key information from a Komatsu technical report PDF.

    Args:
        pdf_path: Path to the PDF file

    Returns:
        pandas.DataFrame: DataFrame containing processed and formatted report data
    """
    results = {
        "datos_equipo": {},
        "datos_generales": {},
        "descripcion_caso": "",
        "causas_falla": "",
    }

    with pdfplumber.open(pdf_path) as pdf:
        # Process each page to extract text
        all_pages_text = ""
        for page in pdf.pages:
            page_text = page.extract_text()
            all_pages_text += page_text + "\n"
            lines = page_text.split("\n")

            # Extract equipment data
            for line in lines:
                if "Camión:" in line:
                    results["datos_equipo"]["camion"] = line.split("Camión:")[1].strip()

                elif "Modelo:" in line:
                    if "Modelo de motor Diesel:" in line:
                        parts = line.split("Modelo de motor Diesel:")
                        model_part = parts[0].split("Modelo:")[1].strip()
                        results["datos_equipo"]["modelo"] = model_part
                    else:
                        parts = line.split("Modelo:")
                        if len(parts) > 1:
                            results["datos_equipo"]["modelo"] = parts[1].strip()

                elif "Serie:" in line:
                    results["datos_equipo"]["serie"] = line.split("Serie:")[1].strip()

                elif "Horómetro de equipo:" in line:
                    results["datos_equipo"]["horometro"] = line.split("Horómetro de equipo:")[1].strip()

                # Extract general data
                elif "Fecha del reporte:" in line:
                    results["datos_generales"]["report_date"] = line.split("Fecha del reporte:")[1].strip()

                elif "Fecha de la falla:" in line:
                    results["datos_generales"]["failure_date"] = line.split("Fecha de la falla:")[1].strip()

        # Extract case description - find it anywhere in the document
        if "4. Descripción del caso" in all_pages_text:
            start_idx = all_pages_text.find("4. Descripción del caso") + len("4. Descripción del caso")
            end_idx = all_pages_text.find("5. Causas de falla")
            if start_idx > 0 and end_idx > start_idx:
                description = all_pages_text[start_idx:end_idx].strip()
                results["descripcion_caso"] = description.replace("•", "").strip()

        # Extract failure causes - find it anywhere in the document
        if "5. Causas de falla" in all_pages_text:
            start_idx = all_pages_text.find("5. Causas de falla") + len("5. Causas de falla")
            end_idx = all_pages_text.find("6. Detallar como se solucionó la falla")
            if start_idx > 0 and end_idx > start_idx:
                causes = all_pages_text[start_idx:end_idx].strip()
                results["causas_falla"] = causes.replace("•", "").strip()

    # Process and format the data for DataFrame
    data = {
        # Format equipment name: extract 3 digits and append TK prefix
        "equipment_name": (
            "TK" + re.search(r"(\d{3})", results["datos_equipo"].get("camion", "")).group(1)
            if re.search(r"(\d{3})", results["datos_equipo"].get("camion", ""))
            else None
        ),
        # Process equipment model
        "equipment_model": results["datos_equipo"].get("modelo", "").strip(),
        # Process equipment serial
        "equipment_serial": results["datos_equipo"].get("serie", "").strip(),
        # Process horometro: remove dots and "HRS" and convert to integer
        # "horometro": results["datos_equipo"].get("horometro", "").strip(),
        "equipment_hours": parse_horometer_value(results.get("datos_equipo", {}).get("horometro", "-1")),
        # Format dates in dd-mm-yyyy format
        "report_date": results.get("datos_generales", {}).get("report_date", ""),
        "failure_date": results.get("datos_generales", {}).get("failure_date", ""),
        "descripcion_caso": results.get("descripcion_caso", {}).strip(),
        "causas_falla": results.get("causas_falla", {}).strip(),
    }

    return data


def format_date(date_str):
    """
    Parse date string in multiple formats (dd-mm-yyyy or dd-mm-yy).
    Returns a datetime object if successful.
    Raises ValueError with descriptive message if parsing fails.
    """
    date_str = date_str.strip()
    if not date_str:
        raise ValueError("Empty date string provided")

    # Try different date formats
    formats = ["%Y-%m-%d", "%d-%m-%Y", "%d-%m-%y"]  # 08-10-2024  # 08-10-24

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    # If we get here, none of the formats matched
    raise ValueError(f"Could not parse date '{date_str}' - expected format dd-mm-yyyy or dd-mm-yy")


def parse_filename(file_path):
    """
    Parse filename to extract equipment number, ICC number, component code, and date.

    Args:
        file_path: Path object of the file

    Returns:
        Dict with extracted information
    """
    file_name = file_path.stem  # Get filename without extension

    # Initialize extraction results
    result = {
        "file_path": str(file_path),
        "icc_file_name": file_name,
        "equipment_name": None,
        "icc_number": None,
        "component_code": None,
        "position_code": 0,
        "position_name": "UNICA",  # Default position
        "changeout_date": None,
    }

    # Try to extract date (testing both YYYY-MM-DD and DD-MM-YYYY formats)
    date_match_yyyy_first = re.search(r"(\d{4}-\d{2}-\d{2})", file_name)
    date_match_dd_first = re.search(r"(\d{2}-\d{2}-\d{4})", file_name)

    if date_match_yyyy_first:
        result["changeout_date"] = datetime.strptime(date_match_yyyy_first.group(1), "%Y-%m-%d")
        # Remove date from filename for other pattern matching
        filename_without_date = file_name.replace(date_match_yyyy_first.group(1), "").strip()
    elif date_match_dd_first:
        result["changeout_date"] = datetime.strptime(date_match_dd_first.group(1), "%d-%m-%Y")
        # Remove date from filename for other pattern matching
        filename_without_date = file_name.replace(date_match_dd_first.group(1), "").strip()
    else:
        filename_without_date = file_name

    # Try to extract TK number (equipment number)
    tk_match = re.search(r"TK(\d{3})", filename_without_date, re.IGNORECASE)
    if tk_match:
        result["equipment_name"] = f"TK{tk_match.group(1)}"

    # Try to extract ICC number
    icc_match = re.search(r"ICC(\d*)", filename_without_date, re.IGNORECASE)
    if icc_match:
        result["icc_number"] = icc_match.group(1) if icc_match.group(1) else None

    # Extract remaining parts which might be the code
    # First, remove TK and ICC parts from the filename
    remaining = filename_without_date
    if tk_match:
        remaining = re.sub(r"TK\d{3}", "", remaining, flags=re.IGNORECASE)
    if icc_match:
        remaining = re.sub(r"ICC\d*", "", remaining, flags=re.IGNORECASE)

    # Clean and extract code
    remaining = remaining.strip()
    if remaining:
        # Check if the code has digits
        digits_match = re.search(r"([A-Za-z]+)(\d+)", remaining)
        if digits_match:
            result["component_code"] = digits_match.group(1)
            result["position_code"] = int(digits_match.group(2))
        else:
            result["component_code"] = remaining

    return result
