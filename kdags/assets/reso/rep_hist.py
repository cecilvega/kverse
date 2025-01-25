import pandas as pd
import re
import pdfplumber


def process_file_paths(file_paths):
    data = []

    for path in file_paths:
        # Convert to Path object for easier manipulation

        # Extract component serial (between COMP\ and next \)
        match = re.search(r"COMP/([^/]+)", path.__str__())
        component_serial = match.group(1) if match else None

        # Get filename
        filename = path.name

        # Determine file type
        file_type = None
        if "INI_APRO" in filename:
            # Extract service order (everything before first _)
            service_order = filename.split("_")[0]
            file_type = "REPORTE_TECNICO_INI"
        elif "FIN_APRO" in filename:
            service_order = filename.split("_")[0]
            file_type = "REPORTE_TECNICO_FIN"
        elif "PPTO" in filename:
            service_order = filename.split("_")[0]
            file_type = "PPTO"
        else:
            service_order = None

        data.append(
            {
                "component_serial": component_serial,
                "file_path": path.absolute().__str__(),
                "os_reso": service_order,
                "file_type": file_type,
            }
        )

    # Create DataFrame
    df = pd.DataFrame(data)
    return df


def analyze_multiple_filetypes(df):
    # Group by service_order and count file types
    file_type_counts = df.groupby(["os_reso", "file_type"])["file_type"].count()

    # Get service orders with more than one file type
    multiple_filetypes = file_type_counts[file_type_counts > 1].index

    if len(multiple_filetypes) > 0:
        # Filter original dataframe to show only these service orders
        multiple_files_df = df[df["os_reso"].isin(multiple_filetypes)]

        # Sort by service_order for better readability
        multiple_files_df = multiple_files_df.sort_values(["os_reso", "file_type"])

        print("Service orders with multiple file types:")
        print("\nDetailed view:")
        print(multiple_files_df)

        print("\nSummary by service order and file type:")
        summary = pd.crosstab(multiple_files_df["os_reso"], multiple_files_df["file_type"])
        print(summary)
    else:
        print("No service orders found with multiple file types.")


def extract_pdf_data(pdf_path):

    sections = {
        "DATOS GENERALES": [
            "ORDEN DE SERVICIO",
        ],
        "DATOS MÁQUINA": ["SERIE", "MODELO"],
        "DATOS COMPONENTE": [
            "HORÓMETRO",
            "N° PARTE",
        ],
    }

    result = {"file_path": pdf_path}

    try:
        with pdfplumber.open(pdf_path) as pdf:
            text_content = ""
            for page in pdf.pages:
                text_content += page.extract_text()

            for section_name, fields in sections.items():
                if section_name in text_content:
                    text_chunk = (
                        text_content.split(section_name)[1].split("DATOS")[0]
                        if "DATOS" in text_content.split(section_name)[1]
                        else text_content.split(section_name)[1]
                    )

                    for field in fields:
                        try:
                            if field + " : " in text_chunk:
                                field_text = text_chunk.split(field + " : ")[1].split("\n")[0].strip()
                            elif field + ": " in text_chunk:
                                field_text = text_chunk.split(field + ": ")[1].split("\n")[0].strip()
                            else:
                                field_text = None

                            if field_text:
                                for f in fields:
                                    if f + " : " in field_text:
                                        field_text = field_text.split(f + " : ")[0].strip()
                                    elif f + ": " in field_text:
                                        field_text = field_text.split(f + ": ")[0].strip()

                            result[field] = field_text

                        except IndexError:
                            result[field] = None

        return result

    except Exception as e:
        print(f"Error extracting data from {pdf_path}: {str(e)}")
        return {}


def clean_service_and_horometro(df):

    df["ORDEN DE SERVICIO"] = df["ORDEN DE SERVICIO"].str.extract(r"((?:14|13)\d{6})").astype(int)

    df["HORÓMETRO"] = (
        df["HORÓMETRO"]
        .str.extract(r"(\d+[.,]?\d*)")[0]  # Extract first number, with optional decimal
        .str.replace(",", ".")  # Standardize decimal separator
        .str.replace(".", "")
        .map(lambda x: pd.to_numeric(x))
        .fillna(-1)  # Remove decimal point
        .astype(int)
    )  # Convert to integer

    return df


def add_report_ini_info(df):
    df = df.loc[df["file_type"] == "REPORTE_TECNICO_INI"][["os_reso", "file_path"]]

    extracted_data = pd.DataFrame(df["file_path"].apply(extract_pdf_data).tolist())
    extracted_data = clean_service_and_horometro(extracted_data)
    df = pd.merge(df, extracted_data, how="left", on="file_path")
    assert all(df["os_reso"] == df["ORDEN DE SERVICIO"])
    df = df.drop(columns=["ORDEN DE SERVICIO", "file_path"])
    return df
