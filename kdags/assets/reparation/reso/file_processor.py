import pandas as pd
import unicodedata
import re


def remove_accents(input_str):
    """Remove accents from the input string."""
    return "".join(c for c in unicodedata.normalize("NFD", input_str) if unicodedata.category(c) != "Mn")


def clean_subtitle(sub):
    """Clean the subtitle: remove newlines, extra whitespace, and unwanted dots at the edges."""
    sub = sub.replace("\n", " ")
    sub = re.sub(r"\s+", " ", sub).strip()
    sub = sub.strip(".")  # remove any leading/trailing dots
    sub = sub.replace("/", "-")
    return sub


def process_docs_data(docs_data, subcomponent_name, component_serial):
    df = pd.DataFrame(docs_data)

    # Define folder conditions as (lambda, folder)
    conditions = [
        (lambda s: "presupuesto" in s, "PRESUPUESTOS"),
        (lambda s: "informe tecnico final" in s, "INFORMES"),
        (
            lambda s: "informe tecnico preliminar" in s
            and ("[aprobado]" in s or "[en revision]" in s or "[revisado]" in s),
            "INFORMES",
        ),
        (
            lambda s: "informe tecnico" in s and ("[anexos]" in s or "[lec]" in s),
            "ENSAYOS",
        ),
    ]

    def get_folder(norm_sub):
        for condition, folder in conditions:
            if condition(norm_sub):
                return folder
        return "OTROS"

    # Normalize the subtitle: remove accents and convert to lowercase
    df["normalized"] = df["subtitle"].apply(lambda x: remove_accents(x)).str.lower()
    df["file_type"] = df["normalized"].apply(get_folder)

    # Extract blob filename from the URL, then split into base and extension.
    df["blob_filename"] = df["blob_url"].apply(lambda url: url.split("/")[-1].split("?")[0])
    df[["blob_base", "extension"]] = df["blob_filename"].str.rsplit(".", n=1, expand=True)
    df["extension"] = "." + df["extension"]  # re-add the dot

    # Clean the subtitle (remove newlines and extra spaces)
    df["cleaned_subtitle"] = df["subtitle"].apply(clean_subtitle)

    # Construct the final file name:
    # Format: {blob_base}_{service_work_order}_{cleaned_subtitle}{extension}
    df["file_name"] = df["blob_base"] + "_" + df["cleaned_subtitle"] + df["extension"]

    # Build the target folder path using your prefix, subcomponent name, component serial, and file_type.
    prefix = "01.%20%C3%81REAS%20KCH/1.6%20CONFIABILIDAD/CAEX/4.%20Antecedentes/REPARACION"
    df["target_folder"] = f"{prefix}/{subcomponent_name.upper()}/{component_serial}/" + df["file_type"]

    return df
