import dagster as dg
from kdags.config.catalog_loader import DATA_CATALOG


@dg.asset
def hola(context):
    catalog = {}
    for k, v in DATA_CATALOG.items():
        catalog.update(v)

    context.log.info(catalog)


import dagster as dg
from kdags.resources.tidyr import DataLake  # Assuming DataLake is in this path
import pdfplumber
from io import BytesIO


# @dg.asset
# def process_pdf_from_datalake(context: dg.AssetExecutionContext, datalake: DataLake, pdf_path: str) -> None:
#     """
#     This asset retrieves a PDF file from Azure Data Lake, opens it using pdfplumber,
#     and then you can add your logic to process the PDF.
#
#     Args:
#         context: The Dagster execution context.
#         datalake: An instance of the DataLake resource.
#         pdf_path: The Azure path to the PDF file (e.g., "az://container/path/to/your.pdf").
#     """
#     context.log.info(f"Attempting to read PDF from: {pdf_path}")
#
#     # Read the PDF file as bytes from DataLake
#     pdf_bytes = datalake.read_bytes(az_path=pdf_path)
#     context.log.info(f"Successfully read {len(pdf_bytes)} bytes from {pdf_path}")
#
#     # Open the PDF using pdfplumber with the bytes
#     # The user mentioned they will be using pdfplumber
#     with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
#         context.log.info(f"Successfully opened PDF: {pdf_path} with pdfplumber.")
#         context.log.info(f"Number of pages: {len(pdf.pages)}")
#
#         # --------------------------------------------------------------------
#         # TODO: Add your logic here to process the 'pdf' object.
#         #
#         # Examples:
#         # for page_num, page in enumerate(pdf.pages):
#         #     text = page.extract_text()
#         #     context.log.info(f"Text from page {page_num + 1}: {text[:100]}...") # Log first 100 chars
#         #
#         #     tables = page.extract_tables()
#         #     for i, table_data in enumerate(tables):
#         #         context.log.info(f"Table {i+1} on page {page_num + 1}: {table_data[:2]}") # Log first 2 rows
#         # --------------------------------------------------------------------
#
#     context.log.info(f"Finished processing PDF: {pdf_path}")
#
#
# # Example of how you might define your Datalake resource in your definitions.py
# # from dagster import Definitions, EnvVar
# # from kdags.resources.tidyr import DataLake
# #
# # defs = Definitions(
# #     assets=[process_pdf_from_datalake],
# #     resources={
# #         "datalake": DataLake.configure_for_deployment(
# #             config_schema={"connection_string": EnvVar("AZURE_STORAGE_CONNECTION_STRING")}
# #         )
# #     }
# # )
