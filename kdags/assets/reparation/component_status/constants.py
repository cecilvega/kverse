# --- Constants for Component Status ---

# ignored_columns = [
#     "Order Type",
#     "Customer",
#     "Workshop",
#     "Repair Component Request",
#     "Part No.",
#     "Arrival Classification",
#     "Workshop Days",
#     "Warranty",
#     "Date Creation Parts Order",
#     "Date Request Parts Order",
#     "Date Approval Parts Order",
#     "Date Load Parts Order",
#     "Estimated Arrival Date Parts",
#     "Purchase Order",
#     "Estimated Repair Date",
#     "Actual Repair Date",
#     "Reception Component Warehouse",
#     "Dispatch Date",
#     "Reception Document",
#     "Validity",
#     "Quotation Status",
#     "Estimated Quotation Date",
#     "Service Order Status",
#     "Component Status",
#     "Group",
#     "Fault Date",
#     "Warranty Type",
#     "Quotation Rejection",
#     "Repair Type",
# ]

# Raw Column Names (from source Excel/CSV)
RAW_COL_SERVICE_ORDER = "Service Order"
RAW_COL_PREVIOUS_SERVICE_ORDER = "Previous Service Order"
RAW_COL_CUSTOMER_WORK_ORDER = "Customer Work Order"
RAW_COL_EQUIPMENT_INTERNAL_NO = "Equipment Internal No."
RAW_COL_LOCATION = "Location"
RAW_COL_MAIN_COMPONENT = "Main Component"
RAW_COL_EQUIPMENT_MODEL = "Equipment Model"
RAW_COL_COMPONENT = "Component"
RAW_COL_MACHINE_SERIAL_NO = "Machine Serial No."
RAW_COL_EQUIPMENT_HOURS = "Equipment Hour(s)"
RAW_COL_COMPONENT_SERIES_NO = "Component Series No."
RAW_COL_RECEPTION_DATE = "Reception Date"
RAW_COL_OPENING_DATE = "Opening Date"
RAW_COL_LOAD_PRELIM_REPORT_REVIEW = "Load Preliminary Report [In Review]"
RAW_COL_LOAD_PRELIM_REPORT_REVISED = "Load Preliminary Report [Revised]"
RAW_COL_FIRST_QUOTATION_PUBLICATION = "First Quotation Publication"
RAW_COL_LATEST_QUOTATION_PUBLICATION = "Latest Quotation Publication"
RAW_COL_DATE_SEND_QUOTATION = "Date Send Quotation"
RAW_COL_APPROVAL_DATE = "Approval Date"
RAW_COL_COMPONENTS_HOURS = "Components Hours"
RAW_COL_LOAD_FINAL_REPORT_REVIEW = "Load Final Report [In Review]"
RAW_COL_LOAD_FINAL_REPORT_REVISED = "Load Final Report [Revised]"
RAW_COL_SAP_CLOSING_DATE = "SAP Closing Date"
RAW_COL_REASON_FOR_REPAIR = "Reason for Repair"
RAW_COL_OBSERVATION_OS = "Observation OS"
RAW_COL_RESO_CLOSING_DATE = "RESO Closing Date"

# Standardized Column Names (used in the final DataFrame)
COL_SERVICE_ORDER = "service_order"
COL_PREVIOUS_SERVICE_ORDER = "previous_service_order"
COL_CUSTOMER_WORK_ORDER = "customer_work_order"
COL_SAP_EQUIPMENT_NAME = "sap_equipment_name"
COL_SITE_NAME = "site_name"
COL_MAIN_COMPONENT = "main_component"
COL_EQUIPMENT_MODEL = "equipment_model"
COL_COMPONENT = "component"
COL_EQUIPMENT_SERIAL = "equipment_serial"
COL_EQUIPMENT_HOURS = "equipment_hours"
COL_COMPONENT_SERIAL = "component_serial"
COL_RECEPTION_DATE = "reception_date"
COL_OPENING_DATE = "opening_date"
COL_LOAD_PRELIM_REPORT_REVIEW = "load_preliminary_report_in_review"
COL_LOAD_PRELIM_REPORT_REVISED = "load_preliminary_report_revised"
COL_FIRST_QUOTATION_PUBLICATION = "first_quotation_publication"
COL_LATEST_QUOTATION_PUBLICATION = "latest_quotation_publication"
COL_DATE_SEND_QUOTATION = "date_send_quotation"
COL_APPROVAL_DATE = "approval_date"
COL_COMPONENTS_HOURS = "components_hours"
COL_LOAD_FINAL_REPORT_REVIEW = "load_final_report_in_review"
COL_LOAD_FINAL_REPORT_REVISED = "load_final_report_revised"
COL_SAP_CLOSING_DATE = "sap_closing_date"
COL_REASON_FOR_REPAIR = "reason_for_repair"
COL_OBSERVATION_OS = "observation_os"
COL_RESO_CLOSING_DATE = "reso_closing_date"

# List of raw columns selected from the source
SELECTED_RAW_COLUMNS = [
    RAW_COL_SERVICE_ORDER,
    RAW_COL_PREVIOUS_SERVICE_ORDER,
    RAW_COL_CUSTOMER_WORK_ORDER,
    RAW_COL_EQUIPMENT_INTERNAL_NO,
    RAW_COL_LOCATION,
    RAW_COL_MAIN_COMPONENT,
    RAW_COL_EQUIPMENT_MODEL,
    RAW_COL_COMPONENT,
    RAW_COL_MACHINE_SERIAL_NO,
    RAW_COL_EQUIPMENT_HOURS,
    RAW_COL_COMPONENT_SERIES_NO,
    RAW_COL_RECEPTION_DATE,
    RAW_COL_OPENING_DATE,
    RAW_COL_LOAD_PRELIM_REPORT_REVIEW,
    RAW_COL_LOAD_PRELIM_REPORT_REVISED,
    RAW_COL_FIRST_QUOTATION_PUBLICATION,
    RAW_COL_LATEST_QUOTATION_PUBLICATION,
    RAW_COL_DATE_SEND_QUOTATION,
    RAW_COL_APPROVAL_DATE,
    RAW_COL_COMPONENTS_HOURS,
    RAW_COL_LOAD_FINAL_REPORT_REVIEW,
    RAW_COL_LOAD_FINAL_REPORT_REVISED,
    RAW_COL_SAP_CLOSING_DATE,
    RAW_COL_REASON_FOR_REPAIR,
    RAW_COL_OBSERVATION_OS,
    RAW_COL_RESO_CLOSING_DATE,
]

# Mapping from raw column names to standardized names
COLUMN_MAPPING = {
    RAW_COL_SERVICE_ORDER: COL_SERVICE_ORDER,
    RAW_COL_PREVIOUS_SERVICE_ORDER: COL_PREVIOUS_SERVICE_ORDER,
    RAW_COL_CUSTOMER_WORK_ORDER: COL_CUSTOMER_WORK_ORDER,
    RAW_COL_EQUIPMENT_INTERNAL_NO: COL_SAP_EQUIPMENT_NAME,
    RAW_COL_LOCATION: COL_SITE_NAME,
    RAW_COL_MAIN_COMPONENT: COL_MAIN_COMPONENT,
    RAW_COL_EQUIPMENT_MODEL: COL_EQUIPMENT_MODEL,
    RAW_COL_COMPONENT: COL_COMPONENT,
    RAW_COL_MACHINE_SERIAL_NO: COL_EQUIPMENT_SERIAL,
    RAW_COL_EQUIPMENT_HOURS: COL_EQUIPMENT_HOURS,
    RAW_COL_COMPONENT_SERIES_NO: COL_COMPONENT_SERIAL,
    RAW_COL_RECEPTION_DATE: COL_RECEPTION_DATE,
    RAW_COL_OPENING_DATE: COL_OPENING_DATE,
    RAW_COL_LOAD_PRELIM_REPORT_REVIEW: COL_LOAD_PRELIM_REPORT_REVIEW,
    RAW_COL_LOAD_PRELIM_REPORT_REVISED: COL_LOAD_PRELIM_REPORT_REVISED,
    RAW_COL_FIRST_QUOTATION_PUBLICATION: COL_FIRST_QUOTATION_PUBLICATION,
    RAW_COL_LATEST_QUOTATION_PUBLICATION: COL_LATEST_QUOTATION_PUBLICATION,
    RAW_COL_DATE_SEND_QUOTATION: COL_DATE_SEND_QUOTATION,
    RAW_COL_APPROVAL_DATE: COL_APPROVAL_DATE,
    RAW_COL_COMPONENTS_HOURS: COL_COMPONENTS_HOURS,
    RAW_COL_LOAD_FINAL_REPORT_REVIEW: COL_LOAD_FINAL_REPORT_REVIEW,
    RAW_COL_LOAD_FINAL_REPORT_REVISED: COL_LOAD_FINAL_REPORT_REVISED,
    RAW_COL_SAP_CLOSING_DATE: COL_SAP_CLOSING_DATE,
    RAW_COL_REASON_FOR_REPAIR: COL_REASON_FOR_REPAIR,
    RAW_COL_OBSERVATION_OS: COL_OBSERVATION_OS,
    RAW_COL_RESO_CLOSING_DATE: COL_RESO_CLOSING_DATE,
}

# Columns to convert to integer after cleaning
INT_CONVERSION_COLUMNS = [
    # COL_SERVICE_ORDER,
    COL_PREVIOUS_SERVICE_ORDER,
    COL_CUSTOMER_WORK_ORDER,
]

# Columns to convert to date
DATE_CONVERSION_COLUMNS = [
    COL_RECEPTION_DATE,
    COL_OPENING_DATE,
    # Add other date columns here if they exist in the source and need conversion
    COL_LOAD_PRELIM_REPORT_REVIEW,
    COL_LOAD_PRELIM_REPORT_REVISED,
    COL_FIRST_QUOTATION_PUBLICATION,
    COL_LATEST_QUOTATION_PUBLICATION,
    COL_DATE_SEND_QUOTATION,
    COL_APPROVAL_DATE,
    COL_LOAD_FINAL_REPORT_REVIEW,
    COL_LOAD_FINAL_REPORT_REVISED,
    COL_SAP_CLOSING_DATE,
    COL_RESO_CLOSING_DATE,
]

# Columns to convert to numeric/float (example, adjust as needed)
FLOAT_CONVERSION_COLUMNS = [
    COL_EQUIPMENT_HOURS,
    COL_COMPONENTS_HOURS,
]
