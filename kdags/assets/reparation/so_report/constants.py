# -----------------------------------------------------------------------------
# Raw Column Names (from source Excel/CSV) - Provided as the basis
# -----------------------------------------------------------------------------
RAW_COL_SERVICE_ORDER = "Service Order"
RAW_COL_PREVIOUS_SERVICE_ORDER = "Previous Service Order"
RAW_COL_CUSTOMER_WORK_ORDER = "Customer Work Order"
RAW_COL_EQUIPMENT_INTERNAL_NO = "Equipment Internal No."
RAW_COL_LOCATION = "Location"
RAW_COL_WORKSHOP = "Workshop"
RAW_COL_MAIN_COMPONENT = "Main Component"
RAW_COL_EQUIPMENT_MODEL = "Equipment Model"
RAW_COL_COMPONENT = "Component"
RAW_COL_COMPONENT_SERIES_NO = "Serial No."
RAW_COL_COMPONENTS_HOURS = "Equipment Hour(s)"
RAW_COL_RECEPTION_DATE = "Reception Date"
RAW_COL_OPENING_DATE = "Opening Date"
RAW_COL_WARRANTY_TYPE = "Warranty Type"
RAW_COL_LOAD_PRELIM_REPORT_REVIEW = "Load Preliminary Report [In Review]"
RAW_COL_LOAD_PRELIM_REPORT_REVISED = "Load Preliminary Report [Revised]"
RAW_COL_FIRST_QUOTATION_PUBLICATION = "First Quotation Publication"
RAW_COL_LATEST_QUOTATION_PUBLICATION = "Latest Quotation Publication"
RAW_COL_DATE_SEND_QUOTATION = "Date Send Quotation"
RAW_COL_APPROVAL_DATE = "Approval Date"
RAW_COL_QUOTATION_REJECTION = "Quotation Rejection"
RAW_PURCHASE_ORDER = "Purchase Order"
RAW_COL_RESO_CLOSING_DATE = "RESO Closing Date"
RAW_COL_SERVICE_ORDER_STATUS = "Service Order Status"
RAW_COL_COMPONENT_STATUS = "Component Status"
RAW_COL_LOAD_FINAL_REPORT_REVIEW = "Load Final Report [In Review]"
RAW_COL_LOAD_FINAL_REPORT_REVISED = "Load Final Report [Revised]"
RAW_COL_SAP_CLOSING_DATE = "SAP Closing Date"
RAW_COL_QUOTATION_STATUS = "Quotation Status"
RAW_COL_RESO_REPAIR_REASON = "Repair Reason"


# -----------------------------------------------------------------------------
# Standardized Column Names (used in the final DataFrame)
# Adjusted to match all corresponding RAW_COL definitions
# -----------------------------------------------------------------------------
COL_SERVICE_ORDER = "service_order"
COL_PREVIOUS_SERVICE_ORDER = "previous_service_order"
COL_CUSTOMER_WORK_ORDER = "customer_work_order"
COL_SAP_EQUIPMENT_NAME = "sap_equipment_name"  # Maps from RAW_COL_EQUIPMENT_INTERNAL_NO
COL_SITE_NAME = "site_name"  # Maps from RAW_COL_LOCATION
COL_WORKSHOP = "workshop"
COL_MAIN_COMPONENT = "main_component"
COL_EQUIPMENT_MODEL = "equipment_model"
COL_COMPONENT = "component"
COL_COMPONENT_SERIAL = "component_serial"  # Maps from RAW_COL_COMPONENT_SERIES_NO
# Note: Maps from RAW_COL_COMPONENTS_HOURS which has raw name "Equipment Hour(s)"
COL_COMPONENTS_HOURS = "components_hours"
COL_RECEPTION_DATE = "reception_date"
COL_OPENING_DATE = "opening_date"
COL_WARRANTY_TYPE = "warranty_type"
COL_LOAD_PRELIM_REPORT_REVIEW = "load_preliminary_report_in_review"
COL_LOAD_PRELIM_REPORT_REVISED = "load_preliminary_report_revised"
COL_FIRST_QUOTATION_PUBLICATION = "first_quotation_publication"
COL_LATEST_QUOTATION_PUBLICATION = "latest_quotation_publication"
COL_DATE_SEND_QUOTATION = "date_send_quotation"
COL_APPROVAL_DATE = "approval_date"
COL_QUOTATION_REJECTION = "quotation_rejection"
COL_PURCHASE_ORDER = "purchase_order"  # Maps from RAW_PURCHASE_ORDER
COL_RESO_CLOSING_DATE = "reso_closing_date"
COL_SERVICE_ORDER_STATUS = "service_order_status"  # Added definition
COL_COMPONENT_STATUS = "component_status"
COL_LOAD_FINAL_REPORT_REVIEW = "load_final_report_in_review"
COL_LOAD_FINAL_REPORT_REVISED = "load_final_report_revised"
COL_SAP_CLOSING_DATE = "sap_closing_date"
COL_QUOTATION_STATUS = "quotation_status"
COL_RESO_REPAIR_REASON = "reso_repair_reason"  # Added definition


# -----------------------------------------------------------------------------
# List of raw columns selected from the source
# Updated to include ALL raw columns defined above
# -----------------------------------------------------------------------------
SELECTED_RAW_COLUMNS = [
    RAW_COL_SERVICE_ORDER,
    RAW_COL_PREVIOUS_SERVICE_ORDER,
    RAW_COL_CUSTOMER_WORK_ORDER,
    RAW_COL_EQUIPMENT_INTERNAL_NO,
    RAW_COL_LOCATION,
    RAW_COL_WORKSHOP,  # Added
    RAW_COL_MAIN_COMPONENT,
    RAW_COL_EQUIPMENT_MODEL,
    RAW_COL_COMPONENT,
    RAW_COL_COMPONENT_SERIES_NO,
    RAW_COL_COMPONENTS_HOURS,
    RAW_COL_RECEPTION_DATE,
    RAW_COL_OPENING_DATE,
    RAW_COL_WARRANTY_TYPE,  # Added
    RAW_COL_LOAD_PRELIM_REPORT_REVIEW,
    RAW_COL_LOAD_PRELIM_REPORT_REVISED,
    RAW_COL_FIRST_QUOTATION_PUBLICATION,
    RAW_COL_LATEST_QUOTATION_PUBLICATION,
    RAW_COL_DATE_SEND_QUOTATION,
    RAW_COL_APPROVAL_DATE,
    RAW_COL_QUOTATION_REJECTION,  # Added
    RAW_PURCHASE_ORDER,
    RAW_COL_RESO_CLOSING_DATE,
    RAW_COL_SERVICE_ORDER_STATUS,  # Added
    RAW_COL_COMPONENT_STATUS,  # Added
    RAW_COL_LOAD_FINAL_REPORT_REVIEW,
    RAW_COL_LOAD_FINAL_REPORT_REVISED,
    RAW_COL_SAP_CLOSING_DATE,
    RAW_COL_QUOTATION_STATUS,  # Added
    RAW_COL_RESO_REPAIR_REASON,  # Added
]

# -----------------------------------------------------------------------------
# Mapping from raw column names to standardized names
# Updated to include mappings for ALL raw columns defined above
# -----------------------------------------------------------------------------
COLUMN_MAPPING = {
    RAW_COL_SERVICE_ORDER: COL_SERVICE_ORDER,
    RAW_COL_PREVIOUS_SERVICE_ORDER: COL_PREVIOUS_SERVICE_ORDER,
    RAW_COL_CUSTOMER_WORK_ORDER: COL_CUSTOMER_WORK_ORDER,
    RAW_COL_EQUIPMENT_INTERNAL_NO: COL_SAP_EQUIPMENT_NAME,
    RAW_COL_LOCATION: COL_SITE_NAME,
    RAW_COL_WORKSHOP: COL_WORKSHOP,  # Added mapping
    RAW_COL_MAIN_COMPONENT: COL_MAIN_COMPONENT,
    RAW_COL_EQUIPMENT_MODEL: COL_EQUIPMENT_MODEL,
    RAW_COL_COMPONENT: COL_COMPONENT,
    RAW_COL_COMPONENT_SERIES_NO: COL_COMPONENT_SERIAL,
    # Note: Mapping raw "Equipment Hour(s)" to standardized "components_hours"
    RAW_COL_COMPONENTS_HOURS: COL_COMPONENTS_HOURS,
    RAW_COL_RECEPTION_DATE: COL_RECEPTION_DATE,
    RAW_COL_OPENING_DATE: COL_OPENING_DATE,
    RAW_COL_WARRANTY_TYPE: COL_WARRANTY_TYPE,  # Added mapping
    RAW_COL_LOAD_PRELIM_REPORT_REVIEW: COL_LOAD_PRELIM_REPORT_REVIEW,
    RAW_COL_LOAD_PRELIM_REPORT_REVISED: COL_LOAD_PRELIM_REPORT_REVISED,
    RAW_COL_FIRST_QUOTATION_PUBLICATION: COL_FIRST_QUOTATION_PUBLICATION,
    RAW_COL_LATEST_QUOTATION_PUBLICATION: COL_LATEST_QUOTATION_PUBLICATION,
    RAW_COL_DATE_SEND_QUOTATION: COL_DATE_SEND_QUOTATION,
    RAW_COL_APPROVAL_DATE: COL_APPROVAL_DATE,
    RAW_COL_QUOTATION_REJECTION: COL_QUOTATION_REJECTION,  # Added mapping
    RAW_PURCHASE_ORDER: COL_PURCHASE_ORDER,
    RAW_COL_RESO_CLOSING_DATE: COL_RESO_CLOSING_DATE,
    RAW_COL_SERVICE_ORDER_STATUS: COL_SERVICE_ORDER_STATUS,  # Added mapping
    RAW_COL_COMPONENT_STATUS: COL_COMPONENT_STATUS,  # Added mapping
    RAW_COL_LOAD_FINAL_REPORT_REVIEW: COL_LOAD_FINAL_REPORT_REVIEW,
    RAW_COL_LOAD_FINAL_REPORT_REVISED: COL_LOAD_FINAL_REPORT_REVISED,
    RAW_COL_SAP_CLOSING_DATE: COL_SAP_CLOSING_DATE,
    RAW_COL_QUOTATION_STATUS: COL_QUOTATION_STATUS,  # Added mapping
    RAW_COL_RESO_REPAIR_REASON: COL_RESO_REPAIR_REASON,  # Added mapping
}
# Columns to convert to integer after cleaning
INT_CONVERSION_COLUMNS = [
    COL_SERVICE_ORDER,
    COL_PREVIOUS_SERVICE_ORDER,
    COL_CUSTOMER_WORK_ORDER,
    COL_SAP_EQUIPMENT_NAME,
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
    COL_COMPONENTS_HOURS,
]
