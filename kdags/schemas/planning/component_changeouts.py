# kverse/kdags/schemas/planning/component_changeouts.py

import datetime  # Need to import date
import dagster as dg
from dagster import TableSchema, TableColumn

# --- Component Changeouts Schema using Standard Python Types ---
COMPONENT_CHANGEOUTS_SCHEMA = TableSchema(
    columns=[
        TableColumn("cc_index", "integer", description="Row index added during processing."),  # Use "integer" or "int"
        TableColumn(
            "equipment_name", "string", description="Identifier for the equipment (e.g., CEX###, TK###)."
        ),  # Use "string"
        TableColumn(
            "equipment_model", "string", description="Model of the equipment (e.g., 980E-5, 930E-4)."
        ),  # Use "string"
        TableColumn("component_name", "string", description="Name of the main component changed."),  # Use "string"
        TableColumn("subcomponent_name", "string", description="Name of the sub-component changed."),  # Use "string"
        TableColumn(
            "position_name", "string", description="Position of the component (e.g., derecho, izquierdo)."
        ),  # Use "string"
        TableColumn("changeout_date", "date", description="Date the component changeout occurred."),  # Use "date"
        TableColumn(
            "customer_work_order", "integer", description="Associated customer work order number (-1 if null)."
        ),  # Use "integer" or "int"
        TableColumn(
            "equipment_hours", "float", description="Hours on the equipment at the time of changeout."
        ),  # Use "float" or "double"
        TableColumn(
            "component_hours", "string", description="Hours on the component being removed (as string)."
        ),  # Use "string"
        TableColumn("tbo", "string", description="Time Between Overhaul target (as string)."),  # Use "string"
        TableColumn("component_usage", "string", description="Usage metric or status (as string)."),  # Use "string"
        TableColumn(
            "component_serial", "string", description="Serial number of the component removed."
        ),  # Use "string"
        TableColumn(
            "sap_equipment_name", "integer", description="SAP equipment number (-1 if null)."
        ),  # Use "integer" or "int"
        TableColumn(
            "installed_component_serial", "string", description="Serial number of the component installed."
        ),  # Use "string"
        TableColumn(
            "failure_description", "string", description="Description or reason for the changeout."
        ),  # Use "string"
        TableColumn(
            "site_name", "string", description="Site where the changeout occurred (e.g., MEL, SPENCE)."
        ),  # Use "string"
    ]
)
