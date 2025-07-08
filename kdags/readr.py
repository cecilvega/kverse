"""
Direct registry of important classes and functions for easy access from notebooks.
"""

from dataclasses import dataclass

import dagster as dg


# === COMPONENTS ===


from kdags.assets.components.readr_assets import component_history, component_changeouts

# === MAINTENANCE ===
from kdags.assets.maintenance.readr_assets import oil_analysis, notifications
from kdags.assets.operation.ge.events.pipeline import read_events
from kdags.assets.maintenance.fiori.pm_history import read_pm_history


# === RELIABILITY ===
from kdags.assets.reliability.readr_assets import icc

# from kdags.assets.planning.pool_inventory.assets import component_lifeline

# === OPERATION ===
from kdags.assets.operation.readr_assets import komtrax_smr
from kdags.assets.operation.operation_files_idx import read_op_file_idx
from kdags.assets.operation.plm.alarms import read_alarms
from kdags.assets.operation.plm.haul import read_haul


# === REPARATION ===
from kdags.assets.reparation.so_report.assets import so_report

# from kdags.assets.reparation.component_status.assets import component_status
from kdags.assets.reparation.readr_assets import so_quotations, component_reparations, so_documents, quotations

from kdags.resources.tidyr.masterdata import MasterData

__all__ = ["Readr"]


@dataclass
class Readr:

    MasterData: MasterData = MasterData

    # === MAINTENANCE ===
    oil_analysis: dg.AssetsDefinition = oil_analysis
    notifications: dg.AssetsDefinition = notifications
    pm_history: dg.AssetsDefinition = read_pm_history

    # === OPERATION ===
    op_file_idx: dg.AssetsDefinition = read_op_file_idx
    komtrax_smr: dg.AssetsDefinition = komtrax_smr

    haul: dg.AssetsDefinition = read_haul
    alarms: dg.AssetsDefinition = read_alarms

    events: dg.AssetsDefinition = read_events

    # === REPARATION ===

    so_report: dg.AssetsDefinition = so_report
    so_quotations: dg.AssetsDefinition = so_quotations
    so_documents: dg.AssetsDefinition = so_documents
    quotations: dg.AssetsDefinition = quotations

    # === PLANNING ===
    component_changeouts: dg.AssetsDefinition = component_changeouts

    # === RELIABILITY ===
    icc: dg.AssetsDefinition = icc
    # changeouts_so: dg.AssetsDefinition = component_history
    component_reparations: dg.AssetsDefinition = component_reparations
    component_history: dg.AssetsDefinition = component_history
