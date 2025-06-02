"""
Direct registry of important classes and functions for easy access from notebooks.
"""

from dataclasses import dataclass

import dagster as dg


# === PLANNING ===
from kdags.assets.planning.component_changeouts.assets import component_changeouts

# === MAINTENANCE ===
from kdags.assets.maintenance.tribology.assets import oil_analysis
from kdags.assets.operation.ge.events.pipeline import read_events
from kdags.assets.maintenance.fiori.pm_history import read_pm_history
from kdags.assets.maintenance.fiori.work_orders_history import read_work_order_history

# === RELIABILITY ===
from kdags.assets.reliability.icc.assets import icc
from kdags.assets.reliability.deprecated import component_reparations
from kdags.assets.reliability.component_reparations import component_reparations
from kdags.assets.planning.pool_inventory.assets import component_lifeline

# === OPERATION ===
from kdags.assets.operation.operation_files_idx import read_op_file_idx
from kdags.assets.operation.plm.alarms import read_alarms
from kdags.assets.operation.plm.haul import read_haul


# === REPARATION ===
from kdags.assets.reparation.so_report.assets import so_report

# from kdags.assets.reparation.component_status.assets import component_status
from kdags.assets.reparation.so_details.assets import quotations

from kdags.resources.tidyr.masterdata import MasterData

__all__ = ["Readr"]


@dataclass
class Readr:

    MasterData: MasterData = MasterData

    # === MAINTENANCE ===
    oil_analysis: dg.AssetsDefinition = oil_analysis
    work_order_history: dg.AssetsDefinition = read_work_order_history
    pm_history: dg.AssetsDefinition = read_pm_history

    # === OPERATION ===
    op_file_idx: dg.AssetsDefinition = read_op_file_idx

    haul: dg.AssetsDefinition = read_haul
    alarms: dg.AssetsDefinition = read_alarms

    events: dg.AssetsDefinition = read_events

    # === REPARATION ===

    so_report: dg.AssetsDefinition = so_report
    quotations: dg.AssetsDefinition = quotations

    # === PLANNING ===
    component_changeouts: dg.AssetsDefinition = component_changeouts

    # === RELIABILITY ===
    icc: dg.AssetsDefinition = icc
    changeouts_so: dg.AssetsDefinition = component_reparations
    component_reparations: dg.AssetsDefinition = component_reparations
    component_lifeline: dg.AssetsDefinition = component_lifeline
