from pathlib import Path

import dagster as dg
from dagster._core.definitions.metadata.source_code import AnchorBasedFilePathMapping

from kdags.assets import maintenance, reparation, operation, planning, reliability
from kdags.jobs import (
    # === PLANNING ===
    component_changeouts_job,
    # === REPARATION ===
    component_status_job,
    scrape_component_status_job,
    # === MAINTENANCE ===
    attendances_job,
    oil_analysis_job,
    icc_job,
    work_order_history_job,
    pm_history_job,
    op_file_idx_job,
    # === OPERATION ===
    plm_job,
    ge_job,
)
import warnings

warnings.filterwarnings("ignore", category=Warning, module="dagster._core.definitions.metadata.source_code")
warnings.filterwarnings(
    "ignore", message=".*Function `with_source_code_references` is currently in beta.*", category=Warning
)
warnings.filterwarnings(
    "ignore", message=".*Function `link_code_references_to_git` is currently in beta.*", category=Warning
)
warnings.filterwarnings(
    "ignore", message=".*Class `AnchorBasedFilePathMapping` is currently in beta.*", category=Warning
)
__all__ = ["kdefs"]

# warnings.filterwarnings("ignore", category=ExperimentalWarning)

operation_assets = dg.load_assets_from_package_module(operation, group_name="operation")
maintenance_assets = dg.load_assets_from_package_module(maintenance, group_name="maintenance")
planning_assets = dg.load_assets_from_package_module(planning, group_name="planning")
reliability_assets = dg.load_assets_from_package_module(reliability, group_name="reliability")
reparation_assets = dg.load_assets_from_package_module(reparation, group_name="reparation")


all_assets = dg.with_source_code_references(
    [
        *reparation_assets,
        *operation_assets,
        *maintenance_assets,
        *planning_assets,
        *reliability_assets,
    ]
)

all_assets = dg.link_code_references_to_git(
    assets_defs=all_assets,
    git_url="https://github.com/cecilvega/kverse/",
    git_branch="main",
    file_path_mapping=AnchorBasedFilePathMapping(
        local_file_anchor=Path(__file__).parent,
        file_anchor_path_in_repository="kdags/",
    ),
)

kdefs = dg.Definitions(
    assets=all_assets,
    schedules=[
        # === PLANNING ===
        component_changeouts_job,
        # === REPARATION ===
        component_status_job,
        scrape_component_status_job,
        # === MAINTENANCE ===
        attendances_job,
        icc_job,
        work_order_history_job,
        pm_history_job,
        oil_analysis_job,
        # === OPERATION ===
        op_file_idx_job,
        plm_job,
        ge_job,
    ],
)
