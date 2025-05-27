from pathlib import Path

import dagster as dg
from dagster._core.definitions.metadata.source_code import AnchorBasedFilePathMapping


from kdags.assets import maintenance, reparation, operation, planning, reliability, docs
from kdags.jobs import (
    # === DOCS ===
    docs_job,
    # === RELIABILITY ===
    pool_monitoring_job,
    ep_job,
    warranties_job,
    quotations_job,
    # === PLANNING ===
    # === REPARATION ===
    harvest_so_report_job,
    component_reparations_job,
    harvest_reso_job,
    # === MAINTENANCE ===
    oil_analysis_job,
    icc_job,
    work_order_history_job,
    pm_history_job,
    op_file_idx_job,
    # === OPERATION ===
    plm_job,
    ge_job,
    publish_sp_job,
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


operation_assets = dg.load_assets_from_package_module(operation)
maintenance_assets = dg.load_assets_from_package_module(maintenance)
planning_assets = dg.load_assets_from_package_module(planning)
reliability_assets = dg.load_assets_from_package_module(reliability)
reparation_assets = dg.load_assets_from_package_module(reparation)
docs_assets = dg.load_assets_from_package_module(docs)

all_assets = dg.with_source_code_references(
    [
        *reparation_assets,
        *operation_assets,
        *maintenance_assets,
        *planning_assets,
        *reliability_assets,
        *docs_assets,
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
        # === DOCS ===
        docs_job,
        # === RELIABILITY ===
        pool_monitoring_job,
        ep_job,
        warranties_job,
        quotations_job,
        icc_job,
        # === PLANNING ===
        # === REPARATION ===
        harvest_so_report_job,
        harvest_reso_job,
        component_reparations_job,
        # === MAINTENANCE ===
        work_order_history_job,
        pm_history_job,
        oil_analysis_job,
        # === OPERATION ===
        op_file_idx_job,
        plm_job,
        ge_job,
        publish_sp_job,
    ],
)
