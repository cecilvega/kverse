from pathlib import Path

import dagster as dg
from dagster._core.definitions.metadata.source_code import AnchorBasedFilePathMapping

from kdags.assets import maintenance, reparation, operation
from kdags.jobs import (
    attendances_job,
    oil_analysis_job,
    icc_job,
    work_order_history_job,
    pm_history_job,
    op_file_idx_job,
    plm_job,
    ge_job,
)
from kdags.config.masterdata import MasterData


__all__ = ["kdefs", "MasterData"]

# warnings.filterwarnings("ignore", category=ExperimentalWarning)

operation_assets = dg.load_assets_from_package_module(operation, group_name="operation")
maintenance_assets = dg.load_assets_from_package_module(maintenance, group_name="maintenance")
# planification_assets = dg.load_assets_from_package_module(planification, group_name="planification")
reparation_assets = dg.load_assets_from_package_module(reparation, group_name="reparation")


all_assets = dg.with_source_code_references(
    [
        *reparation_assets,
        *operation_assets,
        *maintenance_assets,
        # *planification_assets,
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
        attendances_job,
        icc_job,
        work_order_history_job,
        pm_history_job,
        oil_analysis_job,
        op_file_idx_job,
        plm_job,
        ge_job,
    ],
)
