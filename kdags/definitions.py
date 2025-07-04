from pathlib import Path

import dagster as dg
from dagster._core.definitions.metadata.source_code import AnchorBasedFilePathMapping


from kdags.assets import maintenance, reparation, operation, components, reliability, docs
from kdags.schedules import (
    component_history_schedule,
    harvest_so_report_schedule,
    oil_analysis_schedule,
    publish_data_schedule,
    ep_schedule,
    fiori_schedule,
)
from kdags.sensors import so_report_sensor, quotations_sensor
from kdags.jobs import (
    # === DOCS ===
    oil_analysis_job,
    docs_job,
    # === RELIABILITY ===
    component_fleet_job,
    pool_inventory_job,
    ep_job,
    warranties_job,
    quotations_job,
    component_reparations_job,
    tabulate_quotations_job,
    component_history_job,
    # === PLANNING ===
    # === REPARATION ===
    so_report_job,
    harvest_so_details_job,
    harvest_so_documents_job,
    reso_documents_job,
    # === MAINTENANCE ===
    icc_job,
    fiori_job,
    pm_history_job,
    op_file_idx_job,
    # === OPERATION ===
    komtrax_job,
    plm_job,
    ge_job,
    harvest_so_report_job,
    publish_data_job,
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


operation_assets = dg.load_assets_from_package_module(operation, group_name="operation")
maintenance_assets = dg.load_assets_from_package_module(maintenance, group_name="maintenance")
components_assets = dg.load_assets_from_package_module(components, group_name="components")
reliability_assets = dg.load_assets_from_package_module(reliability, group_name="reliability")
reparation_assets = dg.load_assets_from_package_module(reparation, group_name="reparation")
docs_assets = dg.load_assets_from_package_module(docs)

all_assets = dg.with_source_code_references(
    [
        *reparation_assets,
        *operation_assets,
        *maintenance_assets,
        *components_assets,
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


schedules = [
    component_history_schedule,
    harvest_so_report_schedule,
    oil_analysis_schedule,
    publish_data_schedule,
    ep_schedule,
    fiori_schedule,
]

if Path.home().__str__() not in ["C:\\Users\\vales"]:
    schedules = []

kdefs = dg.Definitions(
    assets=all_assets,
    jobs=[
        # === COMPONENTS ===
        component_reparations_job,
        harvest_so_report_job,
        warranties_job,
        # === DOCS ===
        docs_job,
        tabulate_quotations_job,
        # === RELIABILITY ===
        component_fleet_job,
        component_history_job,
        pool_inventory_job,
        ep_job,
        quotations_job,
        icc_job,
        # === REPARATION ===
        so_report_job,
        harvest_so_details_job,
        reso_documents_job,
        harvest_so_documents_job,
        # === MAINTENANCE ===
        fiori_job,
        pm_history_job,
        # === OPERATION ===
        op_file_idx_job,
        plm_job,
        ge_job,
        oil_analysis_job,
        komtrax_job,
        publish_data_job,
    ],
    schedules=schedules,
    sensors=[so_report_sensor, quotations_sensor],
)
