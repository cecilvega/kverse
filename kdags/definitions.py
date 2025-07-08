from pathlib import Path

import dagster as dg
from dagster._core.definitions.metadata.source_code import AnchorBasedFilePathMapping


from kdags.assets import maintenance, reparation, operation, components, reliability, docs
from kdags.schedules import schedules
from kdags.sensors import sensors
from kdags.jobs import jobs
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


if Path.home().__str__() not in ["C:\\Users\\vales"]:
    schedules = []
    sensors = []

kdefs = dg.Definitions(
    assets=all_assets,
    jobs=jobs,
    schedules=schedules,
    sensors=sensors,
)
