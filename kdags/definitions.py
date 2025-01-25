from pathlib import Path

from dagster import (
    Definitions,
    graph_asset,
    link_code_references_to_git,
    load_assets_from_package_module,
    op,
    asset,
    with_source_code_references,
    ExperimentalWarning,
)
from dagster._core.definitions.metadata.source_code import AnchorBasedFilePathMapping

from kdags.assets import quickstart
from kdags.assets import planification
from kdags.assets import maintenance
from kdags.jobs import daily_refresh_schedule, daily_attendances_job
import warnings
from kdags.resources.msgraph.auth import acquire_token_func

warnings.filterwarnings("ignore", category=ExperimentalWarning)

import os


@asset
def test_msgraph(context):
    refresh_token = os.environ["MSGRAPH_TOKEN"]
    token = acquire_token_func()
    context.log.info(token)
    context.log.info(refresh_token)
    return token


@op
def foo_op():
    refresh_token = os.environ["MSGRAPH_TOKEN"]

    return refresh_token


@graph_asset
def my_asset():
    return foo_op()


maintenance_assets = load_assets_from_package_module(maintenance, group_name="maintenance")
planification_assets = load_assets_from_package_module(planification, group_name="planification")
hackernews_assets = load_assets_from_package_module(quickstart)


all_assets = with_source_code_references(
    [
        my_asset,
        test_msgraph,
        *hackernews_assets,
        *maintenance_assets,
        *planification_assets,
    ]
)

all_assets = link_code_references_to_git(
    assets_defs=all_assets,
    git_url="https://github.com/cecilvega/kverse/",
    git_branch="main",
    file_path_mapping=AnchorBasedFilePathMapping(
        local_file_anchor=Path(__file__).parent,
        file_anchor_path_in_repository="kdags/",
    ),
)

defs = Definitions(
    assets=all_assets,
    schedules=[daily_refresh_schedule, daily_attendances_job],
)
