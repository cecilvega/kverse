from pathlib import Path

from dagster import (
    Definitions,
    graph_asset,
    link_code_references_to_git,
    load_assets_from_package_module,
    op,
    with_source_code_references,
    ExperimentalWarning,
)
from dagster._core.definitions.metadata.source_code import AnchorBasedFilePathMapping

from kdags.assets import quickstart
from kdags.jobs import daily_refresh_schedule
import warnings


warnings.filterwarnings("ignore", category=ExperimentalWarning)


@op
def foo_op():
    return 5


@graph_asset
def my_asset():
    return foo_op()


hackernews_assets = load_assets_from_package_module(quickstart)  # , group_name="hackernews"


all_assets = with_source_code_references(
    [
        my_asset,
        *hackernews_assets,
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
    schedules=[daily_refresh_schedule],
)
