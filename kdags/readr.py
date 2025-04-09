"""
Direct registry of important classes and functions for easy access from notebooks.
"""

from dataclasses import dataclass

import dagster as dg


# === PLANNING ===
from kdags.assets.planning.component_changeouts.assets import read_component_changeouts

# === MAINTENANCE ===
from kdags.assets.maintenance.tribology.processing import read_oil_analysis
from kdags.assets.operation.ge.events.pipeline import read_events
from kdags.assets.maintenance.fiori.pm_history import read_pm_history
from kdags.assets.maintenance.fiori.work_orders_history import read_work_order_history

# === OPERATION ===
from kdags.assets.operation.operation_files_idx import read_op_file_idx
from kdags.assets.operation.plm.alarms import read_alarms
from kdags.assets.operation.plm.haul import read_haul

from kdags.assets.reliability.icc.assets import read_icc

# === REPARATION ===
from kdags.assets.reparation.component_status.assets import read_component_status
from kdags.resources.tidyr.masterdata import MasterData

__all__ = ["Readr"]


class Readr:

    MasterData: MasterData = MasterData

    @dataclass
    class Maintenance:

        read_oil_analysis: dg.AssetsDefinition = read_oil_analysis
        read_work_order_history: dg.AssetsDefinition = read_work_order_history
        read_pm_history: dg.AssetsDefinition = read_pm_history

    @dataclass
    class Operation:

        read_op_file_idx: dg.AssetsDefinition = read_op_file_idx

        read_haul: dg.AssetsDefinition = read_haul
        read_alarms: dg.AssetsDefinition = read_alarms

        read_events: dg.AssetsDefinition = read_events

    @dataclass
    class Reparation:
        read_component_status: dg.AssetsDefinition = read_component_status

    @dataclass
    class Planning:
        read_component_changeouts: dg.AssetsDefinition = read_component_changeouts

    @dataclass
    class Reliability:
        read_icc: dg.AssetsDefinition = read_icc

    # @staticmethod
    # def get_asset_by_path(defs: dg.Definitions, path: str) -> dg.AssetsDefinition:
    #     """
    #     Find an asset from a Dagster definitions object by its path.
    #
    #     Args:
    #         defs: A Dagster definitions object
    #         path: A string representing the asset path
    #
    #     Returns:
    #         The matching asset
    #
    #     Raises:
    #         KeyError: If no asset with the given path is found
    #     """
    #     # Get all assets from the definitions
    #     all_assets = defs.assets
    #
    #     # Create the AssetKey from the path
    #     asset_key = dg.AssetKey(path)
    #
    #     # Find the asset with the matching key
    #     for asset in all_assets:
    #         if asset.key == asset_key:
    #             return asset
    #
    #     # Raise an error if not found
    #     raise KeyError(f"Asset with path {path} not found in definitions")
    #
    # @staticmethod
    # def create_asset_catalog(definitions: dg.Definitions) -> pl.DataFrame:
    #     """
    #     Create a DataFrame catalog of all assets in the Dagster definitions.
    #
    #     Args:
    #         definitions: Dagster Definitions object
    #
    #     Returns:
    #         Polars DataFrame with asset metadata including name, group, dependencies, etc.
    #     """
    #     # Use the assets list directly from the definitions object
    #     assets = definitions.assets
    #
    #     catalog = []
    #     for asset in assets:
    #         asset_key = asset.key
    #
    #         # Get basic metadata
    #         asset_deps = asset.asset_deps.get(asset_key, None)
    #         asset_deps = [next(iter(dep.path)) for dep in asset_deps] if asset_deps else None
    #         entry = {
    #             "name": next(iter(asset_key.path)),
    #             "asset": asset,
    #             "group_name": asset.group_names_by_key[asset_key],
    #             "description": asset.descriptions_by_key.get(asset_key, None),
    #             "asset_deps": asset_deps,
    #         }
    #
    #         catalog.append(entry)
    #
    #     df = pl.DataFrame(catalog)
    #
    #     return df
