import yaml
from pathlib import Path
import os

CONFIG_DIR = Path(os.path.dirname(os.path.abspath(__file__)))  # if paths_loader.py is in kdags/config


def load_data_catalog():
    catalog_path = CONFIG_DIR / "data_catalog.yaml"
    with open(catalog_path, "r", encoding="utf-8") as f:
        data_catalog = yaml.safe_load(f)

    catalog = {}
    for group_name, group in data_catalog.items():
        for asset_name, asset in group.items():

            catalog.update({asset_name: {"group_name": group_name, **asset}})

    return catalog


DATA_CATALOG = load_data_catalog()
