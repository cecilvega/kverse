from dataclasses import dataclass
import yaml
import pandas as pd
import os
from pathlib import Path


@dataclass
class MasterData:
    """
    Class for accessing master data elements used throughout the kverse project.
    Provides consistent access to reference data like components, equipment, etc.
    """

    # Class variable to store the config directory path
    CONFIG_DIR = Path(os.path.dirname(os.path.abspath(__file__)))

    @classmethod
    def components(cls) -> pd.DataFrame:
        """
        Load and process component data from components.yaml.

        Returns:
            pd.DataFrame: Processed DataFrame containing components and subcomponents
        """
        config_path = cls.CONFIG_DIR / "components.yaml"

        # Verify file exists
        assert config_path.exists(), f"Components config file not found at {config_path}"

        # Load the YAML data
        with open(config_path, "r") as f:
            data = yaml.safe_load(f)

        # Create DataFrame and process the data
        df = pd.DataFrame(data)
        df = df.explode("subcomponent")

        # Normalize the nested structure
        df = pd.json_normalize(
            df.to_dict("records"),
            record_path=None,
            meta=["component_name", "component_code"],
            sep="_",
        ).drop(columns=["subcomponent"])

        return df

    @classmethod
    def positions(cls) -> pd.DataFrame:
        """
        Load position mapping data from positions.yaml.

        Returns:
            pd.DataFrame: Position codes and their corresponding names
        """
        config_path = cls.CONFIG_DIR / "positions.yaml"

        # Verify file exists
        assert config_path.exists(), f"Positions config file not found at {config_path}"

        # Load the YAML data
        with open(config_path, "r") as f:
            data = yaml.safe_load(f)

        # Create DataFrame directly from the list of dictionaries
        df = pd.DataFrame(data)

        # Ensure position_code is treated as integer
        df["position_code"] = df["position_code"].astype(int)

        return df

    @classmethod
    def taxonomy(cls) -> pd.DataFrame:
        components_df = cls.components()
        positions_df = cls.positions()
        taxonomy_df = pd.merge(
            components_df.explode("position_name").drop(columns=["subcomponent_main"]),
            positions_df,
            how="left",
            on="position_name",
        )
        return taxonomy_df

    @classmethod
    def equipments(cls) -> pd.DataFrame:
        """
        Load position mapping data from positions.yaml.

        Returns:
            pd.DataFrame: Position codes and their corresponding names
        """
        config_path = cls.CONFIG_DIR / "equipments.yaml"

        # Verify file exists
        assert config_path.exists(), f"Positions config file not found at {config_path}"

        # Load the YAML data
        with open(config_path, "r") as f:
            data = yaml.safe_load(f)

        # Create DataFrame directly from the list of dictionaries
        df = pd.DataFrame(data).T.rename_axis("equipment_name").reset_index()
        # df = pd.pivot(df, index="equipment_name")

        # Ensure position_code is treated as integer
        # df["position_code"] = df["position_code"].astype(int)

        return df
