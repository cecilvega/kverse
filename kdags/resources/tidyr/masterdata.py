import os
from dataclasses import dataclass
from pathlib import Path

import polars as pl
import yaml


@dataclass
class MasterData:
    """
    Class for accessing master data elements used throughout the kverse project.
    Provides consistent access to reference data like components, equipment, etc.
    """

    # Class variable to store the config directory path
    CONFIG_DIR = Path(os.path.dirname(os.path.abspath(__file__))).parents[1] / "config"

    @classmethod
    def components(cls) -> pl.DataFrame:
        """
        Load and process component data from components.yaml.

        Returns:
            pl.DataFrame: Processed DataFrame containing components and subcomponents
        """
        config_path = cls.CONFIG_DIR / "components.yaml"

        # Verify file exists
        assert config_path.exists(), f"Components config file not found at {config_path}"

        # Load the YAML data
        with open(config_path, "r") as f:
            data = yaml.safe_load(f)

        # Create DataFrame and process the data
        df = pl.from_dicts(data)
        df = df.explode("subcomponent")
        records = df.to_dicts()

        normalized_df = pl.json_normalize(records, separator="_")

        return normalized_df

    @classmethod
    def positions(cls) -> pl.DataFrame:
        """
        Load position mapping data from positions.yaml.

        Returns:
            pl.DataFrame: Position codes and their corresponding names
        """
        config_path = cls.CONFIG_DIR / "positions.yaml"

        # Verify file exists
        assert config_path.exists(), f"Positions config file not found at {config_path}"

        # Load the YAML data
        with open(config_path, "r") as f:
            data = yaml.safe_load(f)

        # Create DataFrame directly from the list of dictionaries
        df = pl.from_dicts(data)

        # Ensure position_code is treated as integer
        df = df.with_columns(pl.col("position_code").cast(pl.Int64))

        return df

    @classmethod
    def taxonomy(cls) -> pl.DataFrame:
        components_df = cls.components()
        positions_df = cls.positions()

        # Explode the "position_name" column and drop "subcomponent_main"
        exploded_components = components_df.explode("position_name").drop("subcomponent_main")

        # Perform a left join on "position_name"
        df = exploded_components.join(positions_df, on="position_name", how="left")
        # Fill subcomponent_name with component_name and subcomponent_code with component_code
        # only when subcomponent_name is null and component_name is not null
        # Fill subcomponent_name with component_name and subcomponent_code with component_code
        # only when subcomponent_name is null and component_name is not null
        df = df.with_columns(
            [
                pl.when(pl.col("subcomponent_name").is_null() & pl.col("component_name").is_not_null())
                .then(pl.col("component_name"))
                .otherwise(pl.col("subcomponent_name"))
                .alias("subcomponent_name"),
                pl.when(pl.col("subcomponent_name").is_null() & pl.col("component_name").is_not_null())
                .then(pl.col("component_code"))
                .otherwise(pl.col("subcomponent_code"))
                .alias("subcomponent_code"),
            ]
        )
        return df

    @classmethod
    def equipments(cls) -> pl.DataFrame:
        """
        Load equipment mapping data from equipments.yaml.

        Returns:
            pl.DataFrame: Equipment data
        """
        config_path = cls.CONFIG_DIR / "equipments.yaml"

        # Verify file exists
        assert config_path.exists(), f"Equipments config file not found at {config_path}"

        # Load the YAML data
        with open(config_path, "r") as f:
            data = yaml.safe_load(f)

        equipments = []
        for equipment, details in data.items():
            e = {
                "equipment_name": equipment,
                "equipment_model": details.get("equipment_model"),
                "equipment_serial": details.get("equipment_serial"),
                "site_name": details.get("site_name"),
                "operating": details.get("operating"),
            }
            equipments.append(e)
        df = pl.DataFrame(equipments)

        return df

    @classmethod
    def read_io_map(cls) -> pl.DataFrame:

        config_path = cls.CONFIG_DIR / "io_map.yaml"

        # Load the YAML data - will raise YAMLError if invalid
        with open(config_path, "r", encoding="utf-8-sig") as f:
            data = yaml.safe_load(f)

        jobs = []

        for module, items in data.items():
            for name, details in items.items():
                input_details = details.get("input", {})
                output_details = details.get("output", {})

                row = {
                    "name": name,
                    "publish_path": details.get("publish_path"),
                    "module": module,
                    "job_name": details.get("job_name"),
                    "source": details.get("source"),
                    "description": details.get("description"),
                    "cron_schedule": details.get("cron_schedule"),
                    "schedule_status": details.get("schedule_status"),
                    "input_path": input_details.get("path").__str__(),
                    "output_path": output_details.get("path").__str__(),
                }
                jobs.append(row)

        df = pl.DataFrame(jobs)

        return df
