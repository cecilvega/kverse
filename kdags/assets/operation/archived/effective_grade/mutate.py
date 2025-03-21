from dataclasses import dataclass, field

import pandas as pd

# from kdags.assets.operation.assets.common.utils import *
# from kdags.resources.tidypolars.testthat import *
# from data_science_tools.config import Config


@dataclass
class EffectiveGrade:
    machine_columns: list = field(default_factory=lambda: ["faena", "filename_serial", "header_machine_name"])
    columns_map: dict = field(default_factory=lambda: {"Date": "metric_datetime"})
    features: dict = field(default_factory=lambda: Config().features["eg"])

    def mutate(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.pipe(
            bind_master_machines,
            columns_first_layer=[
                "faena",
                "filename_serial",
                "header_machine_name",
            ],
            columns_second_layer=["faena", "merge_header_machine_name"],
            columns_third_layer=["faena", "filename_serial"],
        ).rename(columns={**self.columns_map, **{v["name"]: k for k, v in self.features.items() if v["raw"] is True}})

        int_columns = [k for k, v in self.features.items() if ((v["datatype"] == "int") & (k in df.columns))]

        # Botar todos los EG si no están
        df[int_columns] = df[int_columns].apply(lambda x: pd.to_numeric(x, errors="coerce"))
        df = df.dropna(subset=int_columns).reset_index(drop=True)
        df[int_columns] = df[int_columns].astype(int)

        num_columns = [k for k, v in self.features.items() if ((v["datatype"] == "num") & (k in df.columns))]
        df[num_columns] = df[num_columns].apply(lambda x: pd.to_numeric(x, errors="coerce"))

        df = df.drop(columns=self.machine_columns, errors="ignore")
        assert_no_nulls(df, "machine_id")
        df = df.sort_values(["machine_id", "metric_datetime"]).reset_index(drop=True)

        df["metric_datetime"] = pd.to_datetime(
            df["metric_datetime"].dt.strftime("%Y-%m-%d %H:%M:%S"), format="%Y-%m-%d %H:%M:%S"
        )
        features = [k for k, v in self.features.items() if k in df.columns]
        df = df.groupby(["machine_id", "metric_datetime"])[features].mean().reset_index()
        return df
