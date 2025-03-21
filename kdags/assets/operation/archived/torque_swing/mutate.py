from dataclasses import dataclass, field

import pandas as pd

# from kdags.resources.tidypolars.testthat import *
# from kdags.assets.operation.common.utils import *
# from data_science_tools.config import Config


@dataclass
class TorqueSwing:
    features: dict = field(default_factory=lambda: Config().features["trqsw"])
    machine_columns: list = field(default_factory=lambda: ["faena", "filename_serial", "header_machine_name"])

    def mutate(self, df: pd.DataFrame):
        df = df.pipe(
            bind_master_machines,
            columns_first_layer=[
                "faena",
                "filename_serial",
                "merge_header_machine_name",
            ],
            columns_second_layer=["faena", "header_machine_name"],
            columns_third_layer=["faena", "filename_serial"],
        ).rename(columns={v["name"]: k for k, v in self.features.items() if v["raw"] is True})

        int_columns = [k for k, v in self.features.items() if ((v["datatype"] == "int") & (k in df.columns))]
        df[int_columns] = df[int_columns].apply(lambda x: pd.to_numeric(x).astype(int))

        num_columns = [k for k, v in self.features.items() if ((v["datatype"] == "num") & (k in df.columns))]
        df[num_columns] = df[num_columns].apply(lambda x: pd.to_numeric(x))
        df = df.drop(columns=self.machine_columns, errors="ignore")
        assert_no_nulls(df, "machine_id")
        df["metric_datetime"] = pd.to_datetime(df["Date"].dt.strftime("%Y-%m-%d %H:%M:%S"), format="%Y-%m-%d %H:%M:%S")
        df = df.sort_values(["machine_id", "metric_datetime"]).reset_index(drop=True)

        features = [k for k, v in self.features.items() if k in df.columns]
        df = df.groupby(["machine_id", "metric_datetime"])[features].mean().reset_index()
        return df
