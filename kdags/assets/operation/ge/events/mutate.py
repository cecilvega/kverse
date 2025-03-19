import pandas as pd


def mutate_events(df):
    columns_map = {
        "Time": "metric_datetime",
    }
    df = df.rename(columns=columns_map)
    int_columns = ["Event #", "Sub ID"]
    df[int_columns] = df[int_columns].apply(lambda x: pd.to_numeric(x).astype(int))
    chr_columns = ["Type", "Name", "Sub ID Name"]
    df[chr_columns] = df[chr_columns].apply(lambda x: x.str.strip())
    df = df.assign(
        metric=df["Type"]
        .str.strip()
        .str.replace("----", "")
        .str.cat(df["Event #"].astype(str), sep="")
        .str.cat(df["Sub ID"].astype(str), sep="-"),
        metric_name=df["Name"].str.cat(df["Sub ID Name"]).str.replace("--", ""),
    )
    df = df.rename(columns={"Time": "metric_datetime"}).drop(columns=["Extra"])
    return df
