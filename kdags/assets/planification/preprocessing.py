import pandas as pd


def modify_dataframe(df):
    # Sort the dataframe
    df = df.sort_values(["pool_slot", "changeout_date"])

    # Create new dataframes for overlaps and gaps
    overlaps = []
    gaps = []

    # Iterate through the dataframe
    for i in range(len(df) - 1):
        current_row = df.iloc[i]
        next_row = df.iloc[i + 1]

        # Check for overlaps
        if (
            current_row["pool_slot"] == next_row["pool_slot"]
            and current_row["arrival_date"] > next_row["changeout_date"]
        ):

            overlap_start = next_row["changeout_date"]
            overlap_end = min(current_row["arrival_date"], next_row["arrival_date"])

            overlaps.append(
                {
                    "pool_slot": current_row["pool_slot"],
                    "changeout_date": overlap_start,
                    "arrival_date": overlap_end,
                    "equipo": next_row["equipo"],
                    "component_serial": next_row["component_serial"],
                    "pool_changeout_type": "A",  # A for Ahead of schedule
                    "component": current_row["component"],
                    "componente": current_row["componente"],
                    "subcomponent": current_row["subcomponent"],
                    "position": current_row["position"],
                }
            )

        # Check for gaps
        if current_row["pool_slot"] == next_row["pool_slot"]:
            gap_start = current_row["arrival_date"]
            gap_end = next_row["changeout_date"]

            if gap_start < gap_end:
                gaps.append(
                    {
                        "pool_slot": current_row["pool_slot"],
                        "changeout_date": gap_start,
                        "arrival_date": gap_end,
                        "pool_changeout_type": "R",  # R for Ready
                        "component": current_row["component"],
                        "componente": current_row["componente"],
                        "subcomponent": current_row["subcomponent"],
                        "position": current_row["position"],
                    }
                )

    # Create dataframes from the new rows and concatenate with the original
    overlaps_df = pd.DataFrame(overlaps)
    gaps_df = pd.DataFrame(gaps)
    result_df = pd.concat([df, overlaps_df, gaps_df], ignore_index=True)

    # Sort the final dataframe
    result_df = result_df.sort_values(["pool_slot", "changeout_date"])

    # Forward fill component_code, componente, subcomponente, and position
    result_df[["component", "subcomponent", "position"]] = result_df.groupby("pool_slot")[
        ["component", "subcomponent", "position"]
    ].ffill()
    result_df[["changeout_date", "arrival_date"]] = result_df[["changeout_date", "arrival_date"]].apply(
        lambda x: pd.to_datetime(x, format="%Y-%m-%dT00:00:00.000")
    )

    # Generate changeout_week and arrival_week
    result_df["changeout_week"] = result_df["changeout_date"].dt.strftime("%Y-W%V")
    result_df["arrival_week"] = result_df["arrival_date"].dt.strftime("%Y-W%V")
    return result_df
