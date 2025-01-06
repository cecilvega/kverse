import pandas as pd
import re


# Function to extract information from comments
def extract_info(comment):
    equipo_match = re.search(r"Equipo:\s*(\d+)", comment)
    ns_match = re.search(r"NS:\s*(#+\w*-?\w*)", comment)
    ns = ns_match.group(1) if ns_match else None
    ns = re.sub(r"^#+", "#", ns)
    equipo = equipo_match.group(1) if equipo_match else None

    return equipo, ns


def idx_to_pool_slot(df):
    df = (
        df.reset_index()
        .rename(columns={"index": "pool_slot"})
        .assign(pool_slot=lambda x: x["pool_slot"].str.strip("N°"))
    )
    return df


# Function to get weeks and comments where value is 1
def get_weeks_and_comments(row, sheet):
    weeks = []
    comments = []
    row_idx = int(row.name.strip("N°"))
    col_idx = 1
    for col, value in row.items():
        if value == 1:
            weeks.append(col)
            cell = sheet[row_idx + 1][col_idx]
            comments.append(cell.comment.text if cell.comment else "")
        col_idx += 1
    return pd.Series({"weeks": weeks, "comments": comments})


def get_end_week(row, sheet):
    weeks = []
    pool_changeout_types = []
    row_idx = int(row.name.strip("N°"))
    for col_idx in range(0, row.__len__()):
        if col_idx > 2:
            prev_cell = sheet[row_idx + 1][col_idx - 1]
            cell = sheet[row_idx + 1][col_idx]
            if (
                (prev_cell.fill.fgColor.rgb != cell.fill.fgColor.rgb)
                & (prev_cell.value is None)
                & (prev_cell.fill.fgColor.rgb in ["FFEDBFBB", "FFC5E0B4", "FFE88880"])
                # & (prev_cell.fill.fgColor.rgb in ["FFC5E0B4", "FFE88880"])
            ):
                weeks.append(list(row.keys())[col_idx - 2])
                if prev_cell.fill.fgColor.rgb in ["FFEDBFBB", '"FFE88880"']:
                    pool_changeout_types.append("I")
                else:
                    pool_changeout_types.append("P")

    return pd.Series({"weeks": weeks, "pool_changeout_type": pool_changeout_types})
