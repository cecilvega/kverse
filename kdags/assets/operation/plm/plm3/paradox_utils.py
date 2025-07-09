import os
import shutil
import tempfile
import uuid
from pathlib import Path
from zipfile import ZipFile

import polars as pl
from pypxlib import Table

WEIGHT_FACTOR = 9.07  # Convert to tons
DISTANCE_FACTOR = 160.934  # Convert to kilometers
TORQUE_FACTOR = 27.65  # Convert to kNm

HAUL_COLUMNS = [
    "Frame_SN",
    "Seconds",
    "Total_Payload",
    "Cust_Unit",
    "Truck_Type",
    "Operator_ID",
    "Reserved1",
    "Reserved2",
    "Reserved3",
    "Reserved4",
    "Reserved5",
    "Reserved6",
    "Reserved7",
    "Reserved8",
    "Reserved9",
    "Reserved10",
    "DB_Reserved_1",
    "Peak_Pos_Frame_Torque",
    "TimePeakPosFrameTorque",
    "Peak_Neg_Frame_Torque",
    "TimePeakNegFrameTorque",
    "Peak_Sprung_Load",
    "Time_Peak_Sprung_Load",
    "Loaded_Haul_Distance",
    "Loaded_Haul_Time",
    "Loaded_Stop_Time",
    "Loaded_Max_Speed",
    "Time_Loaded_Max_Speed",
    "Time_Body_Lift_Off",
    "Empty_Carry_Back_Load",
    "Empty_Haul_Distance",
    "Empty_Haul_Time",
    "Empty_Stop_Time",
    "Empty_Max_Speed",
    "Time_Empty_Max_Speed",
    "Loaded_Start_Time",
    "Loading_Time",
    "Dumping_Time",
    "Lft_Front_Tire_Ton_mph",
    "Rt_Front_Tire_Ton_mph",
    "Rear_Tire_Ton_mph",
    "Swingload_Num",
    "Tare_Bin_Data",
    "Loaded_Bin_Data",
    "PDate",
    "PTime",
]


ALARMS_COLUMNS = [
    "Frame_SN",
    "Cust_Unit",
    "Alarm_Type",
    "Set_Date",
    "Set_Time",
    "Description",
    "Cleared_Date",
    "Cleared_Time",
]


def extract_from_paradox(zip_path: str) -> [pl.DataFrame, pl.DataFrame]:

    # Validate input file exists
    assert Path(zip_path).exists(), f"Zip file not found: {zip_path}"

    # Set up temporary directory for extraction
    unique_id = str(uuid.uuid4())
    temp_dir = os.path.join(tempfile.gettempdir(), f"paradox_extract_{unique_id}")
    os.makedirs(temp_dir, exist_ok=True)

    # Extract database from zip file
    with ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extract("Export_Haul.db", temp_dir)
        zip_ref.extract("Export_Alarms.db", temp_dir)

    # Construct path to extracted file
    haul_temp_db_path = os.path.join(temp_dir, "Export_Haul.db")
    alarms_temp_db_path = os.path.join(temp_dir, "Export_Alarms.db")
    assert Path(haul_temp_db_path).exists(), "Failed to extract Export_Haul.db from zip file"
    assert Path(alarms_temp_db_path).exists(), "Failed to extract Export_Alarms.db from zip file"

    # Open database and read to DataFrame
    haul_table = Table(haul_temp_db_path)
    alarms_table = Table(alarms_temp_db_path)

    haul_df = extract_haul_from_paradox(haul_table)
    alarms_df = extract_alarms_from_paradox(alarms_table)

    haul_table.close()
    alarms_table.close()

    # Clean up temporary files
    shutil.rmtree(temp_dir, ignore_errors=True)

    return haul_df, alarms_df


def extract_haul_from_paradox(table: Table) -> pl.DataFrame:

    df = pl.DataFrame([[row[col] for col in HAUL_COLUMNS] for row in table], schema=HAUL_COLUMNS, orient="row")
    # Process the data using method chaining for better readability
    df = df.with_columns(
        [
            pl.sum_horizontal(
                [
                    "Empty_Haul_Time",
                    "Empty_Stop_Time",
                    "Loading_Time",
                    "Loaded_Haul_Time",
                    "Loaded_Stop_Time",
                    "Dumping_Time",
                ]
            ).alias("Total_Cycle_Time"),
            (pl.col("Total_Payload") + pl.col("Empty_Carry_Back_Load")).alias("Gross_Payload"),
        ]
    )
    df = (
        # Rename columns and select only the renamed ones
        df.drop(
            [
                "Frame_SN",
                "Seconds",
                "Truck_Type",
                "Reserved2",  # Tare_Sprung_Weight
                "Reserved3",
                "Reserved4",
                "Reserved5",
                "Reserved6",
                "Reserved7",
                "Reserved8",
                "Reserved9",
                "Reserved10",
                "DB_Reserved_1",
            ]
        )
        # Apply conversions and round values
        .with_columns(
            # Weight conversions
            **{
                col: (pl.col(col) / WEIGHT_FACTOR).round(2)
                for col in [
                    "Total_Payload",
                    "Empty_Carry_Back_Load",
                    "Gross_Payload",
                    "Peak_Sprung_Load",
                ]
            },
            # Distance conversions
            **{
                col: (pl.col(col) / DISTANCE_FACTOR).round(2)
                for col in ["Loaded_Haul_Distance", "Empty_Haul_Distance", "Loaded_Max_Speed", "Empty_Max_Speed"]
            },
            # Torque conversions
            **{
                col: (pl.col(col) / TORQUE_FACTOR).round(2)
                for col in ["Peak_Pos_Frame_Torque", "Peak_Neg_Frame_Torque"]
            },
        )
    ).with_columns(Cust_Unit=pl.col("Cust_Unit").str.extract(r"(\d{3})$", 1).pipe(lambda x: pl.lit("TK") + x))

    return df


def extract_alarms_from_paradox(table: Table) -> pl.DataFrame:

    df = pl.DataFrame([[row[col] for col in ALARMS_COLUMNS] for row in table], schema=ALARMS_COLUMNS, orient="row")
    df = df.drop(["Frame_SN"]).with_columns(
        Cust_Unit=pl.col("Cust_Unit").str.extract(r"(\d{3})$", 1).pipe(lambda x: pl.lit("TK") + x)
    )
    return df
