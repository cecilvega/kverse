import polars as pl
import dagster as pl


def read_csv_events(data: bytes, az_path: str) -> pl.DataFrame:
    """
    Reads OHV style csv data and returns a Polars DataFrame.

    Args:
        data: Raw CSV data as bytes
        az_path: Path to the file, used for extracting metadata

    Returns:
        pl.DataFrame: Processed events data
    """
    # Decode binary data
    data_str = data.decode("latin-1")

    # Step 1: Information from header and filename
    header_info = extract_header_info(data_str, ["equipment_model", "equipment_name"])
    filename_info = extract_filename_info(az_path, ["equipment_serial", "site_name"])

    # Step 2: Find the CSV data section starting at "Type"
    type_idx = data_str.find("Type")
    if type_idx < 2:
        logging.warning(f"Cannot find 'Type' column in file: {az_path}")
        return pl.DataFrame()

    data_str = data_str[type_idx - 2 :]

    # Define column names
    columns = [
        "#",
        "Type",
        "Event #",
        "Sub ID",
        "Name",
        "Sub ID Name",
        "Time",
        "Restriction",
        "Extra",
    ]

    try:
        # Step 3: Read the CSV data
        df = pl.read_csv(StringIO(data_str), separator=",", has_header=False, new_columns=columns, skip_rows=1)

        # Step 4: Process the data
        df = (
            df
            # Filter rows where "Event #" is not null
            .filter(pl.col("Event #").is_not_null())
            # Select and process required columns
            .select(
                [
                    pl.col("Type"),
                    pl.col("Event #"),
                    pl.col("Sub ID"),
                    pl.col("Name"),
                    pl.col("Sub ID Name"),
                    parse_time_column_polars(pl.col("Time")),
                    pl.col("Restriction"),
                    pl.col("Extra"),
                ]
            )
        )

        # Add metadata columns from header_info and filename_info
        for key, value in {**header_info, **filename_info}.items():
            df = df.with_columns(pl.lit(value).alias(key))

        return df

    except Exception as e:
        logging.warning(f"Error {e} reading file: {az_path}")
        return pl.DataFrame()


def parse_time_column_polars(time_series: pl.Expr) -> pl.Expr:
    """
    Polars version of parse_time_column for processing time data

    Args:
        time_series: Time column expression

    Returns:
        pl.Expr: Processed datetime expression
    """
    # Clean special characters and parse datetime
    return (
        time_series.str.replace_all('"', "")
        .str.replace_all("=", "")
        .str.replace_all("'", "")
        .str.strip()
        .str.to_datetime(format="%Y-%m-%d %H:%M:%S.%f", strict=False)
        .alias("Time")
    )
