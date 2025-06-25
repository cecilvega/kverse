import polars as pl
import dagster as dg

TIDY_NAMES = {
    "component_name": "Componente",
    "subcomponent_name": "Subcomponente",
    "equipment_name": "Equipo",
    "position_name": "Posición",
    # "changeout_week": "Semana CC",
    # "changeout_year": "Año CC",
    # "plan_changeout_date": "Fecha Plan CC",
    # "changeout_month": "Mes Plan CC",
    # "status",
    "site_name": "Faena",
    "equipment_model": "Modelo",
    # "last_smr_date": "Última Fecha SMR",
    # "last_smr": "Último SMR",
    "changeout_date": "Fecha CC",
    "equipment_hours": "SMR",
    "component_hours": "Horas CC",
    # "last_changeout_date",
    # "last_changeout_smr",
    "mean_daily_operated_hours": "Horas Operadas Promedio",
    # "plan_operated_smr",
    # "plan_smr",
    # "component_reparation_status",
    # "plan_component_hours",
}


def tidy_tibble(df: pl.DataFrame, context: dg.AssetExecutionContext) -> pl.DataFrame:
    """
    Convert columns with suffix '*date' to pl.Date format and columns with suffix '*dt' to pl.Datetime format.

    Args:
        df: Polars DataFrame to process
        context: Dagster AssetExecutionContext for logging

    Returns:
        Polars DataFrame with converted date/datetime columns
    """
    # Get column names and their types
    columns_info = [(col, df[col].dtype) for col in df.columns]

    # Track converted columns for logging
    converted_date_columns = []
    converted_datetime_columns = []

    # Process each column
    for col_name, col_dtype in columns_info:
        try:
            # Handle columns ending with 'date'
            if col_name.endswith("date"):
                if col_dtype == pl.Utf8:  # String column
                    # Take first 10 characters (YYYY-MM-DD) and convert to Date
                    df = df.with_columns(
                        pl.col(col_name)
                        .str.slice(0, 10)
                        .str.strptime(pl.Date, format="%Y-%m-%d", strict=False)
                        .alias(col_name)
                    )
                    converted_date_columns.append(f"{col_name} (from string)")

                elif col_dtype in [pl.Datetime, pl.Datetime("us"), pl.Datetime("ms"), pl.Datetime("ns")]:
                    # Convert Datetime to Date
                    df = df.with_columns(pl.col(col_name).cast(pl.Date).alias(col_name))
                    converted_date_columns.append(f"{col_name} (from datetime)")

                elif col_dtype != pl.Date:
                    # Already a Date, skip
                    context.log.info(f"Column '{col_name}' is already in Date format")

            # Handle columns ending with 'dt'
            elif col_name.endswith("dt"):
                if col_dtype == pl.Utf8:  # String column
                    # Take first 19 characters (YYYY-MM-DD HH:MM:SS) and convert to Datetime
                    df = df.with_columns(
                        pl.col(col_name)
                        .str.slice(0, 19)
                        .str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False)
                        .alias(col_name)
                    )
                    converted_datetime_columns.append(f"{col_name} (from string)")

                elif col_dtype == pl.Date:
                    # Convert Date to Datetime (midnight)
                    df = df.with_columns(pl.col(col_name).cast(pl.Datetime).alias(col_name))
                    converted_datetime_columns.append(f"{col_name} (from date)")

                elif col_dtype not in [pl.Datetime, pl.Datetime("us"), pl.Datetime("ms"), pl.Datetime("ns")]:
                    # Already a Datetime, skip
                    context.log.info(f"Column '{col_name}' is already in Datetime format")

        except Exception as e:
            context.log.warning(f"Could not convert column '{col_name}': {str(e)}")
            continue

    # Log results
    if converted_date_columns:
        context.log.info(
            f"Converted {len(converted_date_columns)} columns to Date format: {', '.join(converted_date_columns)}"
        )
    else:
        context.log.info("No columns required conversion to Date format")

    if converted_datetime_columns:
        context.log.info(
            f"Converted {len(converted_datetime_columns)} columns to Datetime format: {', '.join(converted_datetime_columns)}"
        )
    else:
        context.log.info("No columns required conversion to Datetime format")

    return df
