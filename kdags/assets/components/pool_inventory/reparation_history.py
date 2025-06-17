import dagster as dg
from kdags.resources.tidyr import MasterData, DataLake
import polars as pl


@dg.asset(group_name="reliability")
def mutate_reparation_history(so_report: pl.DataFrame):
    df = (
        so_report.filter(pl.col("site_name") == "MINERA ESCONDIDA")
        .filter(pl.col("equipment_model").str.contains("960E"))
        # TODO: Evaluar que realizar con las garantías de fábrica
        # Sacar las garantías de fábrica porque duplican el contador por mientras, evaluar que hacer posteriormente
        # un mejor algoritmo puede ser deduplicar
        .filter(pl.col("warranty_type") != "Factory Warranty")
        .join(
            pl.DataFrame(
                [
                    {
                        "main_component": "WHEEL TRANSMISSION",
                        "subcomponent_name": "transmision",
                        "component": [
                            "MOTOR MECHANIC 960E-2",
                            "MOTOR MECHANIC 960E-1",
                            "WHEEL TRANSMISSION 930E-4",
                            "WHEEL TRANSMISSION 930E",
                            "WHEEL TRANSMISSION 930E-4SE",
                        ],
                    },
                    {
                        "main_component": "STEERING CYLINDER",
                        "subcomponent_name": "cilindro_direccion",
                        "component": [
                            "STEERING CYLINDER 960E-2",
                            "STEERING CYLINDER 960E-1",
                            "STEERING CYLINDER 930E",
                            "STEERING CYLINDER 930E-4",
                            "STEERING CYLINDER 980E-5",
                            "STEERING CYLINDER 930E-4SE",
                            "STEERING CYLINDER 980E-4",
                        ],
                    },
                    {
                        "main_component": "HOIST CYLINDER",
                        "subcomponent_name": "cilindro_levante",
                        "component": [
                            "HOIST CYLINDER 960E-1",
                            "HOIST CYLINDER 930E",
                            "HOIST CYLINDER 930E-4",
                            "HOIST CYLINDER 930E-4SE",
                            "HOIST CYLINDER 930E-5",
                        ],
                    },
                    {
                        "main_component": "REAR SUSPENSION",
                        "subcomponent_name": "suspension_trasera",
                        "component": [
                            "REAR SUSPENSION 960E-2",
                            "REAR SUSPENSION 960E-1",
                            "REAR SUSPENSION 930E",
                            "REAR SUSPENSION 930E-4",
                        ],
                    },
                    {
                        "main_component": "FRONT SUSPENSION",
                        "subcomponent_name": "suspension_delantera",
                        "component": [
                            "FRONT SUSPENSION 960E-2",
                            "FRONT SUSPENSION 930E",
                            "FRONT SUSPENSION 960E-1",
                            "FRONT SUSPENSION 930E-4",
                            "FRONT SUSPENSION 960E",
                            "FRONT SUSPENSION 930E-4SE",
                        ],
                    },
                ]
            ).explode("component"),
            how="inner",
            on="component",
        )
    )
    # Define non-numeric values to replace with null
    non_numeric_values = [
        "REPARADO",
        "SIN INFORMACIÓN",
        "Sin información",
        "SIN INFORMACION.",
        "S/I",
        "Sin Información",
        "No aplica",
        "NUEVO",
        "NUEVA",
        "PRUEBAS",
        "NO INFORMADO",
    ]
    invalid_component_serials = [
        "S/I",
        "",
        ".",
        "SIN",
        "N/S",
        "NS",
        "AF",
        "SN",
        "1",
        "Sin",
        "KRCC-1",
        "PENDIENTE",
        "NO",
        "SI",
        "N/A",
        "0",
    ]
    df = (
        df.with_columns(
            [
                pl.col(c).str.replace_all(r"\s+", " ").str.strip_chars()
                for c in ["main_component", "component_serial", "component"]
            ]
        )
        .filter(~pl.col("component_serial").is_in(invalid_component_serials))
        .with_columns(
            component_hours=pl.col("component_hours")
            # First, handle null/non-numeric values
            .str.strip_chars()  # Remove leading/trailing spaces
            # Replace known non-numeric values with empty string
            .str.replace_all(f"^({'|'.join(non_numeric_values)})$", "")
            # Remove text suffixes and special characters
            .str.replace_all(
                r"\s*(hrs|Hrs|hrs aprox|/ PRUEBAS).*$", ""
            ).str.replace_all(  # Remove hour indicators and anything after
                r"[()]", ""
            )  # Remove parentheses
            # Handle ranges (take first number)
            .str.replace_all(r"^(\d+)-\d+$", "$1")
            # Handle thousand separators
            # First, identify and fix periods used as thousand separators (3+ digits after period)
            .str.replace_all(r"\.(\d{3,})", "$1")  # Remove period when 3+ digits follow
            # Handle mixed formats with both comma and period
            # If we have format like "1.234,56", convert to "1234.56"
            .str.replace_all(r"(\d+)\.(\d{3}),(\d+)", "$1$2.$3")  # European format with comma as decimal
            # Remove remaining commas (thousand separators)
            .str.replace_all(",", "")
            # Handle special case of multiple zeros
            .str.replace_all(r"^0+$", "0")
            # Convert empty strings to null by checking length
            .str.replace_all(r"^$", "NULL_PLACEHOLDER")  # Temporary placeholder
            # Convert to float (empty strings and invalid values will become null)
            .cast(pl.Float64, strict=False)
        )
        .filter(pl.col("component_hours") != 0)
        .with_columns(repair_count=pl.col("component_serial").count().over("component_serial"))
    )
    df = df.sort(["component_serial", "reception_date"]).with_columns(
        [
            # Repair count (cumulative/increasing)
            pl.int_range(pl.len())
            .over(["component_serial", "subcomponent_name", "main_component"])
            .alias("repair_count")
            + 1,
            # Repair recency rank (inverse of repair count - most recent = 1)
            pl.int_range(pl.len())
            .reverse()
            .over(["component_serial", "subcomponent_name", "main_component"])
            .alias("repair_recency_rank"),
            # Cumulative component hours
            pl.col("component_hours")
            .cum_sum()
            .over(["component_serial", "subcomponent_name", "main_component"])
            .alias("cumulative_component_hours"),
        ]
    )
    # merge_columns = ["service_order", "component_serial", "main_component"]
    # assert so_report.filter(pl.struct(merge_columns).is_duplicated()).height == 0
    # df = df.join(
    #     so_report.select([*merge_columns, "component_status"]), on=merge_columns, how="left"
    # )  # , validate="1:1"
    return df
