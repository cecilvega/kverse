import dagster as dg
from kdags.resources.tidyr import MasterData, DataLake
import polars as pl


@dg.asset(group_name="planning")
def component_serials(component_changeouts: pl.DataFrame):
    """Obtener las series únicas de los componentes principales del pool a monitorear"""
    # Válido para MEL solamente

    components_df = MasterData.components()
    # Filtrar componentes principales
    components_df = components_df.filter(pl.col("subcomponent_main").is_not_null()).select(
        ["component_name", "subcomponent_name"]
    )

    cc_df = (
        component_changeouts.clone()
        .filter(pl.col("equipment_model").str.contains("960E"))
        .join(components_df, how="inner", on=["component_name", "subcomponent_name"])
        .filter(
            pl.col("subcomponent_name").is_in(
                [
                    "transmision",
                    "cilindro_direccion",
                    "cilindro_levante",
                    "suspension_delantera",
                    "suspension_trasera",
                ]
            )
        )
    )

    df = cc_df.select(["component_name", "subcomponent_name", "component_serial", "sap_equipment_name"]).unique()

    return df


@dg.asset(group_name="planning")
def mounted_components(component_changeouts):
    merge_columns = [
        "equipment_name",
        "component_name",
        "subcomponent_name",
        "position_name",
        "changeout_date",
    ]

    # Tomar los componentes montados
    df = (
        component_changeouts.sort(
            merge_columns,
            descending=False,
        )
        .unique(
            subset=[
                "equipment_name",
                "component_name",
                "subcomponent_name",
                "position_name",
            ],
            keep="last",
            maintain_order=True,
        )
        .select([*merge_columns, "installed_component_serial", "installed_sap_equipment_name"])
        .rename(
            {
                "installed_component_serial": "component_serial",
                "installed_sap_equipment_name": "sap_equipment_name",
                "changeout_date": "mounted_date",
            }
        )
        # Remover componentes duplicados quedarse con el último
        .sort(["component_serial", "sap_equipment_name", "mounted_date"])
        .unique(subset=["component_serial", "sap_equipment_name"], keep="last")
        .with_columns(mounted=pl.lit(True))
    )
    return df


@dg.asset(group_name="reliability")
def summarize_reso(so_report: pl.DataFrame):
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
        .with_columns(repair_count=pl.col("component_serial").count().over("component_serial"))
    )
    df = (
        df.sort(["component_serial", "reception_date"]).group_by(
            ["component_serial", "subcomponent_name", "main_component"]
        )
        # TODO:  Es necesario el subcomponent_name? Creo que si pero no debiese, aparentemente no ... Porqué?
        .agg(
            repair_count=pl.len(),
            total_component_hours=pl.sum("component_hours"),
            service_order=pl.last("service_order"),
        )
    )
    merge_columns = ["service_order", "component_serial", "main_component"]
    assert so_report.filter(pl.struct(merge_columns).is_duplicated()).height == 0
    df = df.join(
        so_report.select([*merge_columns, "component_status"]), on=merge_columns, how="left"
    )  # , validate="1:1"
    return df


@dg.asset(group_name="reliability")
def mutate_component_tracking(component_serials, mounted_components, summarize_reso, component_reparations):

    # Agregar componentes montados
    df = component_serials.join(
        mounted_components, on=["component_serial", "sap_equipment_name"], how="left", validate="1:1"
    )
    retired_components = MasterData.retired_components()
    df = df.join(
        summarize_reso,
        how="left",
        on=["component_serial", "subcomponent_name"],
        # validate="1:1",
    )

    # Add retired components
    df = df.join(
        retired_components.drop(["last_changeout_date", "last_service_order"]),
        how="left",
        on=["component_serial", "subcomponent_name"],
        # validate="1:1",
    )

    return df
