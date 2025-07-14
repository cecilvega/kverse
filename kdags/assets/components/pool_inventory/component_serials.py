import dagster as dg
from kdags.resources.tidyr import MasterData, DataLake
import polars as pl


@dg.asset()
def component_serials(component_changeouts: pl.DataFrame):
    """Obtener las series únicas de los componentes principales del pool a monitorear"""
    # Válido para MEL solamente
    retired_components = (
        MasterData.component_serials()
        .filter(pl.col("is_retired"))
        .with_columns(retired_date=pl.col("retired_date").str.to_date())
    )
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

    columns = ["component_name", "subcomponent_name", "component_serial", "sap_equipment_name"]
    df = pl.concat(
        [
            cc_df.select(columns),
            cc_df.drop(["component_serial", "sap_equipment_name"])
            .rename(
                {"installed_component_serial": "component_serial", "installed_sap_equipment_name": "sap_equipment_name"}
            )
            .select(columns),
        ]
    ).unique()

    # Add retired components
    df = df.join(
        retired_components.drop(["last_service_order"]),
        how="left",
        on=["component_serial", "subcomponent_name"],
        # validate="1:1",
    )

    df = df.filter(pl.col("subcomponent_name") == "transmision")

    return df
