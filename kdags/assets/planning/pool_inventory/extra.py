import dagster as dg
from kdags.resources.tidyr import MasterData, DataLake
import polars as pl


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


# @dg.asset(group_name="reliability")
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
