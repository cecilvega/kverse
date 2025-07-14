import dagster as dg
import polars as pl

from kdags.config import DATA_CATALOG, TIDY_NAMES, tidy_tibble
from kdags.resources.tidyr import DataLake, MasterData, MSGraph


@dg.asset(compute_kind="mutate")
def mutate_component_fleet(
    context: dg.AssetExecutionContext,
    component_changeouts: pl.DataFrame,
    component_history: pl.DataFrame,
    component_reparations: pl.DataFrame,
    smr: pl.DataFrame,
    so_report: pl.DataFrame,
) -> pl.DataFrame:
    dl = DataLake(context)
    smr_df = smr.select(["equipment_name", "smr_date", "smr"]).sort(["equipment_name", "smr_date"])
    equipments_df = (
        MasterData.equipments()
        .filter(pl.col("equipment_model").is_in(["960E"]))
        .select(["site_name", "equipment_name", "equipment_model"])
    )
    components_df = (
        MasterData.components().filter(pl.col("subcomponent_main")).select(["component_name", "subcomponent_name"])
    )
    taxonomy_df = (
        MasterData.taxonomy()
        .join(
            components_df,
            how="inner",
            on=["component_name", "subcomponent_name"],
        )
        .select(["subcomponent_tag", "position_tag"])
    ).filter(pl.col("subcomponent_tag").is_in(["0980"]))

    subc_columns = ["equipment_name", "subcomponent_tag", "position_tag"]

    mounted_df = (
        component_changeouts.join(
            components_df,
            how="inner",
            on=["component_name", "subcomponent_name"],
        )
        .select(
            [
                "site_name",
                "equipment_model",
                *subc_columns,
                "changeout_date",
                "installed_component_serial",
            ]
        )
        .rename(
            {
                "installed_component_serial": "component_serial",
                "changeout_date": "installed_date",
            }
        )
        .sort([*subc_columns, "installed_date"])
        .unique(subset=subc_columns, keep="last")
    )

    df = equipments_df.join(taxonomy_df, how="cross")

    df = df.join(mounted_df, on=["site_name", "equipment_model", *subc_columns], how="left").sort(
        ["site_name", *subc_columns, "installed_date"]
    )

    # Agregar último horómetro equipo y a la fecha de montaje
    # Calcular horas operadas del equipo
    df = (
        df.join(
            smr_df,
            left_on=["equipment_name", "installed_date"],
            right_on=["equipment_name", "smr_date"],
            how="left",
        )
        .rename({"smr": "installed_smr"})
        .join(
            smr_df.unique(subset=["equipment_name"], keep="last").drop("smr_date").rename({"smr": "current_smr"}),
            how="left",
            on="equipment_name",
        )
        .with_columns(runtime_hours=pl.col("current_smr") - pl.col("installed_smr"))
        # .with_columns(
        #     runtime_hours=pl.when(pl.col("runtime_hours").is_null())
        #     .then(pl.col("current_smr"))
        #     .otherwise(pl.col("runtime_hours"))
        # )
    )

    # Agregar información de cambio de componente anterior utiizando el historial componentes
    ch_df = (
        component_history.select(
            [
                "equipment_name",
                "subcomponent_tag",
                "position_tag",
                "component_serial",
                "changeout_date",
                "service_order",
                # "equipment_hours",
            ]
        )
        .sort(["subcomponent_tag", "component_serial", "changeout_date"])
        .unique(subset=["subcomponent_tag", "component_serial"], keep="last")
        .rename(
            {
                "equipment_name": "previous_equipment_name",
                "position_tag": "previous_position_tag",
                # "equipment_hours": "previous_smr",
            }
        )
    )

    df = df.join(ch_df, on=["subcomponent_tag", "component_serial"], how="left")

    df = df.drop(["site_name", "equipment_model"]).sort(["subcomponent_tag", "equipment_name", "position_tag"])

    # Agregarle contadores de reparación
    merge_columns = ["service_order", "component_serial"]
    df = df.join(
        component_reparations.filter(pl.col("repair_recency_rank") == 0).select(
            [
                *merge_columns,
                "component_hours",
                "reception_date",
                "repair_count",
                "repair_recency_rank",
                "cumulative_component_hours",
            ]
        ),
        how="left",
        on="component_serial",
    )
    df = df.with_columns(total_component_hours=pl.col("cumulative_component_hours") + pl.col("runtime_hours"))

    # repair_df = under_repair_transmisions(df, component_history)
    # df = pl.concat([df, repair_df], how="diagonal")

    # df = df.join(so_report.select(["service_order", "component_status"]), how="left", on="service_order")

    dl.upload_tibble(df, DATA_CATALOG["component_fleet"]["analytics_path"])

    return df


@dg.asset
def under_repair_transmisions(
    context: dg.AssetExecutionContext, mutate_component_fleet: pl.DataFrame, component_history: pl.DataFrame
) -> pl.DataFrame:
    df = MasterData.component_serials().join(mutate_component_fleet, how="anti", on="component_serial")
    merge_columns = ["subcomponent_tag", "component_serial"]
    df = df.join(
        component_history.filter(pl.col("subcomponent_tag") == "0980")
        .sort([*merge_columns, "changeout_date"])
        .unique(merge_columns, keep="last")
        .select([*merge_columns, "reception_date", "service_order"]),
        how="left",
        on="component_serial",
        validate="1:1",
    )
    return df
