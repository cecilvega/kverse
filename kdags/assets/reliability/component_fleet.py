import dagster as dg
import polars as pl

from kdags.config import DATA_CATALOG, TIDY_NAMES, tidy_tibble
from kdags.resources.tidyr import DataLake, MasterData, MSGraph


def add_status_column(
    df: pl.DataFrame,
    value_column: str = "total_part_hours",
    condemnatory_column: str = "part_lc_hours",
    precautionary_column: str = "part_lp_hours",
    status_column: str = "part_status",
) -> pl.DataFrame:
    """
    Add a status column to a Polars DataFrame based on condemnatory limits.

    Parameters:
    -----------
    df : pl.DataFrame
        Input DataFrame
    value_column : str
        Column name containing the values to analyze (default: "total_part_hours")
    condemnatory_column : str
        Column name containing the condemnatory limits (default: "part_lc_hours")
    precautionary_column : str
        Column name containing the precautionary limits (default: "part_lp_hours")
    status_column : str
        Name for the new status column (default: "status")

    Returns:
    --------
    pl.DataFrame
        DataFrame with added status column
    """

    return df.with_columns(
        [
            pl.when(pl.col(value_column) >= pl.col(condemnatory_column))
            .then(pl.lit("CRITICO"))
            .when(pl.col(value_column) >= pl.col(precautionary_column))
            .then(pl.lit("PRECAUTORIO"))
            .otherwise(pl.lit("NORMAL"))
            .alias(status_column)
        ]
    )


@dg.asset(compute_kind="mutate")
def mutate_component_fleet(
    context: dg.AssetExecutionContext,
    component_changeouts: pl.DataFrame,
    component_history: pl.DataFrame,
    component_reparations: pl.DataFrame,
    mutate_part_reparations: pl.DataFrame,
) -> pl.DataFrame:
    dl = DataLake(context)
    smr_df = (
        dl.read_tibble("az://bhp-analytics-data/smr.parquet")
        .select(["equipment_name", "smr_date", "smr"])
        .sort(["equipment_name", "smr_date"])
    )
    equipments_df = (
        MasterData.equipments()
        .filter(pl.col("equipment_model").is_in(["930E-4", "960E", "980E-5"]))
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
    )
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

    df = equipments_df.join(
        taxonomy_df,
        how="cross",
    )

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
        .with_columns(
            runtime_hours=pl.when(pl.col("runtime_hours").is_null())
            .then(pl.col("current_smr"))
            .otherwise(pl.col("runtime_hours"))
        )
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

    df = (
        df.filter((pl.col("equipment_model") == "960E") & (pl.col("subcomponent_tag").is_in(["0980", "5A30"])))
        .drop(["site_name", "equipment_model"])
        .sort(["subcomponent_tag", "equipment_name", "position_tag"])
    )
    # df = df.join(
    #     MasterData.components().select(["subcomponent_tag", "component_name", "subcomponent_name"]),
    #     how="left",
    #     on="subcomponent_tag",
    # )

    # Agregarle contadores de reparación
    merge_columns = ["service_order", "component_serial"]
    df = df.join(
        component_reparations.filter(pl.col("repair_recency_rank") == 0).select(
            [
                *merge_columns,
                "reception_date",
                "repair_count",
                "repair_recency_rank",
                "cumulative_component_hours",
            ]
        ),
        how="left",
        on=merge_columns,
    )
    df = df.with_columns(
        total_component_hours=pl.col("cumulative_component_hours")
        + pl.col("runtime_hours")
        # + pl.when(pl.col("runtime_hours") > 0).then(pl.col("runtime_hours")).otherwise(pl.col("runtime_hours"))
    )

    # Agregar lo de las partes

    # parts_df = mutate_part_reparations.pipe(pivot_part_metrics)
    # df = df.join(parts_df, on=["service_order", "component_serial"], how="left")
    #
    # df = df.drop(["reception_date_right", "repair_recency_rank", "Fecha CC"], strict=False).rename(
    #     {
    #         "runtime_hours": "Horas Operadas Comp.",
    #         "repair_count": "Cantidad Reparaciones",
    #         "total_component_hours": "Horas Totales Comp.",
    #     }
    # )

    dl.upload_tibble(df, DATA_CATALOG["component_fleet"]["analytics_path"])

    return df


@dg.asset
def mutate_parts_fleet(context: dg.AssetExecutionContext, mutate_component_fleet, parts_base, mutate_part_reparations):
    dl = DataLake(context)
    df = (
        mutate_part_reparations.filter(pl.col("part_repair_recency_rank") == 0)
        .drop_nulls(["part_lifecycle_hours"])
        .filter(pl.col("part_lifecycle_hours") != 0)
        .drop(
            [
                "final_part_serial",
                "initial_part_serial",
                "num_components_visited",
                "part_name_right",
                "subpart_name_right",
                "part_repair_recency_rank",
            ]
        )
    )
    # Unirlo a la base para cuantificar nulos

    merge_columns = ["subcomponent_tag", "component_serial", "service_order"]
    partial_cr_df = mutate_component_fleet.select(
        [
            *merge_columns,
            "equipment_name",
            "total_component_hours",
            "repair_count",
            "runtime_hours",
        ]
    ).join(
        parts_base.select(["subcomponent_tag", "part_name", "subpart_name"])
        .unique()
        .sort(["subcomponent_tag", "part_name", "subpart_name"]),
        on="subcomponent_tag",
        how="full",
        coalesce=True,
    )
    df = (
        partial_cr_df.join(
            df,
            on=[*merge_columns, "part_name", "subpart_name"],
            how="left",
        )
        .drop(["part_lifecycle_hours", "part_lifecycle_repairs", "total_movements"])
        .rename(
            {
                "total_lifecycle_hours": "part_lifecycle_hours",
                "total_repairs": "part_repairs",
            }
        )
    )

    df = df.drop(
        [
            "component_hours",
            "reception_date",
            "part_movement_history",
            "repair_count",
            "part_movement_count",
        ]
    )
    df = df.with_columns(total_part_hours=pl.col("runtime_hours") + pl.col("part_lifecycle_hours")).with_columns(
        censoring=pl.when((pl.col("total_part_hours").is_null()) | (pl.col("total_part_hours") > 80000))
        .then(pl.lit(True))
        .otherwise(pl.lit(False))
    )
    df = df.join(
        pl.DataFrame(
            [
                {
                    "part_name": "low_speed_gear",
                    "part_lp_hours": 21785,
                    "part_lc_hours": 47576,
                },
                {
                    "part_name": "sun_pinion",
                    "part_lp_hours": 20000,
                    "part_lc_hours": 48214,
                },
                {
                    "part_name": "coupling_plate",
                    "part_lp_hours": 24514,
                    "part_lc_hours": 50867,
                },
                {
                    "part_name": "driveshaft",
                    "part_lp_hours": 20000,
                    "part_lc_hours": 45723,
                },
                {
                    "part_name": "ring_gear",
                    "part_lp_hours": 20000,
                    "part_lc_hours": 52000,
                },
                {
                    "part_name": "high_speed_gear",
                    "part_lp_hours": 20000,
                    "part_lc_hours": 51333,
                },
            ]
        ),
        how="left",
        on=["part_name"],
    )

    df = df.pipe(add_status_column)

    dl.upload_tibble(df, DATA_CATALOG["parts_fleet"]["analytics_path"])
    return df


@dg.asset(compute_kind="publish")
def publish_component_fleet(context: dg.AssetExecutionContext, mutate_component_fleet: pl.DataFrame):
    msgraph = MSGraph(context)

    df = mutate_component_fleet.clone()
    df = df.rename(TIDY_NAMES, strict=False).pipe(tidy_tibble, context)
    msgraph.upload_tibble(
        tibble=df,
        sp_path=DATA_CATALOG["component_fleet"]["publish_path"],
    )
    return df


@dg.asset(compute_kind="publish")
def publish_parts_fleet(context: dg.AssetExecutionContext, mutate_parts_fleet: pl.DataFrame):
    msgraph = MSGraph(context)

    df = mutate_parts_fleet.clone()
    df = df.rename(TIDY_NAMES, strict=False).pipe(tidy_tibble, context)
    msgraph.upload_tibble(
        tibble=df,
        sp_path=DATA_CATALOG["parts_fleet"]["publish_path"],
    )
    return df
