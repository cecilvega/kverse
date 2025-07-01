import dagster as dg
import polars as pl

from kdags.config import DATA_CATALOG, TIDY_NAMES, tidy_tibble
from kdags.resources.tidyr import DataLake, MasterData, MSGraph


def pivot_part_metrics(enriched_df):

    merge_columns = ["service_order", "component_serial"]
    # Step 1: Filter for most recent records only (recency rank = 1)
    latest_records = enriched_df.filter(pl.col("part_repair_recency_rank") == 0)

    # Step 2: Get component-level info (no document_type or extraction_status)
    component_info = latest_records.group_by(merge_columns).agg(
        [
            pl.col("reception_date").max().alias("reception_date"),
        ]
    )

    # Step 3: Get all unique part names to control column ordering
    part_names = sorted(latest_records["part_name"].unique().to_list())

    # Step 4: Pivot each metric separately and join to component_info
    final_result = component_info

    for part_name in part_names:
        part_data = latest_records.filter(pl.col("part_name") == part_name)

        # Part serial
        part_serial_df = (
            part_data.pivot(
                values="part_serial",
                index=merge_columns,
                on="part_name",
                aggregate_function="first",
            )
            .select([*merge_columns, part_name])
            .rename({part_name: part_name})
        )

        # # Repair count
        part_count_df = (
            part_data.pivot(
                values="part_repair_count",
                index=merge_columns,
                on="part_name",
                aggregate_function="first",
            )
            .select([*merge_columns, part_name])
            .rename({part_name: f"{part_name}_repair_count"})
        )

        # Cumulative hours
        part_hours_df = (
            part_data.pivot(
                values="cumulative_part_hours",
                index=merge_columns,
                on="part_name",
                aggregate_function="first",
            )
            .select([*merge_columns, part_name])
            .rename({part_name: f"{part_name}_cumulative_part_hours"})
        )

        # Join all metrics for this part
        final_result = (
            final_result.join(part_serial_df, on=merge_columns, how="left")
            .join(part_hours_df, on=merge_columns, how="left")
            .join(part_count_df, on=merge_columns, how="left")
        )

    return final_result


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

    parts_df = mutate_part_reparations.pipe(pivot_part_metrics)
    df = df.join(parts_df, on=["service_order", "component_serial"], how="left")

    df = df.drop(["reception_date_right", "repair_recency_rank", "Fecha CC"], strict=False).rename(
        {
            "runtime_hours": "Horas Operadas Comp.",
            "repair_count": "Cantidad Reparaciones",
            "total_component_hours": "Horas Totales Comp.",
        }
    )

    dl.upload_tibble(df, DATA_CATALOG["component_fleet"]["analytics_path"])

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
