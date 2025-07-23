import os
from datetime import date
from datetime import datetime
from pathlib import Path

import dagster as dg
import polars as pl

from kdags.assets.reliability.icc.utils import parse_filename, get_shift_dates, extract_technical_report_data
from kdags.config import DATA_CATALOG
from kdags.resources.tidyr import DataLake, MSGraph
from kdags.resources.tidyr import MasterData


@dg.asset()
def mutate_icc(
    context: dg.AssetExecutionContext,
    component_history: pl.DataFrame,
    component_changeouts: pl.DataFrame,
    component_reparations: pl.DataFrame,
    oil_analysis: pl.DataFrame,
    notifications: pl.DataFrame,
):
    dl = DataLake(context)
    today = date.today()
    taxonomy_df = MasterData.taxonomy().select(["subcomponent_tag", "position_tag", "component_code", "position_code"])
    components_df = (
        MasterData.components()
        .filter(pl.col("subcomponent_main").is_not_null())
        .select(["component_name", "subcomponent_name", "subcomponent_tag"])
    )
    taxonomy_df = taxonomy_df.join(components_df, how="inner", on="subcomponent_tag")
    icc_columns = [
        "equipment_name",
        "component_name",
        "position_name",
        "changeout_date",
    ]
    icc_df = (
        component_changeouts.clone()
        .filter(pl.col("site_name") == "MEL")
        .filter(pl.col("changeout_date") >= datetime(2024, 9, 26))
        # .filter(~pl.col("component_name").is_in(["cilindro_levante", "cilindro_direccion"]))
    )

    agg_df = (
        component_changeouts.group_by(icc_columns)
        .agg(
            [
                pl.col(c)
                for c in [
                    "subcomponent_name",
                    "component_serial",
                    "installed_component_serial",
                ]
            ]
        )
        .with_columns(
            component_serial=pl.struct(
                [
                    pl.col("subcomponent_name"),
                    pl.col("component_serial"),
                    pl.col("installed_component_serial"),
                ]
            ).map_elements(
                lambda x: [
                    {
                        "subcomponent_name": subcomponent_name,
                        "component_serial": component_serial,
                        "installed_component_serial": component_serial,
                    }
                    for subcomponent_name, component_serial, installed_component_serial in zip(
                        x["subcomponent_name"],
                        x["component_serial"],
                        x["installed_component_serial"],
                    )
                ],
                return_dtype=pl.Object,
            )
        )
        .drop(["subcomponent_name", "installed_component_serial"])
    )

    # Get component_name and subcomponent_name from component_code and position_code
    icc_df = (
        icc_df.join(
            taxonomy_df,
            on=["subcomponent_tag", "position_tag"],
            how="inner",
        )
        .with_columns(
            equipment_hours=pl.col("equipment_hours").cast(pl.Float64).round(0).cast(pl.Int64),
            component_hours=pl.col("component_hours").cast(pl.Float64).round(0).fill_null(-1).cast(pl.Int64),
        )
        .select(
            [
                *icc_columns,
                "component_code",
                "position_code",
                "subcomponent_tag",
                "position_tag",
                "equipment_hours",
                "component_hours",
                "component_usage",
                "tbo",
            ]
        )
    ).sort("changeout_date", descending=True)

    icc_df = icc_df.join(
        MasterData.equipments().select(["site_name", "equipment_name", "equipment_model", "equipment_serial"]),
        how="left",
        on="equipment_name",
    )

    merge_columns = ["equipment_name", "subcomponent_tag", "position_tag", "changeout_date"]
    icc_df = icc_df.join(
        component_history.select([*merge_columns, "service_order"]),
        on=merge_columns,
        how="left",
    )

    icc_df = icc_df.with_columns(
        icc_code=pl.concat_str(
            pl.col("equipment_name"),
            pl.lit("_"),
            pl.col("component_code"),
            pl.col("position_code"),
            pl.lit("_"),
            pl.col("changeout_date"),
        )
    )
    files = dl.list_paths("az://bhp-reliability-data/ICC")
    files = files.with_columns(
        icc_code=pl.col("az_path").str.extract(r"ICC/y=\d{4}/m=\d{2}/([^/]+)"),
    )
    files = files.with_columns(
        # Extract ICC code
        icc_code=pl.col("az_path").str.extract(r"ICC/y=\d{4}/m=\d{2}/([^/]+)"),
        # Categorize file type
        file_type=pl.when(pl.col("az_path").str.ends_with("/DATOS.xlsx"))
        .then(pl.lit("DATOS"))
        .when(pl.col("az_path").str.contains("/FOTOS/"))
        .then(pl.lit("FOTOS"))
        .otherwise(pl.lit("OTHER")),
    ).filter(pl.col("az_path").str.contains("(?i).jpg|.jpeg|.png|.xlsx"))
    files_df = files.group_by(["file_type", "icc_code"]).agg(pl.col("az_path"), pl.len())

    assert files_df.filter(pl.col("file_type") == "DATOS").select((pl.col("len") == 1).all()).item()

    data = files_df.filter(pl.col("file_type") == "DATOS").rename({"az_path": "data_path"}).drop("len")
    photos = (
        files_df.filter(pl.col("file_type") == "FOTOS").rename({"az_path": "photos_path"}).drop(["len", "file_type"])
    )
    icc_df = (
        icc_df.join(data, how="left", on="icc_code")
        .join(photos, how="left", on="icc_code")
        .with_columns(
            az_path=pl.concat_str(
                [
                    pl.lit("az://bhp-analytics-data/RELIABILITY/ICC/REPORTS/"),
                    pl.col("equipment_name"),
                    pl.lit("/"),
                    pl.col("component_name").str.to_uppercase(),
                    pl.lit("/ICC_"),
                    pl.col("icc_code"),
                    pl.lit(".pdf"),
                ]
            )
        )
    )

    icc_df = (
        icc_df.join(
            component_changeouts.select(
                [
                    *icc_columns,
                    "subcomponent_name",
                    "component_serial",
                    "installed_component_serial",
                    "customer_work_order",
                ]
            ),
            how="left",
            on=icc_columns,
        )
        .join(
            MasterData.components().select(["subcomponent_name", "subcomponent_label", "subcomponent_main"]),
            how="left",
            on="subcomponent_name",
        )
        .drop_nulls("subcomponent_label")
    )

    # df = (
    #     df.join(get_shift_dates(), how="left", left_on="changeout_date", right_on="date")
    #     .sort("changeout_date", descending=True)
    #     .join(reference_df, on=["equipment_name", "component_name", "position_name", "changeout_date"], how="left")
    # )

    # df = df.with_columns(
    #     icc_url=pl.when(pl.col("changeout_date").is_not_null())
    #     .then(
    #         pl.lit(
    #             "https://globalkomatsu.sharepoint.com/sites/KCHCLSP00022/Shared%20Documents/01.%20%C3%81REAS%20KCH/1.6%20CONFIABILIDAD/"
    #             "CAEX/INFORMES/INFORMES_CAMBIO_DE_COMPONENTE/"
    #         )
    #         + pl.col("equipment_name")
    #         + pl.lit("/")
    #         + pl.col("component_name").str.to_uppercase()
    #         + pl.lit("/")
    #         + pl.col("position_name").str.to_uppercase()
    #         + pl.lit("/ICC ")
    #         + pl.col("equipment_name")
    #         + pl.lit(" ")
    #         + pl.col("component_code").cast(pl.String)
    #         + pl.col("position_code").cast(pl.String)
    #         + pl.lit(" ")
    #         + pl.col("changeout_date").dt.to_string(format="%Y-%m-%d")
    #         + pl.lit(".pdf")
    #     )
    #     .otherwise(pl.lit("ICC EXTRAÑO"))
    #     .str.replace_all(" ", "%20")
    # ).drop("file_path")
    # df = df.filter(~(pl.col("component_name")).is_in(["cilindro_levante", "cilindro_direccion"]))
    # df = df.select(
    #     [
    #         "icc_week",
    #         "work_shift",
    #         # Equipment identification
    #         "equipment_name",
    #         # Component information
    #         "component_name",
    #         "component_code",
    #         "position_name",
    #         "position_code",
    #         # Dates and events
    #         "report_date",
    #         "changeout_date",
    #         # Work order information
    #         # "customer_work_order",
    #         # "icc_number",
    #         # File metadata
    #         "icc_file_name",
    #         # Additional details
    #         # "failure_description",
    #     ]
    # )

    dl.upload_tibble(tibble=icc_df, az_path=DATA_CATALOG["icc"]["analytics_path"])

    return icc_df
