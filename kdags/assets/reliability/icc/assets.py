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


@dg.asset
def gather_icc(context: dg.AssetExecutionContext):
    # Find all PDF and DOCX files
    icc_files = [
        f
        for f in (Path(os.environ["ONEDRIVE_LOCAL_PATH"]) / "INFORMES_CAMBIO_DE_COMPONENTE").rglob("*")
        if ((f.is_file()) & (f.suffix.lower() in [".pdf"]) & (f.stem.lower().startswith("icc")))
    ]

    data = []
    for file_path in icc_files:
        try:
            i_data = {}

            # Only extract report data if it's a PDF file
            if file_path.suffix.lower() == ".pdf":
                i_data = extract_technical_report_data(file_path)

            # Parse filename for both PDF and DOCX files
            i_data = {**i_data, **parse_filename(file_path)}

            # Add file type to the data
            i_data["file_type"] = file_path.suffix.lower().replace(".", "")

            data.append(i_data)
        except Exception as e:
            context.log.error(file_path)
            raise e

    # Create Polars DataFrame from list of dictionaries
    df = (
        pl.from_dicts(data)
        .rename({"equipment_hours": "icc_equipment_hours"})
        .select(
            [
                "equipment_name",
                "icc_equipment_hours",
                "report_date",
                "changeout_date",
                "icc_file_name",
                "icc_number",
                "component_code",
                "position_code",
                "file_path",
            ]
        )
        .with_columns(changeout_date=pl.col("changeout_date").cast(pl.Date))
    )

    return df


def main_component_changeouts(component_changeouts: pl.DataFrame):
    taxonomy_df = MasterData.taxonomy().select(["subcomponent_tag", "position_tag", "component_code", "position_code"])
    components_df = (
        MasterData.components()
        .filter(pl.col("subcomponent_main").is_not_null())
        .select(["component_name", "subcomponent_name", "subcomponent_tag"])
    )
    taxonomy_df = taxonomy_df.join(components_df, how="inner", on="subcomponent_tag")
    df = (
        component_changeouts.clone()
        .filter(pl.col("site_name") == "MEL")
        .filter(pl.col("changeout_date") >= datetime(2024, 9, 26))
    )

    agg_df = (
        component_changeouts.group_by(
            [
                "equipment_name",
                "component_name",
                "position_name",
                "changeout_date",
            ]
        )
        .agg(
            subcomponent_name=pl.col("subcomponent_name"),
            component_serial=pl.col("component_serial").alias("component_serial"),
            installed_component_serial=pl.col("installed_component_serial"),
        )
        .with_columns(
            component_serial=pl.struct([pl.col("subcomponent_name"), pl.col("component_serial")]).map_elements(
                lambda x: {
                    name: comp_serial
                    for name, comp_serial in zip(
                        x["subcomponent_name"],
                        x["component_serial"],
                    )
                }.__str__()
            ),
            installed_component_serial=pl.struct(
                [pl.col("subcomponent_name"), pl.col("installed_component_serial")]
            ).map_elements(
                lambda x: {
                    name: comp_serial
                    for name, comp_serial in zip(
                        x["subcomponent_name"],
                        x["installed_component_serial"],
                    )
                }.__str__()
            ),
        )
        .drop(["subcomponent_name"])
    )

    # Get component_name and subcomponent_name from component_code and position_code
    df = (
        df.join(
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
                "equipment_name",
                "component_name",
                "position_name",
                "component_code",
                "position_code",
                "changeout_date",
                "equipment_hours",
                "component_hours",
            ]
        )
        .join(
            agg_df,
            how="left",
            on=[
                "equipment_name",
                "component_name",
                "position_name",
                "changeout_date",
            ],
        )
    )

    return df


@dg.asset()
def mutate_icc(context: dg.AssetExecutionContext, component_changeouts, gather_icc):
    datalake = DataLake(context)
    msgraph = MSGraph(context)
    reference_df = msgraph.read_tibble(DATA_CATALOG["ep"]["reference_path"])
    reference_df = reference_df.select(
        [
            "equipment_name",
            "component_name",
            "position_name",
            "changeout_date",
            "component_photos",
            "icc_ready",
            "failure_effect",
        ]
    )

    cc_summary = main_component_changeouts(component_changeouts)
    merge_columns = [
        "equipment_name",
        "component_code",
        "position_code",
        "changeout_date",
    ]
    df = cc_summary.join(
        gather_icc.select([*merge_columns, "report_date", "icc_equipment_hours", "icc_file_name", "file_path"]),
        on=merge_columns,
        how="outer",
        coalesce=True,
    )

    df = (
        df.join(get_shift_dates(), how="left", left_on="changeout_date", right_on="date")
        .sort("changeout_date", descending=True)
        .join(reference_df, on=["equipment_name", "component_name", "position_name", "changeout_date"], how="left")
    )

    df = df.with_columns(
        icc_url=pl.when(pl.col("changeout_date").is_not_null())
        .then(
            pl.lit(
                "https://globalkomatsu.sharepoint.com/sites/KCHCLSP00022/Shared%20Documents/01.%20%C3%81REAS%20KCH/1.6%20CONFIABILIDAD/"
                "CAEX/INFORMES/INFORMES_CAMBIO_DE_COMPONENTE/"
            )
            + pl.col("equipment_name")
            + pl.lit("/")
            + pl.col("component_name").str.to_uppercase()
            + pl.lit("/")
            + pl.col("position_name").str.to_uppercase()
            + pl.lit("/ICC ")
            + pl.col("equipment_name")
            + pl.lit(" ")
            + pl.col("component_code").cast(pl.String)
            + pl.col("position_code").cast(pl.String)
            + pl.lit(" ")
            + pl.col("changeout_date").dt.to_string(format="%Y-%m-%d")
            + pl.lit(".pdf")
        )
        .otherwise(pl.lit("ICC EXTRAÑO"))
        .str.replace_all(" ", "%20")
    ).drop("file_path")
    df = df.filter(~(pl.col("component_name")).is_in(["cilindro_levante", "cilindro_direccion"]))
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

    datalake.upload_tibble(tibble=df, az_path=DATA_CATALOG["icc"]["analytics_path"])

    return df
