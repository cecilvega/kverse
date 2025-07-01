import dagster as dg
from kdags.config import DATA_CATALOG

# --- Relative module imports
from kdags.resources.tidyr import DataLake
import polars as pl
from ..extract_utils import *
from ..extra_utils import get_documents, clean_part_serial


@dg.asset()
def preliminary_report_mt_docs(
    context: dg.AssetExecutionContext, component_reparations: pl.DataFrame, so_documents: pl.DataFrame
):
    reports = get_documents(
        component_reparations=component_reparations,
        so_documents=so_documents,
        subcomponent_tag="0980",
        file_type="preliminary_report",
    )
    # reports = [
    #     {
    #         "service_order": 14151781,
    #         "component_serial": "WX14100048T",
    #         "az_path": "az://bhp-raw-data/RESO/DOCUMENTS/y=2025/m=01/d=16/14151781/PRELIMINARY_REPORT/14151781_311735_INI_APRO.pdf",
    #     },
    #     {
    #         "service_order": 14146426,
    #         "component_serial": "WX14020007T",
    #         "az_path": "az://bhp-raw-data/RESO/DOCUMENTS/y=2024/m=07/d=10/14146426/PRELIMINARY_REPORT/14146426_301207_INI_APRO.pdf",
    #     },
    #     *reports[0:1],
    # ]

    high_speed_gear_df = extract_tibble_from_report(
        context,
        reports,
        table_title="control dimensional de engranajes de carrier de alta velocidad",
        table_columns=["número de serie"],
    ).drop(["pages", "section_number"])

    # Extract baja table
    low_speed_gear_df = extract_tibble_from_report(
        context,
        reports,
        table_title="control dimensional de engranajes de carrier de baja velocidad",
        table_columns=["número de serie"],
    ).drop(["pages", "section_number"])

    # Extract sub-components table
    subcomp_df = extract_tibble_from_report(
        context,
        reports,
        table_title="numero de serie de sub-componentes",
        table_columns=["número de serie"],
    ).drop(["pages", "section_number"])

    # Process alta gears - translate to English
    high_speed_gear_df = high_speed_gear_df.with_columns(
        pl.col("index")
        .map_elements(lambda x: f"high_speed_gear_{x.lower().replace('engranaje ', '').replace(' ', '_')}")
        .alias("part_name")
    ).rename(
        {"Número de serie": "part_serial"}
    )  # Fixed: Capital N

    # Process baja gears - translate to English
    low_speed_gear_df = low_speed_gear_df.with_columns(
        pl.col("index")
        .map_elements(
            lambda x: f"low_speed_gear_{x.lower().replace('engranaje ', '').replace(' hr.', 'hr').replace(' ', '_')}"
        )
        .alias("part_name")
    ).rename(
        {"Número de serie": "part_serial"}
    )  # Fixed: Capital N

    translation_map = {
        "coupling plate": "coupling_plate",
        "ring gear": "ring_gear",
        "piñon solar": "sun_pinion",
        "eje palier": "driveshaft",
    }

    # Filter only the components we want and translate
    subcomp_df = (
        subcomp_df.filter(
            pl.col("index").str.to_lowercase().is_in(["coupling plate", "ring gear", "piñon solar", "eje palier"])
        )
        .with_columns(
            [
                pl.col("index")
                .map_elements(lambda x: translation_map.get(x.lower(), x.lower().replace(" ", "_")))
                .alias("part_name"),
                pl.col("Numero de Serie").str.to_lowercase().alias("part_serial"),  # This one was already correct
            ]
        )
        .select(["service_order", "component_serial", "extraction_status", "part_name", "part_serial"])
    )

    # Standardize column names for all dataframes
    high_speed_gear_df = high_speed_gear_df.select(
        ["service_order", "component_serial", "extraction_status", "part_name", "part_serial"]
    )
    low_speed_gear_df = low_speed_gear_df.select(
        ["service_order", "component_serial", "extraction_status", "part_name", "part_serial"]
    )

    # Combine all dataframes
    df = (
        pl.concat([high_speed_gear_df, low_speed_gear_df, subcomp_df])
        .pipe(clean_part_serial)
        .with_columns(document_type=pl.lit("preliminary_report"))
    )

    # # Pivot to get final structure
    # df = combined_df.pivot(
    #     values="serial_number",
    #     index=["service_order", "component_serial", "extraction_status"],
    #     on="component_type",
    #     aggregate_function="first",  # Take the first value if duplicates exist
    # )

    return df


@dg.asset()
def final_report_mt_docs(
    context: dg.AssetExecutionContext, component_reparations: pl.DataFrame, so_documents: pl.DataFrame
):
    reports = get_documents(
        component_reparations=component_reparations,
        so_documents=so_documents,
        subcomponent_tag="0980",
        file_type="final_report",  # Changed to final_report
    )

    # reports = [
    #     {
    #         "service_order": 14146426,
    #         "component_serial": "WX14020007T",
    #         "az_path": "az://bhp-raw-data/RESO/DOCUMENTS/y=2024/m=07/d=10/14146426/FINAL_REPORT/14146426_301207_FIN_APRO.pdf",
    #     },
    #     *reports[0:1],
    # ]

    # Extract planetary gears table
    planetary_gears_df = extract_tibble_from_report(
        context,
        reports,
        table_title="numero de series de planetarios",
        table_columns=["numero de serie"],
    ).drop(["pages", "section_number"])

    # Extract sub-components table
    subcomp_df = extract_tibble_from_report(
        context,
        reports,
        table_title="numero de serie de sub-componentes",
        table_columns=["numero de serie"],
    ).drop(["pages", "section_number"])

    # Process planetary gears - separate high speed (A,B,C) and low speed (Hr gears)
    # High speed gears (A, B, C)
    high_speed_planetary_df = (
        planetary_gears_df.filter(pl.col("index").str.contains("Engranaje [ABC]"))
        .with_columns(
            pl.col("index")
            .map_elements(lambda x: f"high_speed_gear_{x.lower().replace('engranaje ', '').replace(' ', '_')}")
            .alias("part_name")
        )
        .rename({"Numero de Serie": "part_serial"})
    )

    # Low speed gears (Hr gears)
    low_speed_planetary_df = (
        planetary_gears_df.filter(pl.col("index").str.contains("Hr"))
        .with_columns(
            pl.col("index")
            .map_elements(
                lambda x: f"low_speed_gear_{x.lower().replace('engranaje ', '').replace(' hr.', 'hr').replace(' ', '_')}"
            )
            .alias("part_name")
        )
        .rename({"Numero de Serie": "part_serial"})
    )

    # Translation map for sub-components
    translation_map = {
        "coupling plate": "coupling_plate",
        "ring gear": "ring_gear",
        "piñon solar": "sun_pinion",
        "eje palier": "driveshaft",
    }

    # Filter and process sub-components
    subcomp_df = (
        subcomp_df.filter(
            pl.col("index").str.to_lowercase().is_in(["coupling plate", "ring gear", "piñon solar", "eje palier"])
        )
        .with_columns(
            [
                pl.col("index")
                .map_elements(lambda x: translation_map.get(x.lower(), x.lower().replace(" ", "_")))
                .alias("part_name"),
                pl.col("Numero de Serie").str.to_lowercase().alias("part_serial"),
            ]
        )
        .select(["service_order", "component_serial", "extraction_status", "part_name", "part_serial"])
    )

    # Standardize column names for all dataframes
    high_speed_planetary_df = high_speed_planetary_df.select(
        ["service_order", "component_serial", "extraction_status", "part_name", "part_serial"]
    )
    low_speed_planetary_df = low_speed_planetary_df.select(
        ["service_order", "component_serial", "extraction_status", "part_name", "part_serial"]
    )

    # Combine all dataframes
    df = (
        pl.concat([high_speed_planetary_df, low_speed_planetary_df, subcomp_df])
        .pipe(clean_part_serial)
        .with_columns(document_type=pl.lit("final_report"))
    )

    # # Pivot to get final structure with aggregation to handle duplicates
    # final_df = combined_df.pivot(
    #     values="serial_number",
    #     index=["service_order", "component_serial", "extraction_status"],
    #     on="component_type",
    #     aggregate_function="first",
    # )

    return df


@dg.asset(compute_kind="mutate")
def mutate_mt_docs(
    context: dg.AssetExecutionContext, preliminary_report_mt_docs: pl.DataFrame, final_report_mt_docs: pl.DataFrame
):
    dl = DataLake(context)

    df = pl.concat([preliminary_report_mt_docs, final_report_mt_docs], how="diagonal")

    dl.upload_tibble(tibble=df, az_path=DATA_CATALOG["mt_docs"]["analytics_path"])
    return df
