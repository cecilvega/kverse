import dagster as dg
from kdags.config import DATA_CATALOG

# --- Relative module imports
from kdags.resources.tidyr import DataLake
import polars as pl
from ..extract_utils import *
from ..extra_utils import get_documents, clean_part_serial
from concurrent.futures import ThreadPoolExecutor, as_completed


def preliminary_report_mt_docs(context: dg.AssetExecutionContext, report: dict):
    """Process a SINGLE preliminary report"""

    # Define table configurations
    table_configs = [
        {
            "key": "high_speed_gear",
            "title": "control dimensional de engranajes de carrier de alta velocidad",
            "columns": ["numero de serie"],
        },
        {
            "key": "low_speed_gear",
            "title": "control dimensional de engranajes de carrier de baja velocidad",
            "columns": ["numero de serie"],
        },
        {"key": "subcomp", "title": "numero de serie de sub-componentes", "columns": ["numero de serie"]},
    ]

    # Extract tables from single report (no parallelization)
    results = extract_tibble_from_single_report(context, report, table_configs)

    # Access results
    high_speed_gear_df = results["high_speed_gear"].drop(["pages", "section_number"])
    low_speed_gear_df = results["low_speed_gear"].drop(["pages", "section_number"])
    subcomp_df = results["subcomp"].drop(["pages", "section_number"])

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

    return df


def final_report_mt_docs(context: dg.AssetExecutionContext, report: dict):
    """Process a SINGLE final report"""

    # Define table configurations
    table_configs = [
        {"key": "planetary_gears", "title": "numero de series de planetarios", "columns": ["numero de serie"]},
        {"key": "subcomponents", "title": "numero de serie de sub-componentes", "columns": ["numero de serie"]},
    ]

    # Extract tables from single report (no parallelization)
    results = extract_tibble_from_single_report(context, report, table_configs)

    # Access results
    planetary_gears_df = results["planetary_gears"].drop(["pages", "section_number"])
    subcomp_df = results["subcomponents"].drop(["pages", "section_number"])

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

    return df


@dg.asset(compute_kind="mutate")
def process_all_mt_reports(context, component_reparations, so_documents):
    overwrite = False
    """
    Process all MT reports with overwrite control

    Args:
        overwrite: If True, process all. If False, skip existing files.

    Returns:
        List of created file paths
    """

    dl = DataLake(context)

    # Get preliminary and final reports
    preliminary_reports = get_documents(
        component_reparations=component_reparations,
        so_documents=so_documents,
        subcomponent_tag="0980",
        file_type="preliminary_report",
    )

    final_reports = get_documents(
        component_reparations=component_reparations,
        so_documents=so_documents,
        subcomponent_tag="0980",
        file_type="final_report",
    )

    # Create a dict mapping service_order to reports
    reports_by_so = {}

    for report in preliminary_reports:
        so = report["service_order"]
        if so not in reports_by_so:
            reports_by_so[so] = {"preliminary": None, "final": None}
        reports_by_so[so]["preliminary"] = report

    for report in final_reports:
        so = report["service_order"]
        if so not in reports_by_so:
            reports_by_so[so] = {"preliminary": None, "final": None}
        reports_by_so[so]["final"] = report

    # Check existing files if overwrite=False
    if not overwrite:
        context.log.info("Checking for existing files...")
        existing_files = set()

        # List existing files in the output directory
        try:
            existing_df = dl.list_paths("az://bhp-process-data/RESO/DOCUMENTS/MOTOR_TRACCION/")
            existing_files = {
                int(path.split("/")[-1].replace(".parquet", ""))
                for path in existing_df["az_path"].to_list()
                if path.endswith(".parquet") and path.split("/")[-1].replace(".parquet", "").isdigit()
            }
            context.log.info(f"Found {len(existing_files)} existing files")
        except Exception as e:
            context.log.warning(f"Could not list existing files: {e}")

    # Filter service orders to process
    if not overwrite:
        so_to_process = {so: reports for so, reports in reports_by_so.items() if so not in existing_files}
        context.log.info(f"Skipping {len(reports_by_so) - len(so_to_process)} already processed files")
    else:
        so_to_process = reports_by_so

    # Process each service order
    def process_service_order(so, reports_dict):
        """Process both preliminary and final reports for a service order"""
        output_path = f"az://bhp-process-data/RESO/DOCUMENTS/MOTOR_TRACCION/{so}.parquet"

        dfs_to_concat = []

        # Process preliminary report if exists
        if reports_dict["preliminary"]:
            try:
                prelim_df = preliminary_report_mt_docs(context, reports_dict["preliminary"])
                if prelim_df.height > 0:
                    dfs_to_concat.append(prelim_df)
            except Exception as e:
                context.log.warning(f"Error processing preliminary report for SO {so}: {e}")

        # Process final report if exists
        if reports_dict["final"]:
            try:
                final_df = final_report_mt_docs(context, reports_dict["final"])
                if final_df.height > 0:
                    dfs_to_concat.append(final_df)
            except Exception as e:
                context.log.warning(f"Error processing final report for SO {so}: {e}")

        # Concatenate results
        if dfs_to_concat:
            combined_df = pl.concat(dfs_to_concat, how="diagonal")

            # Upload to Azure
            try:
                dl.upload_tibble(combined_df, output_path)
                context.log.info(f"Uploaded results for SO {so} to {output_path}")
                return output_path, True
            except Exception as e:
                context.log.error(f"Failed to upload SO {so}: {e}")
                return None, False
        else:
            context.log.warning(f"No data extracted for SO {so}")
            return None, False

    # Execute in parallel
    created_files = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_service_order, so, reports): so for so, reports in so_to_process.items()}

        for future in as_completed(futures):
            file_path, success = future.result()
            if success and file_path:
                created_files.append(file_path)

    context.log.info(f"Successfully processed {len(created_files)}/{len(so_to_process)} service orders")

    return created_files


#
# def mutate_mt_docs(context: dg.AssetExecutionContext, component_reparations: pl.DataFrame, so_documents: pl.DataFrame):
#     dl = DataLake(context)
#     preliminary_reports = get_documents(
#         component_reparations=component_reparations,
#         so_documents=so_documents,
#         subcomponent_tag="0980",
#         file_type="preliminary_report",
#     )
#
#     final_reports = get_documents(
#         component_reparations=component_reparations,
#         so_documents=so_documents,
#         subcomponent_tag="0980",
#         file_type="final_report",
#     )
#
#     df = pl.concat([preliminary_report_mt_docs, final_report_mt_docs], how="diagonal")
#
#     dl.upload_tibble(tibble=df, az_path=DATA_CATALOG["mt_docs"]["analytics_path"])
#     return df
