import time

import dagster as dg

from kdags.resources.tidyr import DataLake, MSGraph


@dg.asset(
    description="Uploads maintenance report PDFs from Data Lake to SharePoint organized by equipment",
    compute_kind="standalone",
)
def transfer_sp_pm_reports(context):
    """
    Uploads maintenance reports from Data Lake to SharePoint based on DataFrame data.

    The function reads files from paths in the maintenance_reports asset and uploads
    them to a structured folder in SharePoint based on equipment name.

    Returns:
        dict: Summary of upload results including success rate and count
    """
    # Initialize resources
    datalake = DataLake()
    msgraph = MSGraph()

    # Get input dataframe from upstream assets

    df = msgraph.read_tibble(
        site_id="KCHCLSP00022",
        filepath="/01. ÁREAS KCH/1.6 CONFIABILIDAD/CAEX/ANTECEDENTES/MANTENIMIENTO/PAUTAS_MANTENIMIENTO/pm_history.xlsx",
        use_polars=False,
    )
    df = df.loc[df["site_name"] == "MEL"].reset_index(drop=True)
    df = df.assign(file_path=df["pm_id"].map(lambda x: f"BHP/FIORI/PM_REPORTS/{x}.pdf"))

    # Configuration
    source_container = "kcc-raw-data"
    target_site_id = "KCHCLSP00022"
    overwrite = False

    # Validate input DataFrame
    required_columns = ["file_path", "equipment_name", "pm_id", "summary_content"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"DataFrame missing required columns: {missing_columns}")

    base_target_folder = "/01. ÁREAS KCH/1.6 CONFIABILIDAD/CAEX/ANTECEDENTES/MANTENIMIENTO/PAUTAS_MANTENIMIENTO"
    results = []

    total_files = len(df)
    successful_uploads = 0
    failed_uploads = 0

    context.log.info(f"Starting upload of {total_files} maintenance reports to SharePoint")
    start_time_total = time.time()

    for index, row in df.iterrows():
        file_start_time = time.time()
        try:
            # Get source file path
            source_path = row["file_path"]

            # Create custom file name
            file_name = f"{row['pm_id']}_{row['summary_content']}.pdf"

            # Get equipment name
            equipment_name = row["equipment_name"]

            context.log.info(f"Processing file {index+1}/{total_files}: {file_name} for {equipment_name}")

            # Create target folder path with equipment name
            target_folder = f"{base_target_folder}/{equipment_name}"

            # Read file content from Data Lake
            context.log.debug(f"Reading file content from Data Lake: {source_path}")
            read_start = time.time()
            content = datalake.read_bytes(source_container, source_path)
            read_time = time.time() - read_start

            # Upload to SharePoint
            context.log.debug(f"Uploading to SharePoint folder: {target_folder}")
            upload_start = time.time()
            upload_result = msgraph.upload_file(
                site_id=target_site_id,
                folder_path=target_folder,
                file_name=file_name,
                content=content,
                overwrite=overwrite,
            )
            upload_time = time.time() - upload_start

            file_total_time = time.time() - file_start_time

            # Record the result
            result_entry = {
                "equipment_name": equipment_name,
                "source_path": source_path,
                "target_path": f"{target_folder}/{file_name}",
                "processing_time_sec": round(file_total_time, 2),
                "read_time_sec": round(read_time, 2),
                "upload_time_sec": round(upload_time, 2),
                **upload_result,
            }

            results.append(result_entry)

            if upload_result["status"] == "uploaded":
                successful_uploads += 1
                context.log.info(
                    f"Successfully uploaded ({index+1}/{total_files}): {file_name} in {file_total_time:.2f}s"
                )
            else:
                context.log.info(f"File already exists ({index+1}/{total_files}): {file_name}")

            # Show progress information
            remaining = total_files - (index + 1)
            context.log.info(f"Progress: {index+1}/{total_files} complete, {remaining} remaining")

        except Exception as e:
            failed_uploads += 1
            file_error_time = time.time() - file_start_time
            error_entry = {
                "equipment_name": row.get("equipment_name", "Unknown"),
                "source_path": row.get("file_path", "Unknown"),
                "status": "error",
                "processing_time_sec": round(file_error_time, 2),
                "message": f"Error transferring file: {str(e)}",
            }
            results.append(error_entry)
            context.log.error(f"Error processing file {index+1}/{total_files}: {str(e)}")

    total_time = time.time() - start_time_total

    # Prepare summary information
    summary = {
        "total_files": total_files,
        "successful_uploads": successful_uploads,
        "failed_uploads": failed_uploads,
        "skipped_files": total_files - successful_uploads - failed_uploads,
        "success_rate": round(successful_uploads / total_files * 100, 2) if total_files > 0 else 0,
        "total_time_sec": round(total_time, 2),
        "average_time_per_file_sec": round(total_time / total_files, 2) if total_files > 0 else 0,
        "detailed_results": results,
    }

    context.log.info(
        f"Upload complete. Total time: {total_time:.2f}s. "
        f"Success rate: {summary['success_rate']}% "
        f"({successful_uploads}/{total_files})"
    )

    return summary
