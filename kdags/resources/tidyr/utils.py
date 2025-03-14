from .msgraph import MSGraph
from .datalake import DataLake


def transfer_dl_sp(
    file_list: list,
    datalake: DataLake,
    msgraph: MSGraph,
    source_container: str,
    target_site_id: str,
    target_folder: str,
    overwrite: bool = False,
) -> list:
    """
    Transfers files from Azure Data Lake to SharePoint.

    Args:
        file_list (list): List of file paths in the Data Lake
        datalake (DataLake): Initialized DataLake object
        msgraph (MSGraph): Initialized MSGraph object
        source_container (str): Source container in Azure Data Lake
        target_site_id (str): Target SharePoint site ID
        target_folder (str): Target folder path in SharePoint
        overwrite (bool): Whether to overwrite existing files

    Returns:
        list: Results of each file transfer with status information
    """
    results = []

    for file_path in file_list:
        try:
            # Extract just the filename from the path
            file_name = file_path.split("/")[-1]

            # Get file content from Data Lake
            content = datalake.read_bytes(source_container, file_path)

            # Upload to SharePoint
            upload_result = msgraph.upload_file(
                site_id=target_site_id,
                folder_path=target_folder,
                file_name=file_name,
                content=content,
                overwrite=overwrite,
            )

            # Record the result with the file path for reference
            results.append({"source_path": file_path, **upload_result})  # Include all fields from upload_result

        except Exception as e:
            results.append(
                {"source_path": file_path, "status": "error", "message": f"Error transferring file: {str(e)}"}
            )

    return results
