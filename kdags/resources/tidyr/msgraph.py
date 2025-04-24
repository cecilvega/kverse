import os
from io import BytesIO

import msal
import pandas as pd
import requests
from office365.graph_client import GraphClient
import polars as pl


class MSGraph:
    def __init__(self):
        self.client = GraphClient(self.acquire_token_func)
        self._client_id = "d50ca740-c83f-4d1b-b616-12c519384f0c"

        self._site_url_base = "https://globalkomatsu.sharepoint.com/sites/"

    def _parse_sp_path(self, sp_path: str) -> tuple[str, str]:
        """
        Parses an sp_path string (sp://site_id/path/to/file) into site_id and file_path.

        Args:
            sp_path (str): The SharePoint path string.

        Returns:
            tuple[str, str]: A tuple containing (site_id, file_path).

        Raises:
            ValueError: If the sp_path format is invalid.
        """
        if not sp_path.startswith("sp://"):
            raise ValueError(f"Invalid sp_path format: {sp_path}. Expected format: sp://<site_id>/<file_path>")

        # Remove the scheme and split at the first '/'
        path_part = sp_path[len("sp://") :]
        parts = path_part.split("/", 1)

        if len(parts) != 2:
            # Handle case where there might be no path part (e.g., sp://site_id/) - treat path as empty
            if len(parts) == 1 and parts[0]:
                return parts[0], ""
            else:
                raise ValueError(f"Invalid sp_path format: {sp_path}. Could not extract site_id and file_path.")

        site_id, file_path = parts
        return site_id, file_path

    def delete_file(self, sp_path: str) -> dict:
        """
        Deletes a file from SharePoint using sp_path, bypassing any shared locks.

        Args:
            sp_path (str): SharePoint resource path (e.g., "sp://KCHCLSP00022/Shared Documents/MyFile.xlsx")

        Returns:
            dict: Information about the deletion result (no try/except added per request)
        """
        site_id, file_path = self._parse_sp_path(sp_path)
        site_url = f"{self._site_url_base}{site_id}"  # Construct full site url

        def bypass_lock(request):
            request.headers["Prefer"] = "bypass-shared-lock"

        # Original logic without try/except
        site = self.client.sites.get_by_url(site_url)
        drive_item = site.drive.root.get_by_path(file_path).get().execute_query()
        drive_item.before_execute(bypass_lock)

        drive_item.delete_object().execute_query()
        # Simplified return without error handling wrapper
        return {"status": "success", "message": f"File '{file_path}' successfully deleted from site '{site_id}'"}

    def acquire_token_func(self):
        scopes = ["Files.ReadWrite.All"]
        app = msal.PublicClientApplication(
            client_id=self._client_id,
            authority="https://login.microsoftonline.com/common",
        )
        # Ensure MSGRAPH_TOKEN environment variable is set in your environment
        refresh_token = os.environ["MSGRAPH_TOKEN"]
        return app.acquire_token_by_refresh_token(refresh_token, scopes)

    def read_bytes(self, sp_path: str) -> bytes:
        """
        Reads the content of a file from SharePoint as bytes using sp_path.

        Args:
            sp_path (str): SharePoint resource path (e.g., "sp://KCHCLSP00022/Shared Documents/MyFile.xlsx")

        Returns:
            bytes: The content of the file.
        """
        site_id, file_path = self._parse_sp_path(sp_path)
        site_url = f"{self._site_url_base}{site_id}"  # Construct full site url
        site = self.client.sites.get_by_url(site_url)
        drive_item = site.drive.root.get_by_path(file_path).get().execute_query()
        content = drive_item.get_content().execute_query().value
        return content

    def upload_tibble(self, sp_path: str, df, format: str = "excel", sheet_name: str = "Sheet1", **kwargs) -> object:
        """
        Uploads a DataFrame to SharePoint using sp_path in the specified format.
        Overwrites the file if it exists.

        Args:
            sp_path (str): SharePoint resource path including filename (e.g., "sp://KCHCLSP00022/Shared Documents/MyData.xlsx")
            df: DataFrame to upload (pandas or polars)
            format (str): Output format ('excel', 'csv', 'parquet', 'json')
            sheet_name (str): Sheet name for Excel files (default: 'Sheet1')
            **kwargs: Additional arguments passed to the serialization function

        Returns:
            object: Result of the upload operation (typically a DriveItem)
        """
        site_id, file_path = self._parse_sp_path(sp_path)
        site_url = f"{self._site_url_base}{site_id}"  # Construct full site url

        # Extract folder path and filename from file_path
        folder_path = os.path.dirname(file_path)
        file_name = os.path.basename(file_path)

        # Convert polars DataFrame to pandas if needed for formats like Excel
        # Check specifically if pandas methods are needed and df is not already pandas
        if format.lower() in ["excel", "csv", "json"] and hasattr(df, "to_pandas") and not isinstance(df, pd.DataFrame):
            df_pd = df.to_pandas()
        elif isinstance(df, pd.DataFrame):
            df_pd = df  # It's already pandas
        else:
            df_pd = None  # Polars df for parquet

        # Create buffer and convert DataFrame based on format
        buffer = BytesIO()

        if format.lower() == "excel":
            if df_pd is None:
                raise TypeError("Pandas DataFrame required for Excel format.")
            engine = kwargs.pop("engine", "openpyxl")
            df_pd.to_excel(buffer, sheet_name=sheet_name, engine=engine, index=False, **kwargs)
        elif format.lower() == "csv":
            if df_pd is None:
                raise TypeError("Pandas DataFrame required for CSV format.")
            df_pd.to_csv(buffer, index=False, **kwargs)
        elif format.lower() == "parquet":
            # Use original df (could be polars or pandas)
            df.to_parquet(buffer, **kwargs)
        elif format.lower() == "json":
            if df_pd is None:
                raise TypeError("Pandas DataFrame required for JSON format.")
            df_pd.to_json(buffer, orient="records", lines=True, **kwargs)  # Example, adjust as needed
        else:
            raise ValueError(f"Unsupported format: {format}")

        # Ensure filename has correct extension if not provided
        # (Using os.path.splitext to handle cases like 'file.tar.gz')
        base_name, current_ext = os.path.splitext(file_name)
        expected_ext = f".{format.lower()}"
        if format.lower() == "excel":
            if current_ext.lower() not in [".xlsx", ".xls"]:
                file_name = f"{base_name}.xlsx"  # Default to .xlsx
        elif current_ext.lower() != expected_ext:
            file_name = f"{file_name}{expected_ext}"  # Append if missing, careful with double dots

        # Reset buffer position to start
        buffer.seek(0)
        content = buffer.getvalue()

        # Get site object
        site = self.client.sites.get_by_url(site_url)

        # Attempt to delete the file first (if it exists)
        # Since try/except is disallowed, this might fail if the file doesn't exist.
        # Consider adding an existence check method if needed, or handle potential errors upstream.
        try:
            self.delete_file(sp_path)  # Use the same sp_path for deletion
        except Exception as delete_error:
            # If delete fails (e.g., file not found), ignore and proceed with upload
            print(f"Note: Could not delete existing file '{sp_path}' before upload (may not exist): {delete_error}")
            pass

        # Upload the file to the correct folder
        target_folder = site.drive.root.get_by_path(folder_path)
        upload_result = target_folder.upload(file_name, content).execute_query()
        return upload_result

    def read_tibble(self, sp_path: str, **kwargs) -> pd.DataFrame:
        """
        Read tabular data from SharePoint using sp_path. Automatically detects file type.

        Args:
            sp_path (str): SharePoint resource path (e.g., "sp://KCHCLSP00022/Shared Documents/MyData.csv")
            use_polars (bool): If True, returns a Polars DataFrame, otherwise Pandas.
            **kwargs: Additional arguments passed to the appropriate read function

        Returns:
            Union[pl.DataFrame, pd.DataFrame]: The contents of the file as a DataFrame

        Raises:
            ValueError: If file type cannot be determined or is not supported
        """
        _, file_path = self._parse_sp_path(sp_path)  # Parse once to get file_path for extension check

        # Get the file bytes using the existing read_bytes method
        file_bytes = self.read_bytes(sp_path)  # Pass the sp_path directly

        # Determine file type from extension in the file_path part
        file_ext = os.path.splitext(file_path)[1].lower()

        # Create BytesIO object
        buffer = BytesIO(file_bytes)

        # Parse based on file type
        if file_ext == ".parquet":

            return pl.read_parquet(buffer, **kwargs)

        elif file_ext == ".csv":

            return pl.read_csv(buffer, **kwargs)

        elif file_ext in [".xlsx", ".xls"]:

            return pl.read_excel(buffer, **kwargs)

        else:
            raise ValueError(f"Unsupported file format: {file_ext}")

    # list_paths remains unchanged as it uses site_url and folder_path differently
    def list_paths(self, site_url: str, folder_path: str) -> pd.DataFrame:
        """
        Lists files and folders within a specific SharePoint folder.

        Args:
            site_url (str): The full URL of the SharePoint site (e.g., "[https://globalkomatsu.sharepoint.com/sites/KCHCLSP00022](https://globalkomatsu.sharepoint.com/sites/KCHCLSP00022)")
            folder_path (str): The path within the site's default document library (e.g., "Shared Documents/MyFolder")

        Returns:
            pd.DataFrame: A DataFrame containing information about the items found.
        """
        site = self.client.sites.get_by_url(site_url)
        items = []
        target_folder_item = site.drive.root.get_by_path(folder_path)
        self._list_items_recursive(
            target_folder_item,
            items,
            base_path=folder_path,  # Pass the initial folder path as base
        )

        if not items:
            return pd.DataFrame(
                columns=[
                    "name",
                    "path",
                    "size_bytes",
                    "last_modified",
                    "web_url",
                    "is_folder",  # Added column
                ]
            )
        # Convert list of dicts to DataFrame
        df = pd.DataFrame(items)
        # Ensure consistent column order and add is_folder if missing
        cols = ["name", "path", "size_bytes", "last_modified", "web_url", "is_folder"]
        for col in cols:
            if col not in df.columns:
                df[col] = None  # Add missing columns as None
        return df[cols]

    def _list_items_recursive(self, folder_item, items_list, base_path: str) -> None:
        """Helper for recursively listing items"""
        children = folder_item.children.get().execute_query()

        for item in children:
            current_path = f"{base_path}/{item.name}" if base_path else item.name
            is_folder = item.is_folder

            item_info = {
                "name": item.name,
                "path": current_path,  # Construct full path relative to drive root
                "size_bytes": item.properties.get("size"),
                "last_modified": (
                    item.last_modified_datetime.strftime("%Y-%m-%d %H:%M:%S") if item.last_modified_datetime else None
                ),
                "web_url": item.web_url,
                "is_folder": is_folder,
            }
            items_list.append(item_info)

            # If it's a folder, recurse into it using the constructed path
            if is_folder:
                self._list_items_recursive(item, items_list, base_path=current_path)

    def upload_file(
        self, site_id: str, folder_path: str, file_name: str, content: bytes, overwrite: bool = True
    ) -> dict:
        """
        Uploads file content directly to SharePoint, with optional existence check.

        Args:
            site_id (str): SharePoint site ID (e.g., "KCHCLSP00022")
            folder_path (str): Folder path within the site
            file_name (str): Name for the uploaded file
            content (bytes): Raw content of the file as bytes
            overwrite (bool): If True, upload regardless of existence. If False, check first and skip if exists.

        Returns:
            dict: Information about the upload result, including file URL if uploaded
        """
        try:
            site_url = f"{self._site_url_prefix}/{site_id}"
            site = self.client.sites.get_by_url(site_url)
            folder = site.drive.root.get_by_path(folder_path)

            # Check if the file exists and we're not overwriting
            if not overwrite:
                try:
                    # Try to get the file to see if it exists
                    existing_file = folder.get_by_path(file_name).get().execute_query()
                    # If we get here, file exists
                    return {
                        "status": "skipped",
                        "message": f"File {file_name} already exists and overwrite is False",
                        "web_url": existing_file.web_url,
                    }
                except Exception:
                    # File doesn't exist, continue with upload
                    pass

            # Upload the file
            upload_result = folder.upload(file_name, content).execute_query()

            return {
                "status": "uploaded",
                "message": f"File {file_name} uploaded successfully",
                "web_url": upload_result.web_url,
            }

        except Exception as e:
            # Handle other errors (like folder doesn't exist, permission issues, etc.)
            return {"status": "error", "message": f"Error uploading file: {str(e)}"}

    def _store_new_refresh_token(self):

        client_id = "d50ca740-c83f-4d1b-b616-12c519384f0c"
        scopes = ["Files.ReadWrite.All"]
        from pathlib import Path
        import json

        token_file = Path(__file__).parent / "ms_graph_token.json"

        app = msal.PublicClientApplication(client_id=client_id, authority="https://login.microsoftonline.com/common")

        # Get new tokens if refresh fails or no stored token
        flow = app.initiate_device_flow(scopes)
        print(flow["message"])
        result = app.acquire_token_by_device_flow(flow)

        # Store the refresh token
        with open(token_file, "w") as f:
            json.dump(result, f)
