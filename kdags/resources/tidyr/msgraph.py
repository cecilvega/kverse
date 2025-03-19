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
        self._site_url_prefix = "https://globalkomatsu.sharepoint.com/sites/"

    def delete_file(self, site_id: str, filepath: str) -> dict:
        """
        Deletes a file from SharePoint, bypassing any shared locks.

        Args:
            site_id (str): SharePoint site ID (e.g., "KCHCLSP00022")
            filepath (str): Full path including filename within the site

        Returns:
            dict: Information about the deletion result
        """

        def bypass_lock(request):
            request.headers["Prefer"] = "bypass-shared-lock"

        try:
            site_id = f"{self._site_url_prefix}/{site_id}"

            site = self.client.sites.get_by_url(site_id)
            drive_item = site.drive.root.get_by_path(filepath).get().execute_query()
            drive_item.before_execute(bypass_lock)

            drive_item.delete_object().execute_query()
            return {"status": "success", "message": f"File '{filepath}' successfully deleted"}
        except Exception as e:
            return {"status": "error", "message": f"Error deleting file: {filepath}"}

    def acquire_token_func(self):
        scopes = ["Files.ReadWrite.All"]
        app = msal.PublicClientApplication(
            client_id=self._client_id, authority="https://login.microsoftonline.com/common"
        )
        refresh_token = os.environ["MSGRAPH_TOKEN"]
        return app.acquire_token_by_refresh_token(refresh_token, scopes)

    def read_bytes(self, site_id, filepath):
        site_id = f"{self._site_url_prefix}/{site_id}"
        site = self.client.sites.get_by_url(site_id)
        drive_item = site.drive.root.get_by_path(filepath).get().execute_query()
        content = drive_item.get_content().execute_query().value
        return content

    def upload_tibble(
        self, site_id: str, filepath: str, df, format: str = "excel", sheet_name: str = "Sheet1", **kwargs
    ) -> object:
        """
        Uploads a DataFrame to SharePoint in the specified format.

        Args:
            site_id (str): SharePoint site ID (e.g., "KCHCLSP00022")
            filepath (str): Full path including filename within the site
            df: DataFrame to upload (pandas or polars)
            format (str): Output format ('excel', 'csv', 'parquet', 'json')
            sheet_name (str): Sheet name for Excel files (default: 'Sheet1')
            **kwargs: Additional arguments passed to the serialization function

        Returns:
            object: Result of the upload operation
        """

        # Extract folder path and filename from file_path
        folder_path = os.path.dirname(filepath)
        file_name = os.path.basename(filepath)

        # Convert polars DataFrame to pandas if needed
        if hasattr(df, "to_pandas"):
            df = df.to_pandas()

        # Create buffer and convert DataFrame based on format
        buffer = BytesIO()

        if format.lower() == "excel":
            # Default to xlsx engine
            engine = kwargs.pop("engine", "openpyxl")
            df.to_excel(buffer, sheet_name=sheet_name, engine=engine, index=False, **kwargs)
        elif format.lower() == "csv":
            df.to_csv(buffer, index=False, **kwargs)
        elif format.lower() == "parquet":
            df.to_parquet(buffer, **kwargs)

        else:
            raise ValueError(f"Unsupported format: {format}")

        # Ensure filename has correct extension if not provided
        if not file_name.endswith(f".{format}") and format.lower() != "excel":
            file_name = f"{file_name}.{format}"
        elif format.lower() == "excel" and not (file_name.endswith(".xlsx") or file_name.endswith(".xls")):
            file_name = f"{file_name}.xlsx"

        # Reset buffer position to start
        buffer.seek(0)
        content = buffer.getvalue()

        # Get site and upload
        site_url = f"{self._site_url_prefix}/{site_id}"
        site = self.client.sites.get_by_url(site_url)

        self.delete_file(site_id, filepath)
        # Upload the file
        return site.drive.root.get_by_path(folder_path).upload(file_name, content).execute_query()

    def read_tibble(self, site_id, filepath, use_polars=True, **kwargs) -> pd.DataFrame:
        """
        Read tabular data from downloaded content. Automatically detects file type if not specified.

        Args:
            content (bytes): File content as bytes (from download_file)
            file_type (str, optional): Force specific file type ('excel', 'csv', etc.).
                                     If None, will try to detect from file extension
            **kwargs: Additional arguments passed to the appropriate pandas read function

        Returns:
            pd.DataFrame: The contents of the file as a DataFrame

        Raises:
            ValueError: If file type cannot be determined or is not supported
        """

        # Get the file bytes
        file_bytes = self.read_bytes(site_id, filepath)

        # Determine file type from extension
        file_ext = filepath.split(".")[-1].lower()

        # Create BytesIO object
        buffer = BytesIO(file_bytes)

        # Parse based on file type
        if file_ext == "parquet":
            if use_polars:
                return pl.read_parquet(buffer, **kwargs)
            else:
                return pd.read_parquet(buffer, **kwargs)

        elif file_ext == "csv":
            if use_polars:
                return pl.read_csv(buffer, **kwargs)
            else:
                return pd.read_csv(buffer, **kwargs)

        elif file_ext in ["xlsx", "xls"]:
            # Polars has limited Excel support, so use pandas first
            pandas_df = pd.read_excel(buffer, **kwargs)
            if use_polars:
                return pl.from_pandas(pandas_df)
            else:
                return pandas_df

        elif file_ext == "json":
            if use_polars:
                return pl.read_json(buffer, **kwargs)
            else:
                return pd.read_json(buffer, **kwargs)

        else:
            raise ValueError(f"Unsupported file format: .{file_ext}")

    def list_paths(self, site_url: str, folder_path: str) -> pd.DataFrame:

        site = self.client.sites.get_by_url(site_url)
        items = []
        self._list_items_recursive(
            site.drive.root.get_by_path(folder_path),
            items,
            base_path=folder_path,
        )

        if not items:
            return pd.DataFrame(
                columns=[
                    "name",
                    "path",
                    "size_bytes",
                    "last_modified",
                    "web_url",
                ]
            )

        return pd.DataFrame(items)

    def _list_items_recursive(self, folder_item, items, base_path: str) -> None:

        children = folder_item.children.get().execute_query()

        for item in children:
            if item.is_folder:
                self._list_items_recursive(item, items, base_path=f"{base_path}/{item.name}")
                continue

            item_info = {
                "name": item.name,
                "path": f"{base_path}/{item.name}",
                "size_bytes": item.properties.get("size"),
                "last_modified": item.last_modified_datetime.strftime("%Y-%m-%d %H:%M:%S"),
                "web_url": item.web_url,
            }

            items.append(item_info)

            if item.is_folder:
                self._list_items_recursive(item, items, base_path=f"{base_path}/{item.name}")

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
