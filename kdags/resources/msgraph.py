import os

import msal
import pandas as pd
from office365.graph_client import GraphClient
import requests
from io import BytesIO


class MSGraph:
    def __init__(self):
        self.client = GraphClient(self.acquire_token_func)
        self._client_id = "d50ca740-c83f-4d1b-b616-12c519384f0c"
        self._site_url_prefix = "https://globalkomatsu.sharepoint.com/sites/"

    def acquire_token_func(self):
        scopes = ["Files.ReadWrite.All"]
        app = msal.PublicClientApplication(
            client_id=self._client_id, authority="https://login.microsoftonline.com/common"
        )
        refresh_token = os.environ["MSGRAPH_TOKEN"]
        return app.acquire_token_by_refresh_token(refresh_token, scopes)

    def get_sharepoint_file_content(self, site_id, file_path):
        site_id = f"{self._site_url_prefix}/{site_id}"
        site = self.client.sites.get_by_url(site_id)
        drive_item = site.drive.root.get_by_path(file_path).get().execute_query()
        content = drive_item.get_content().execute_query().value
        return content

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

    def download_file(self, site_url: str, file_path: str) -> bytes:
        """
        Download a file from SharePoint and return its content as bytes.

        Args:
            site_url (str): The SharePoint site URL
            file_path (str): The relative path to the file within the site

        Returns:
            bytes: The raw content of the file

        Raises:
            Exception: If there's an error downloading the file
        """
        try:
            site = self.client.sites.get_by_url(site_url)
            file = site.drive.root.get_by_path(file_path).get().execute_query()
            content = file.get_content().execute_query().value
            return content
        except Exception as e:
            raise Exception(f"Error downloading file {file_path}: {str(e)}")

    def read_tibble(self, content: bytes, file_type: None, **kwargs) -> pd.DataFrame:
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
        try:
            buffer = BytesIO(content)

            if file_type == "excel" or (
                file_type is None and kwargs.get("filepath_or_buffer", "").endswith((".xlsx", ".xls"))
            ):
                return pd.read_excel(buffer, **kwargs)
            elif file_type == "csv" or (file_type is None and kwargs.get("filepath_or_buffer", "").endswith(".csv")):
                return pd.read_csv(buffer, **kwargs)
            elif file_type == "parquet" or (
                file_type is None and kwargs.get("filepath_or_buffer", "").endswith(".parquet")
            ):
                return pd.read_parquet(buffer, **kwargs)
            else:
                raise ValueError(f"Unsupported or undetectable file type. Please specify file_type explicitly.")

        except Exception as e:
            raise Exception(f"Error reading file content: {str(e)}")

    def upload_image_to_sharepoint(
        self,
        site_url: str,
        target_folder: str,
        reso_work_order: int,
        blob_url: str,
        subtitle: str,
    ):
        """
        Downloads an image from a blob URL, appends a cleaned-up subtitle to the filename,
        and uploads it to the specified SharePoint folder.
        """
        # Clean subtitle: replace newline characters with spaces.
        subtitle_clean = subtitle.replace("\n", " ").strip()

        # Download the image using requests.
        response = requests.get(blob_url)
        if response.status_code != 200:
            raise Exception(f"Failed to download image. Status code: {response.status_code}")
        image_content = response.content

        # Extract the original file name from the blob URL (remove any query parameters).
        original_file_name = blob_url.split("/")[-1].split("?")[0]

        # Create the new filename, e.g., "Reception_0a0483c0-3000-45bf-9a9e-46cbf248bcfd.jpg"
        new_file_name = f"{subtitle_clean}_{original_file_name}"

        # Get the target site and folder.
        site = self.client.sites.get_by_url(site_url)
        # This assumes that the folder 'target_folder' (e.g., "14141771") already exists.
        folder = site.drive.root.get_by_path(f"{target_folder}/{reso_work_order}")

        # Upload the image.
        upload_result = folder.upload(new_file_name, image_content).execute_query()
        print("Uploaded file URL:", upload_result.web_url)
        return upload_result.web_url
