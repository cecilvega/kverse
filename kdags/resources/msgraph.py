import os

import msal
import pandas as pd
from office365.graph_client import GraphClient
import requests


class MSGraph:
    def __init__(self):
        self.client = GraphClient(self.acquire_token_func)

    def acquire_token_func(self):

        client_id = "d50ca740-c83f-4d1b-b616-12c519384f0c"
        scopes = ["Files.ReadWrite.All"]

        app = msal.PublicClientApplication(
            client_id=client_id, authority="https://login.microsoftonline.com/common"
        )

        refresh_token = os.environ["MSGRAPH_TOKEN"]
        return app.acquire_token_by_refresh_token(refresh_token, scopes)

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
                self._list_items_recursive(
                    item, items, base_path=f"{base_path}/{item.name}"
                )
                continue

            item_info = {
                "name": item.name,
                "path": f"{base_path}/{item.name}",
                "size_bytes": item.properties.get("size"),
                "last_modified": item.last_modified_datetime.strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "web_url": item.web_url,
            }

            items.append(item_info)

            if item.is_folder:
                self._list_items_recursive(
                    item, items, base_path=f"{base_path}/{item.name}"
                )

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
            raise Exception(
                f"Failed to download image. Status code: {response.status_code}"
            )
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
