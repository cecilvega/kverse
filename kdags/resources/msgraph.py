import json
import os
from pathlib import Path
import pandas as pd
from office365.graph_client import GraphClient
import msal


class MSGraph:
    def __init__(self):
        self.client = GraphClient(self.acquire_token_func)

    def acquire_token_func(self):

        client_id = "d50ca740-c83f-4d1b-b616-12c519384f0c"
        scopes = ["Files.ReadWrite.All"]

        app = msal.PublicClientApplication(client_id=client_id, authority="https://login.microsoftonline.com/common")

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
