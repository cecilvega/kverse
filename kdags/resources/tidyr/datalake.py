from azure.storage.filedatalake import DataLakeServiceClient
import polars as pl
import os
import pandas as pd
from io import BytesIO
from urllib.parse import urlparse
from datetime import datetime, timedelta
import re
import dagster as dg
from azure.storage.blob import BlobClient
import requests


class DataLake:
    def __init__(self, context: dg.AssetExecutionContext = None):
        self.context_check = isinstance(context, dg.AssetExecutionContext)
        self.context = context
        self._conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        # Parse the connection string into a dictionary
        conn_dict = {
            k: v
            for k, v in (
                item.split("=", 1) for item in os.environ["AZURE_STORAGE_CONNECTION_STRING"].strip(";").split(";")
            )
        }

        # Construct the desired dictionary, raising KeyError if keys are missing
        self._storage_options = {
            "AZURE_STORAGE_ACCOUNT_NAME": conn_dict["AccountName"],
            "AZURE_STORAGE_ACCOUNT_KEY": conn_dict["AccountKey"],
        }

        self.client = DataLakeServiceClient.from_connection_string(self._conn_str)

    def _parse_az_path(self, az_path: str) -> tuple:

        if not az_path.startswith("az://"):
            raise ValueError(f"Invalid az_path format: {az_path}. Expected format: az://container/path")

        parsed = urlparse(az_path)
        container = parsed.netloc

        # Handle the path (remove leading slash)
        path = parsed.path
        if path.startswith("/"):
            path = path[1:]

        return container, path

    def get_file_system_client(self, az_path: str):
        container, _ = self._parse_az_path(az_path)
        return self.client.get_file_system_client(container)

    def get_blob_client(self, az_path: str):
        container, file_path = self._parse_az_path(az_path)
        return BlobClient.from_connection_string(
            conn_str=self._conn_str,
            container_name=container,
            blob_name=file_path,
        )

    def list_paths(self, az_path: str, recursive: bool = True) -> pl.DataFrame:

        container, path = self._parse_az_path(az_path)
        file_system_client = self.get_file_system_client(f"az://{container}")

        base_az_path = f"az://{container}/"

        if recursive:
            files = [
                {
                    "az_path": base_az_path + path.name,
                    "file_size": path.content_length,
                    "last_modified": path.last_modified,
                }
                for path in file_system_client.get_paths(path=path, recursive=recursive)
                if not path.is_directory
            ]
        else:
            files = [
                {
                    "az_path": base_az_path + path.name,
                    "file_size": path.content_length,
                    "last_modified": path.last_modified,
                }
                for path in file_system_client.get_paths(path=path, recursive=recursive)
            ]
        return pl.DataFrame(files)

    def read_bytes(self, az_path: str) -> bytes:

        container, file_path = self._parse_az_path(az_path)
        file_system_client = self.get_file_system_client(f"az://{container}")
        file_client = file_system_client.get_file_client(file_path)

        downloaded_data = file_client.download_file()
        return downloaded_data.readall()

    def read_tibble(self, az_path: str, include_az_path: bool = False, **kwargs) -> pl.DataFrame:

        if self.context_check:
            self.context.log.info(f"Reading data from Azure path: {az_path}")

        ext = az_path.split(".")[-1].lower()

        if ext == "parquet":
            df = pl.read_parquet(az_path, storage_options=self._storage_options, **kwargs)
        elif ext == "csv":
            df = pl.read_csv(az_path, storage_options=self._storage_options, **kwargs)
        elif ext in ["xlsx", "xls"]:
            file_content = self.read_bytes(az_path)
            buffer = BytesIO(file_content)
            df = pl.read_excel(buffer, **kwargs)
        else:
            raise ValueError(f"Unsupported file type: {ext}")
        if include_az_path and not df.is_empty():
            df = df.with_columns(pl.lit(az_path).alias("az_path"))

        if self.context_check and not df.is_empty():  # Check df is not None before accessing shape

            self.context.log.info(f"Successfully read data from: {az_path}")

            self.context.log.info(f"Data shape: {df.height} rows, {df.width} columns")
        elif self.context_check and df.is_empty():
            self.context.log.warning(f"Read operation for '{az_path}' resulted in a None DataFrame.")

        # container, file_path = self._parse_az_path(az_path)
        # # Get the file bytes
        # file_bytes = self.read_bytes(f"az://{container}/{file_path}")
        #
        # # Create BytesIO object
        # buffer = BytesIO(file_bytes)

        # Parse based on file type

        return df

    def upload_tibble(self, tibble, az_path: str, **kwargs) -> str:
        self.context_check = isinstance(self.context, dg.AssetExecutionContext)
        if self.context_check:
            self.context.log.info(f"Writing {tibble.height} rows, {tibble.width} columns to {az_path}")

        format = az_path.split(".")[-1].lower()

        # Convert DataFrame to bytes based on format
        if format.lower() == "parquet":
            if hasattr(tibble, "to_parquet"):
                tibble.to_parquet(az_path, storage_options=self._storage_options, **kwargs)
            else:
                tibble.write_parquet(az_path, storage_options=self._storage_options, **kwargs)

        elif format.lower() == "csv":
            if hasattr(tibble, "to_csv"):
                tibble.to_csv(az_path, storage_options=self._storage_options, **kwargs)
            else:
                tibble.write_csv(az_path, storage_options=self._storage_options, **kwargs)
            # tibble.write_csv(az_path, storage_options=self._storage_options, **kwargs).encode("utf-8")

        else:
            raise ValueError(f"Unsupported format: {format}")

        # Upload data with proper error handling
        # try:
        #     # Create or overwrite the file
        #     file_client.create_file()
        #     # Upload the data
        #     file_client.append_data(data, 0, len(data))
        #     # Flush to finalize the file
        #     file_client.flush_data(len(data))

        return az_path

    def upload_file(self, source_url: str, destination_az_path: str) -> str:

        # Download from HTTP URL
        response = requests.get(source_url)
        response.raise_for_status()
        file_content = response.content

        # Parse the destination az_path
        dest_container, dest_path = self._parse_az_path(destination_az_path)

        # Get a file system client for the destination container
        file_system_client = self.client.get_file_system_client(dest_container)

        # Ensure directory exists
        directory_path = "/".join(dest_path.split("/")[:-1])
        if directory_path:
            directory_client = file_system_client.get_directory_client(directory_path)
            directory_client.create_directory()

        # Get a file client for the destination path
        file_client = file_system_client.get_file_client(dest_path)

        # Create or overwrite the file
        file_client.create_file()

        # Upload the data
        file_client.append_data(file_content, 0, len(file_content))

        # Flush to finalize the file
        file_client.flush_data(len(file_content))

        return destination_az_path

    def az_path_exists(self, az_path: str) -> bool:

        try:
            container, file_path = self._parse_az_path(az_path)
            file_system_client = self.get_file_system_client(f"az://{container}")

            # If file_path is empty or ends with '/', treat it as a directory check
            if not file_path or file_path.endswith("/"):
                directory_client = file_system_client.get_directory_client(file_path.rstrip("/"))
                # Check if directory exists by getting properties (will raise exception if not found)
                directory_client.get_directory_properties()
                return True
            else:
                # Check if file exists by getting file client and properties
                file_client = file_system_client.get_file_client(file_path)
                file_client.get_file_properties()
                return True

        except Exception:
            # Any exception (typically ResourceNotFoundError) means the az_path doesn't exist
            return False

    def rename_file(self, source_az_path: str, destination_az_path: str) -> str:
        """
        Renames (moves) a file from a source Azure Data Lake path to a destination path
        within the same container.

        Args:
            source_az_path (str): The full Azure path of the source file (e.g., "az://container/path/to/source.parquet").
            destination_az_path (str): The full Azure path for the destination (e.g., "az://container/path/to/destination.parquet").

        Returns:
            str: The destination_az_path if successful.

        Raises:
            ValueError: If source and destination paths are not in the same container or format is invalid.
            Exception: Propagates errors from the Azure SDK during the rename operation.
        """
        if self.context_check:
            self.context.log.info(f"Attempting to rename '{source_az_path}' to '{destination_az_path}'")

        # Parse source and destination paths
        source_container, source_path = self._parse_az_path(source_az_path)
        dest_container, dest_path = self._parse_az_path(destination_az_path)

        # Get the file system client
        file_system_client = self.get_file_system_client(f"az://{source_container}")

        # Get the file client for the source file
        file_client = file_system_client.get_file_client(source_path)

        # Perform the rename operation
        # The new_name parameter requires the full path within the container
        renamed_file_client = file_client.rename_file(new_name=f"{dest_container}/{dest_path}")

        if self.context_check:
            self.context.log.info(f"Successfully renamed '{source_az_path}' to '{destination_az_path}'")

        # The rename_file method returns the new DataLakeFileClient,
        # confirming the operation was successful at the SDK level.
        # We return the requested destination path string for consistency.
        return destination_az_path

    def copy_file(self, source_az_path: str, destination_az_path: str) -> str:

        # Read the content from source
        file_content = self.read_bytes(source_az_path)

        # Parse the destination az_path
        dest_container, dest_path = self._parse_az_path(destination_az_path)

        # Get a file system client for the destination container
        file_system_client = self.client.get_file_system_client(dest_container)

        # Ensure directory exists
        directory_path = "/".join(dest_path.split("/")[:-1])
        if directory_path:
            directory_client = file_system_client.get_directory_client(directory_path)
            directory_client.create_directory(exist_ok=True)

        # Get a file client for the destination path
        file_client = file_system_client.get_file_client(dest_path)

        # Create or overwrite the file
        file_client.create_file()

        # Upload the data
        file_client.append_data(file_content, 0, len(file_content))

        # Flush to finalize the file
        file_client.flush_data(len(file_content))

        return destination_az_path

    def list_partitioned_paths(
        self, az_path: str, only_recent: bool = False, days_lookback: int = 30, cutoff_date: datetime = None
    ) -> pl.DataFrame:

        if cutoff_date is None:
            cutoff_date = datetime.now()

        # Get all files first
        all_files_df = self.list_paths(az_path, recursive=True)

        # If not filtering by recency or no files found, return as is
        if not only_recent or all_files_df.shape[0] == 0:
            return all_files_df

        # Calculate the cutoff date
        min_date = cutoff_date - timedelta(days=days_lookback)

        # Define common patterns for both monthly and daily partitions
        monthly_pattern = r"y=(\d{4})/m=(\d{2})"
        daily_pattern = r"y=(\d{4})/m=(\d{2})/d=(\d{2})"

        def extract_date_from_az_path(uri):
            # Try daily pattern first
            daily_match = re.search(daily_pattern, uri)
            if daily_match:
                year, month, day = map(int, daily_match.groups())
                return datetime(year, month, day)

            # Try monthly pattern next
            monthly_match = re.search(monthly_pattern, uri)
            if monthly_match:
                year, month = map(int, monthly_match.groups())
                # Use the first day of the month
                return datetime(year, month, 1)

            # If no pattern matches, return None
            return None

        # Create a new column with extracted dates
        dates = [extract_date_from_az_path(uri) for uri in all_files_df["az_path"].to_list()]
        date_series = pl.Series("partition_date", dates)
        all_files_df = all_files_df.with_columns([date_series])

        # Filter based on partition dates
        filtered_df = all_files_df.filter(
            (pl.col("partition_date").is_not_null())
            & (pl.col("partition_date") >= min_date)
            & (pl.col("partition_date") <= cutoff_date)
        )

        # Drop the temporary partition_date column if needed
        filtered_df = filtered_df.drop("partition_date")

        return filtered_df

    def delete_files(self, az_paths: list) -> dict:

        results = {"total": len(az_paths), "successful": 0, "failed": 0, "errors": []}

        for az_path in az_paths:
            try:
                container, file_path = self._parse_az_path(az_path)
                file_system_client = self.get_file_system_client(f"az://{container}")
                file_client = file_system_client.get_file_client(file_path)
                file_client.delete_file()
                results["successful"] += 1
            except Exception as e:
                results["failed"] += 1
                results["errors"].append({"az_path": az_path, "error": str(e)})

        return results
