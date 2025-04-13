from azure.storage.filedatalake import DataLakeServiceClient
import polars as pl
import os
import pandas as pd
from io import BytesIO
from urllib.parse import urlparse
from datetime import datetime, timedelta
import re


class DataLake:
    def __init__(self):
        conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        # Parse the connection string into a dictionary
        conn_dict = {
            k: v
            for k, v in (
                item.split("=", 1) for item in os.environ["AZURE_STORAGE_CONNECTION_STRING"].strip(";").split(";")
            )
        }

        # Construct the desired dictionary, raising KeyError if keys are missing
        self.storage_options = {
            "AZURE_STORAGE_ACCOUNT_NAME": conn_dict["AccountName"],
            "AZURE_STORAGE_ACCOUNT_KEY": conn_dict["AccountKey"],
        }
        self.client = DataLakeServiceClient.from_connection_string(conn_str)

    def _parse_abfs_path(self, abfs_path: str) -> tuple:

        if not abfs_path.startswith("az://"):
            raise ValueError(f"Invalid abfs_path format: {abfs_path}. Expected format: abfs://container/path")

        parsed = urlparse(abfs_path)
        container = parsed.netloc

        # Handle the path (remove leading slash)
        path = parsed.path
        if path.startswith("/"):
            path = path[1:]

        return container, path

    def get_file_system_client(self, abfs_path: str):
        container, _ = self._parse_abfs_path(abfs_path)
        return self.client.get_file_system_client(container)

    def list_paths(self, abfs_path: str, recursive: bool = True) -> pl.DataFrame:

        container, path = self._parse_abfs_path(abfs_path)
        file_system_client = self.get_file_system_client(f"abfs://{container}")

        base_abfs_path = f"abfs://{container}/"

        if recursive:
            files = [
                {
                    "abfs_path": base_abfs_path + path.name,
                    "file_size": path.content_length,
                    "last_modified": path.last_modified,
                }
                for path in file_system_client.get_paths(path=path, recursive=recursive)
                if not path.is_directory
            ]
        else:
            files = [
                {
                    "abfs_path": base_abfs_path + path.name,
                    "file_size": path.content_length,
                    "last_modified": path.last_modified,
                }
                for path in file_system_client.get_paths(path=path, recursive=recursive)
            ]
        return pl.DataFrame(files)

    def read_bytes(self, abfs_path: str) -> bytes:

        container, file_path = self._parse_abfs_path(abfs_path)
        file_system_client = self.get_file_system_client(f"abfs://{container}")
        file_client = file_system_client.get_file_client(file_path)

        downloaded_data = file_client.download_file()
        return downloaded_data.readall()

    def read_tibble(
        self, abfs_path: str, use_polars: bool = True, include_abfs_path: bool = False, **kwargs
    ) -> [pd.DataFrame, pl.DataFrame]:

        try:
            container, file_path = self._parse_abfs_path(abfs_path)

            # Get the file bytes
            file_bytes = self.read_bytes(f"abfs://{container}/{file_path}")

            # Determine file type from extension
            file_ext = file_path.split(".")[-1].lower()

            # Create BytesIO object
            buffer = BytesIO(file_bytes)

            # Parse based on file type
            if file_ext == "parquet":
                if use_polars:
                    df = pl.read_parquet(buffer, **kwargs)
                else:
                    df = pd.read_parquet(buffer, **kwargs)

            elif file_ext == "csv":
                if use_polars:
                    df = pl.read_csv(buffer, **kwargs)
                else:
                    df = pd.read_csv(buffer, **kwargs)

            elif file_ext in ["xlsx", "xls"]:
                if use_polars:
                    df = pl.read_excel(buffer, **kwargs)
                else:
                    df = pd.read_excel(buffer, **kwargs)

            elif file_ext == "json":
                if use_polars:
                    df = pl.read_json(buffer, **kwargs)
                else:
                    df = pd.read_json(buffer, **kwargs)

            else:
                raise ValueError(f"Unsupported file format: .{file_ext}")

            if include_abfs_path and df is not None:
                if use_polars:
                    df = df.with_columns(pl.lit(abfs_path).alias("abfs_path"))
                else:
                    df["abfs_path"] = abfs_path

            return df

        except Exception as e:
            error_message = f"Error reading file from {abfs_path}: {str(e)}"
            # Raise a new exception with the enhanced message but preserve the original exception type
            raise type(e)(error_message) from e

    def upload_tibble(self, df, abfs_path: str, format: str = "parquet", **kwargs) -> str:

        container, file_path = self._parse_abfs_path(abfs_path)
        file_system_client = self.get_file_system_client(f"abfs://{container}")
        file_client = file_system_client.get_file_client(file_path)

        # Convert DataFrame to bytes based on format
        if format.lower() == "parquet":
            buffer = BytesIO()
            # Handle both pandas and polars DataFrames
            if hasattr(df, "to_pandas"):  # It's a polars DataFrame
                df.write_parquet(buffer, **kwargs)
            else:  # Assume it's pandas
                df.to_parquet(buffer, **kwargs)
            buffer.seek(0)
            data = buffer.getvalue()

        elif format.lower() == "csv":
            # Handle both pandas and polars DataFrames
            if hasattr(df, "to_pandas"):  # It's a polars DataFrame
                data = df.write_csv(**kwargs).encode("utf-8")
            else:  # Assume it's pandas
                data = df.to_csv(**kwargs).encode("utf-8")

        else:
            raise ValueError(f"Unsupported format: {format}")

        # Upload data with proper error handling
        try:
            # Create or overwrite the file
            file_client.create_file()
            # Upload the data
            file_client.append_data(data, 0, len(data))
            # Flush to finalize the file
            file_client.flush_data(len(data))

            return abfs_path
        except Exception as e:
            raise ValueError(f"Error uploading DataFrame to {abfs_path}: {str(e)}")

    def abfs_path_exists(self, abfs_path: str) -> bool:
        """
        Check if a file exists at the specified abfs_path in Azure Data Lake.

        Args:
            abfs_path (str): abfs_path in format abfs://container/path

        Returns:
            bool: True if the file exists, False otherwise
        """
        try:
            container, file_path = self._parse_abfs_path(abfs_path)
            file_system_client = self.get_file_system_client(f"abfs://{container}")

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
            # Any exception (typically ResourceNotFoundError) means the abfs_path doesn't exist
            return False

    def copy_file(self, source_abfs_path: str, destination_abfs_path: str) -> str:

        # Read the content from source
        file_content = self.read_bytes(source_abfs_path)

        # Parse the destination abfs_path
        dest_container, dest_path = self._parse_abfs_path(destination_abfs_path)

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

        return destination_abfs_path

    def list_partitioned_paths(
        self, abfs_path: str, only_recent: bool = False, days_lookback: int = 30, cutoff_date: datetime = None
    ) -> pl.DataFrame:

        if cutoff_date is None:
            cutoff_date = datetime.now()

        # Get all files first
        all_files_df = self.list_paths(abfs_path, recursive=True)

        # If not filtering by recency or no files found, return as is
        if not only_recent or all_files_df.shape[0] == 0:
            return all_files_df

        # Calculate the cutoff date
        min_date = cutoff_date - timedelta(days=days_lookback)

        # Define common patterns for both monthly and daily partitions
        monthly_pattern = r"y=(\d{4})/m=(\d{2})"
        daily_pattern = r"y=(\d{4})/m=(\d{2})/d=(\d{2})"

        def extract_date_from_abfs_path(uri):
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
        dates = [extract_date_from_abfs_path(uri) for uri in all_files_df["abfs_path"].to_list()]
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

    def delete_files(self, abfs_paths: list) -> dict:

        results = {"total": len(abfs_paths), "successful": 0, "failed": 0, "errors": []}

        for abfs_path in abfs_paths:
            try:
                container, file_path = self._parse_abfs_path(abfs_path)
                file_system_client = self.get_file_system_client(f"abfs://{container}")
                file_client = file_system_client.get_file_client(file_path)
                file_client.delete_file()
                results["successful"] += 1
            except Exception as e:
                results["failed"] += 1
                results["errors"].append({"abfs_path": abfs_path, "error": str(e)})

        return results
