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

        self.client = DataLakeServiceClient.from_connection_string(conn_str)

    def _parse_abfs_uri(self, uri: str) -> tuple:
        """
        Parse an ABFS URI into container and path components.

        Args:
            uri (str): URI in the format abfs://container/path

        Returns:
            tuple: (container, path)
        """
        if not uri.startswith("abfs://"):
            raise ValueError(f"Invalid ABFS URI format: {uri}. Expected format: abfs://container/path")

        # Parse the URI
        parsed = urlparse(uri)
        container = parsed.netloc

        # Handle the path (remove leading slash)
        path = parsed.path
        if path.startswith("/"):
            path = path[1:]

        return container, path

    def get_file_system_client(self, uri: str):
        container, _ = self._parse_abfs_uri(uri)
        return self.client.get_file_system_client(container)

    def list_paths(self, uri: str, recursive: bool = True) -> pl.DataFrame:
        """
        List files at the specified path.

        Args:
            uri (str): URI in format abfs://container/path
            recursive (bool): Whether to list files recursively

        Returns:
            pl.DataFrame: DataFrame containing file information with URIs
        """
        container, path = self._parse_abfs_uri(uri)
        file_system_client = self.get_file_system_client(f"abfs://{container}")

        base_uri = f"abfs://{container}/"

        if recursive:
            files = [
                {
                    "uri": base_uri + path.name,
                    "file_size": path.content_length,
                    "last_modified": path.last_modified,
                }
                for path in file_system_client.get_paths(path=path, recursive=recursive)
                if not path.is_directory
            ]
        else:
            files = [
                {
                    "uri": base_uri + path.name,
                    "file_size": path.content_length,
                    "last_modified": path.last_modified,
                }
                for path in file_system_client.get_paths(path=path, recursive=recursive)
            ]
        return pl.DataFrame(files)

    def read_bytes(self, uri: str) -> bytes:
        """
        Download a file's contents as bytes.

        Args:
            uri (str): URI in format abfs://container/path

        Returns:
            bytes: File content
        """
        container, file_path = self._parse_abfs_uri(uri)
        file_system_client = self.get_file_system_client(f"abfs://{container}")
        file_client = file_system_client.get_file_client(file_path)

        downloaded_data = file_client.download_file()
        return downloaded_data.readall()

    def read_tibble(
        self, uri: str, use_polars: bool = True, include_uri: bool = False, **kwargs
    ) -> [pd.DataFrame, pl.DataFrame]:
        """
        Read a data file from Azure Data Lake and return as a DataFrame.

        Args:
            uri (str): URI in format abfs://container/path
            use_polars (bool): If True, return a polars DataFrame; otherwise return pandas DataFrame
            include_uri (bool): If True, include the URI as a column in the DataFrame
            **kwargs: Additional arguments passed to the appropriate pandas read function

        Returns:
            Union[pd.DataFrame, pl.DataFrame]: The data as a DataFrame

        Raises:
            ValueError: If file format cannot be determined or is not supported
        """
        try:
            container, file_path = self._parse_abfs_uri(uri)

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

            # Add URI column if requested
            if include_uri and df is not None:
                if use_polars:
                    df = df.with_columns(pl.lit(uri).alias("uri"))
                else:
                    df["uri"] = uri

            return df

        except Exception as e:
            # Enhance the error message with the URI
            error_message = f"Error reading file from {uri}: {str(e)}"
            # Raise a new exception with the enhanced message but preserve the original exception type
            raise type(e)(error_message) from e

    def upload_tibble(self, uri: str, df, format: str = "parquet", **kwargs) -> str:
        """
        Upload a DataFrame to Data Lake in the specified format.

        Args:
            uri (str): URI in format abfs://container/path
            df: DataFrame to upload (pandas or polars)
            format (str): Output format ('parquet', 'csv', 'json')
            **kwargs: Additional arguments passed to the serialization function

        Returns:
            str: Full path to the uploaded file
        """
        container, file_path = self._parse_abfs_uri(uri)
        file_system_client = self.get_file_system_client(f"abfs://{container}")
        file_client = file_system_client.get_file_client(file_path)

        # Convert DataFrame to bytes based on format
        if format.lower() == "parquet":
            buffer = BytesIO()
            # Handle both pandas and polars DataFrames
            if hasattr(df, "to_pandas"):  # It's a polars DataFrame
                df.to_pandas().to_parquet(buffer, **kwargs)
            else:  # Assume it's pandas
                df.to_parquet(buffer, **kwargs)
            buffer.seek(0)
            data = buffer.getvalue()

        elif format.lower() == "csv":
            # Handle both pandas and polars DataFrames
            if hasattr(df, "to_pandas"):  # It's a polars DataFrame
                data = df.to_pandas().to_csv(**kwargs).encode("utf-8")
            else:  # Assume it's pandas
                data = df.to_csv(**kwargs).encode("utf-8")

        elif format.lower() == "json":
            # Handle both pandas and polars DataFrames
            if hasattr(df, "to_pandas"):  # It's a polars DataFrame
                data = df.to_pandas().to_json(**kwargs).encode("utf-8")
            else:  # Assume it's pandas
                data = df.to_json(**kwargs).encode("utf-8")

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

            return uri
        except Exception as e:
            raise ValueError(f"Error uploading DataFrame to {uri}: {str(e)}")

    def copy_file(self, source_uri: str, destination_uri: str) -> str:
        """
        Copy a file from source URI to destination URI within the Data Lake.

        Args:
            source_uri (str): Source URI in format abfs://container/path
            destination_uri (str): Destination URI in format abfs://container/path

        Returns:
            str: Full path to the destination file

        Raises:
            ValueError: If file cannot be copied
        """

        # Read the content from source
        file_content = self.read_bytes(source_uri)

        # Parse the destination URI
        dest_container, dest_path = self._parse_abfs_uri(destination_uri)

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

        return destination_uri

    def list_partitioned_paths(
        self, uri: str, only_recent: bool = False, days_lookback: int = 30, cutoff_date: datetime = None
    ) -> pl.DataFrame:
        """
        List files in a partitioned data lake structure with intelligent filtering by date.

        Lists all files first, then efficiently filters based on partition dates extracted
        from the URI paths. Works with both monthly (y=%Y/m=%m) and daily (y=%Y/m=%m/d=%d) partitions.

        Args:
            uri (str): URI in format abfs://container/path
            only_recent (bool): If True, only include files from recent partitions
            days_lookback (int): Number of days to look back when only_recent is True
            cutoff_date (datetime): Reference date for lookback calculation, defaults to current date

        Returns:
            pl.DataFrame: DataFrame containing file information with URIs
        """
        if cutoff_date is None:
            cutoff_date = datetime.now()

        # Get all files first
        all_files_df = self.list_paths(uri, recursive=True)

        # If not filtering by recency or no files found, return as is
        if not only_recent or all_files_df.shape[0] == 0:
            return all_files_df

        # Calculate the cutoff date
        min_date = cutoff_date - timedelta(days=days_lookback)

        # Extract dates from URIs
        # Define common patterns for both monthly and daily partitions
        monthly_pattern = r"y=(\d{4})/m=(\d{2})"
        daily_pattern = r"y=(\d{4})/m=(\d{2})/d=(\d{2})"

        # Extract partition dates from URIs
        def extract_date_from_uri(uri_str):
            # Try daily pattern first
            daily_match = re.search(daily_pattern, uri_str)
            if daily_match:
                year, month, day = map(int, daily_match.groups())
                return datetime(year, month, day)

            # Try monthly pattern next
            monthly_match = re.search(monthly_pattern, uri_str)
            if monthly_match:
                year, month = map(int, monthly_match.groups())
                # Use the first day of the month
                return datetime(year, month, 1)

            # If no pattern matches, return None
            return None

        # Create a new column with extracted dates
        dates = [extract_date_from_uri(uri_str) for uri_str in all_files_df["uri"].to_list()]
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

    def delete_files(self, uris: list) -> dict:
        """
        Delete multiple files from the Data Lake.

        Args:
            uris (list): List of URIs to delete in format abfs://container/path

        Returns:
            dict: Summary of deletion operation with success and error counts
        """
        results = {"total": len(uris), "successful": 0, "failed": 0, "errors": []}

        for uri in uris:
            try:
                container, file_path = self._parse_abfs_uri(uri)
                file_system_client = self.get_file_system_client(f"abfs://{container}")
                file_client = file_system_client.get_file_client(file_path)
                file_client.delete_file()
                results["successful"] += 1
            except Exception as e:
                results["failed"] += 1
                results["errors"].append({"uri": uri, "error": str(e)})

        return results
