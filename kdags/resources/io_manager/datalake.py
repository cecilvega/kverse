from azure.storage.filedatalake import DataLakeServiceClient
import polars as pl
import os
import pandas as pd
from io import BytesIO
from urllib.parse import urlparse


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
            pl.DataFrame: DataFrame containing file information
        """
        container, path = self._parse_abfs_uri(uri)
        file_system_client = self.get_file_system_client(f"abfs://{container}")

        if recursive:
            files = [
                {
                    "file_path": path.name,
                    "file_size": path.content_length,
                    "last_modified": path.last_modified,
                }
                for path in file_system_client.get_paths(path=path, recursive=recursive)
                if not path.is_directory
            ]
        else:
            files = [
                {
                    "file_path": path.name,
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

    def read_tibble(self, uri: str, use_polars: bool = True, **kwargs) -> [pd.DataFrame, pl.DataFrame]:
        """
        Read a data file from Azure Data Lake and return as a DataFrame.

        Args:
            uri (str): URI in format abfs://container/path
            use_polars (bool): If True, return a polars DataFrame; otherwise return pandas DataFrame

        Returns:
            Union[pd.DataFrame, pl.DataFrame]: The data as a DataFrame

        Raises:
            ValueError: If file format is not supported
        """
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
