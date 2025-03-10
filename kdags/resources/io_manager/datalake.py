from azure.storage.filedatalake import DataLakeServiceClient
import polars as pl
import os
import pandas as pd
from io import BytesIO


class DataLake:
    def __init__(self):
        conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        self.client = DataLakeServiceClient.from_connection_string(conn_str)

    # def __init__(self):
    #     account_url = "https://kchdatalakedes.dfs.core.windows.net"
    #     sas_token = os.environ["AZURE_SAS_TOKEN"]
    #     self.client = DataLakeServiceClient(account_url=account_url, credential=sas_token)
    #     self.base_path = "OPERACIONES/CHILE/KCH"

    def get_file_system_client(self, container: str):
        return self.client.get_file_system_client(container)

    def list_paths(self, container: str, file_path: str, recursive: bool = True) -> pl.DataFrame:

        file_system_client = self.get_file_system_client(container)
        if recursive:

            files = [
                {
                    "file_path": path.name,
                    "file_size": path.content_length,
                    "last_modified": path.last_modified,
                }
                for path in file_system_client.get_paths(path=file_path, recursive=recursive)
                if not path.is_directory
            ]
        else:
            files = [
                {
                    "file_path": path.name,
                    "file_size": path.content_length,
                    "last_modified": path.last_modified,
                }
                for path in file_system_client.get_paths(path=file_path, recursive=recursive)
            ]
        return pl.DataFrame(files)

    def read_bytes(self, container: str, file_path: str) -> bytes:
        """Download a file's contents as bytes"""

        file_system_client = self.get_file_system_client(container)
        file_client = file_system_client.get_file_client(file_path)

        downloaded_data = file_client.download_file()
        return downloaded_data.readall()

    def read_tibble(
        self, container: str, file_path: str, use_polars: bool = True, **kwargs
    ) -> [pd.DataFrame, pl.DataFrame]:
        """
        Read a data file from Azure Data Lake and return as a DataFrame.

        Args:
            container (str): Azure container name
            file_path (str): Path to the file within the container
            use_polars (bool): If True, return a polars DataFrame; otherwise return pandas DataFrame

        Returns:
            Union[pd.DataFrame, pl.DataFrame]: The data as a DataFrame

        Raises:
            ValueError: If file format is not supported
        """
        # Get the file bytes
        file_bytes = self.read_bytes(container, file_path)

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

    def upload_tibble(self, container: str, file_path: str, df, format: str = "parquet", **kwargs) -> str:
        """
        Upload a DataFrame to Data Lake in the specified format.

        Args:
            container (str): Container name
            file_path (str): Target file path within the container
            df: DataFrame to upload (pandas or polars)
            format (str): Output format ('parquet', 'csv', 'json')
            **kwargs: Additional arguments passed to the serialization function

        Returns:
            str: Full path to the uploaded file
        """
        from io import BytesIO

        file_system_client = self.get_file_system_client(container)
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

            return file_path
        except Exception as e:
            raise ValueError(f"Error uploading DataFrame to {file_path}: {str(e)}")
