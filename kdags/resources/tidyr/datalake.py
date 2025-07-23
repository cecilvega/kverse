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


def extract_partition_date(az_path: str) -> datetime:
    """Extract date from partition path"""
    match = re.search(r"y=(\d{4})/m=(\d{2})/d=(\d{2})", az_path)
    if match:
        year, month, day = map(int, match.groups())
        return datetime(year, month, day)
    return None


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

    def _manifest(self, manifest_path: str) -> pl.DataFrame:
        """Get manifest of all files with processing status"""
        if self.az_path_exists(manifest_path):
            return self.read_tibble(manifest_path)
        else:
            # Return empty DataFrame with expected schema
            return pl.DataFrame({"az_path": [], "file_size": [], "last_modified": [], "processed_at": []})

    def upsert_tibble(
        self,
        manifest_df: pl.DataFrame,
        analytics_df: pl.DataFrame,
        dedup_keys: list,
        date_column: str = "partition_date",
        add_partition_date: bool = True,
        cleaning_fn=None,
    ) -> (pl.DataFrame, pl.DataFrame):
        """
        Upsert (update/insert) data with deduplication

        Args:
            manifest_df: DataFrame with all files, where processed_at=null for unprocessed files
            analytics_df: Existing analytics data (can be None/empty)
            dedup_keys: Columns to use as unique keys for deduplication
            date_column: Column name for determining latest record
            add_partition_date: Whether to extract date from partition path

        Returns:
            Tuple of (deduplicated_analytics_df, updated_manifest_df)
        """

        # Find unprocessed files
        unprocessed_files = manifest_df.filter(pl.col("processed_at").is_null())

        if unprocessed_files.height == 0:
            # No new files to process
            if self.context:
                self.context.log.info("No unprocessed files found")
            return analytics_df if analytics_df is not None else pl.DataFrame(), manifest_df

        if self.context:
            self.context.log.info(f"Processing {unprocessed_files.height} new files")

        # Read ALL files (since each contains full history)
        all_tibbles = []
        processed_paths = []

        for row in manifest_df.to_dicts():
            try:

                tibble = self.read_tibble(row["az_path"])

                # Apply custom cleaning or default string cleaning
                if cleaning_fn:
                    tibble = cleaning_fn(tibble)

                # Add partition date if requested
                if add_partition_date:
                    match = re.search(r"y=(\d{4})/m=(\d{2})/d=(\d{2})", row["az_path"])
                    if match:
                        y, m, d = map(int, match.groups())
                        tibble = tibble.with_columns(pl.lit(datetime(y, m, d)).alias(date_column))

                all_tibbles.append(tibble)

                # Track successfully processed files
                if row["processed_at"] is None:
                    processed_paths.append(row["az_path"])

            except Exception as e:
                if self.context:
                    self.context.log.error(f"Failed to read {row['az_path']}: {str(e)}")
                continue

        # Combine all data
        if all_tibbles:
            combined_data = pl.concat(all_tibbles)

            # If we have existing analytics data, include it
            if analytics_df is not None and analytics_df.height > 0:
                combined_data = pl.concat([analytics_df, combined_data], how="diagonal")

            # Deduplicate: keep record with latest date_column value
            deduplicated_df = combined_data.sort(date_column, descending=True).unique(subset=dedup_keys, keep="first")

            if self.context:
                self.context.log.info(f"Deduplicated from {combined_data.height} to {deduplicated_df.height} rows")
        else:
            deduplicated_df = analytics_df if analytics_df is not None else pl.DataFrame()

        # Update manifest: set processed_at for newly processed files
        current_time = datetime.now()
        updated_manifest = manifest_df.with_columns(
            pl.when(pl.col("az_path").is_in(processed_paths))
            .then(pl.lit(current_time))
            .otherwise(pl.col("processed_at"))
            .alias("processed_at")
        )
        deduplicated_df = deduplicated_df.with_columns(date_column=pl.col(date_column).cast(pl.Date))
        return deduplicated_df, updated_manifest

    def prepare_manifest(self, manifest: pl.DataFrame, raw_path: str) -> pl.DataFrame:
        """
        Prepare manifest by comparing raw files with existing manifest
        Ensures all files are listed with their processing status
        """
        # Get all files from raw path
        all_files = self.list_paths(raw_path)

        if manifest.is_empty():
            manifest = pl.DataFrame({"az_path": [], "file_size": [], "last_modified": [], "processed_at": []})

        if manifest.height == 0:
            # No existing manifest, all files are unprocessed
            return all_files.with_columns(pl.lit(None).alias("processed_at"))

        # Left join to preserve all files, with processing status from manifest
        df = all_files.join(
            manifest.select(["az_path", "file_size", "processed_at"]), on=["az_path", "file_size"], how="left"
        )

        return df

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

    def read_tibble(
        self, az_path: str, raise_if_missing: bool = False, include_az_path: bool = False, **kwargs
    ) -> pl.DataFrame:

        # Check if file exists first when raise_if_missing is False
        if not raise_if_missing and not self.az_path_exists(az_path):
            if self.context_check:
                self.context.log.warning(f"File does not exist: {az_path}. Returning empty DataFrame.")
            return pl.DataFrame()

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

    def upload_bytes(self, data: bytes, az_path: str) -> str:
        """
        Upload bytes directly to Azure Data Lake Storage

        Args:
            data: Bytes to upload
            az_path: Destination path in format "az://container/path/to/file.ext"

        Returns:
            str: The destination az_path if successful
        """
        if self.context_check:
            self.context.log.info(f"Uploading {len(data)} bytes to {az_path}")

        # Parse the destination az_path
        container, file_path = self._parse_az_path(az_path)

        # Get file system client
        file_system_client = self.client.get_file_system_client(container)

        # Ensure directory exists
        directory_path = "/".join(file_path.split("/")[:-1])
        if directory_path:
            directory_client = file_system_client.get_directory_client(directory_path)
            directory_client.create_directory()

        # Get file client
        file_client = file_system_client.get_file_client(file_path)

        # Create or overwrite the file
        file_client.create_file()

        # Upload the data
        file_client.append_data(data, 0, len(data))

        # Flush to finalize the file
        file_client.flush_data(len(data))

        if self.context_check:
            self.context.log.info(f"Successfully uploaded to {az_path}")

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
        """
        Delete multiple files sequentially (no threading)

        Args:
            az_paths: List of Azure paths to delete

        Returns:
            dict: Results with counts and errors
        """
        results = {"total": len(az_paths), "successful": 0, "failed": 0, "errors": []}

        for az_path in az_paths:
            try:
                container, file_path = self._parse_az_path(az_path)
                file_system_client = self.get_file_system_client(f"az://{container}")
                file_client = file_system_client.get_file_client(file_path)
                file_client.delete_file()

                results["successful"] += 1

                if self.context_check:
                    self.context.log.info(f"Deleted: {az_path}")

            except Exception as e:
                results["failed"] += 1
                results["errors"].append({"az_path": az_path, "error": str(e)})

        return results

    def list_parallel_paths(
        self, az_path: str, only_recent: bool = False, days_lookback: int = 30, cutoff_date: datetime = None
    ) -> pl.DataFrame:
        """
        Optimized listing that's ALWAYS faster by listing year partitions in parallel
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        container, base_path = self._parse_az_path(az_path)
        file_system_client = self.get_file_system_client(f"az://{container}")

        # Step 1: Discover year partitions (fast - only top level)
        year_partitions = []
        try:
            for item in file_system_client.get_paths(path=base_path, recursive=False):
                if item.is_directory and item.name.split("/")[-1].startswith("y="):
                    try:
                        year = int(item.name.split("/")[-1].split("=")[1])
                        year_partitions.append((year, item.name))
                    except ValueError:
                        continue
        except Exception as e:
            # No partitions found, fall back to regular listing
            return self.list_paths(az_path, recursive=True)

        if not year_partitions:
            return self.list_paths(az_path, recursive=True)

        # Step 2: Filter years if only_recent
        if only_recent:
            if cutoff_date is None:
                cutoff_date = datetime.now()
            min_year = (cutoff_date - timedelta(days=days_lookback)).year
            year_partitions = [(y, p) for y, p in year_partitions if y >= min_year]

        if self.context_check:
            self.context.log.info(f"Listing {len(year_partitions)} year partitions in parallel")

        # Step 3: List each year partition in PARALLEL (this is the optimization!)
        def list_year_partition(year_info):
            year, year_path = year_info
            year_files = []
            try:
                # Create a new client for thread safety
                thread_client = DataLakeServiceClient.from_connection_string(self._conn_str)
                thread_fs_client = thread_client.get_file_system_client(container)

                for item in thread_fs_client.get_paths(path=year_path, recursive=True):
                    if not item.is_directory:
                        year_files.append(
                            {
                                "az_path": f"az://{container}/{item.name}",
                                "file_size": item.content_length,
                                "last_modified": item.last_modified,
                            }
                        )
            except Exception as e:
                if self.context_check:
                    self.context.log.warning(f"Error listing year {year}: {str(e)}")

            return year_files

        # Process all years in parallel
        all_files = []
        with ThreadPoolExecutor(max_workers=min(10, len(year_partitions))) as executor:
            futures = {executor.submit(list_year_partition, yp): yp[0] for yp in year_partitions}

            for future in as_completed(futures):
                year_files = future.result()
                all_files.extend(year_files)

        # Convert to DataFrame
        if not all_files:
            return pl.DataFrame({"az_path": [], "file_size": [], "last_modified": []})

        df = pl.DataFrame(all_files)

        # Apply date filter if needed
        if only_recent:
            min_date = cutoff_date - timedelta(days=days_lookback)
            df = df.filter((pl.col("last_modified") >= min_date) & (pl.col("last_modified") <= cutoff_date))

        return df

    def read_tibbles(self, az_paths: list, max_workers: int = 10, how: str = "diagonal") -> pl.DataFrame:
        """
        Read multiple tibbles in parallel and concatenate them

        Args:
            az_paths: List of Azure paths to read (or can be a DataFrame with 'az_path' column)
            max_workers: Number of parallel workers
            how: How to concatenate ('diagonal' or 'vertical')

        Returns:
            Concatenated DataFrame
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        # Handle input - convert DataFrame to list if needed
        if isinstance(az_paths, pl.DataFrame):
            az_paths = az_paths["az_path"].to_list()
        elif isinstance(az_paths, dict) and "az_path" in az_paths:
            az_paths = az_paths["az_path"]

        if not az_paths:
            return pl.DataFrame()

        # Function to read a single tibble with error handling
        def read_single_tibble(path):
            try:
                df = self.read_tibble(path)
                return df, path, None
            except Exception as e:
                return None, path, str(e)

        # Read all tibbles in parallel
        tibbles = []
        errors = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all read tasks
            future_to_path = {executor.submit(read_single_tibble, path): path for path in az_paths}

            # Collect results as they complete
            for future in as_completed(future_to_path):
                df, path, error = future.result()
                if df is not None and not df.is_empty():
                    tibbles.append(df)
                elif error:
                    errors.append({"path": path, "error": error})

        # # Log summary
        # if self.context_check:
        #     self.context.log.info(f"Successfully read {len(tibbles)}/{len(az_paths)} files")
        #     if errors:
        #         self.context.log.warning(f"Failed to read {len(errors)} files")

        # Concatenate all tibbles
        if tibbles:
            try:
                combined_df = pl.concat(tibbles, how=how)
                return combined_df
            except Exception as e:
                # if self.context_check:
                #     self.context.log.error(f"Failed to concatenate dataframes: {str(e)}")
                raise
        else:
            return pl.DataFrame()
