from azure.storage.filedatalake import DataLakeServiceClient
import polars as pl
import os


class DataLake:
    def __init__(self):
        account_url = "https://kchdatalakedes.dfs.core.windows.net"
        sas_token = os.environ["AZURE_SAS_TOKEN"]
        self.client = DataLakeServiceClient(account_url=account_url, credential=sas_token)
        self.base_path = "OPERACIONES/CHILE/KCH/BHP"

    def get_file_system_client(self, container: str):
        return self.client.get_file_system_client(container)

    def list_paths(self, container: str, file_path: str, recursive: bool = True) -> pl.DataFrame:
        file_path = f"{self.base_path}/{file_path}"
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

    # Add other file/directory operations as needed

    # def read_bytes(self, path: str) -> bytes:
    #     """Read raw bytes from file"""
    #
    # def read_text(self, path: str, encoding: str = "utf-8") -> str:
    #     """Read file as text"""
    #
    # def read_json(self, path: str) -> dict:
    #     """Read JSON file into dictionary"""
    #
    # def read_parquet(self, path: str) -> pd.DataFrame:
    #     """Read Parquet file into DataFrame"""
    #
    # def read_csv(self, path: str, **kwargs) -> pd.DataFrame:
    #     """Read CSV file into DataFrame"""
    #
    # def read_excel(self, path: str, **kwargs) -> pd.DataFrame:
    #     """Read Excel file into DataFrame"""
    #
    # def list_files(self, path: str, pattern: str = "*") -> List[str]:
    #     """List all files in path"""
    #
    # def list_directories(self, path: str) -> List[str]:
    #     """List all directories in path"""
    #
    # def search(self, pattern: str, recursive: bool = True) -> List[str]:
    #     """Search for files matching pattern"""
    #
    # def upload_bytes(self, path: str, data: bytes) -> None:
    #     """Upload raw bytes"""
    #
    # def upload_text(self, path: str, text: str, encoding: str = 'utf-8') -> None:
    #     """Upload text content"""
    #
    # def upload_dataframe(self, path: str, df: pd.DataFrame, format: str = 'parquet') -> None:
    #     """Upload DataFrame in specified format"""
    #
    # def upload_file(self, path: str, local_path: str) -> None:
    #     """Upload local file"""
    #
    # def upload_batch(self, files: Dict[str, Union[str, bytes, pd.DataFrame]]) -> None:
    #     """Upload multiple files"""
