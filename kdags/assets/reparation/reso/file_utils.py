import os
import requests
from pathlib import Path
from .config import BASE_PATH


def save_file_locally(
    blob_url: str,
    target_folder: str,
    file_name: str,
    base_path: str = r"C:\Users\andmn\OneDrive - Komatsu Ltd\REPARACION",
):
    """
    Downloads a file from a blob URL and saves it to the local OneDrive folder.

    Parameters:
    blob_url (str): URL of the blob to download
    target_folder (str): Target folder path relative to the base OneDrive path
    file_name (str): Name of the file to save
    base_path (str): Base path of the OneDrive folder

    Returns:
    str: Full path of the saved file
    """
    try:
        # Create full target path
        full_target_path = Path(base_path) / target_folder

        # Create target directory if it doesn't exist
        os.makedirs(full_target_path, exist_ok=True)

        # Full file path
        file_path = full_target_path / file_name

        # Download and save the file
        response = requests.get(blob_url)
        response.raise_for_status()  # Raise an exception for bad status codes

        with open(file_path, "wb") as f:
            f.write(response.content)

        return str(file_path)

    except requests.exceptions.RequestException as e:
        print(f"Error downloading file from {blob_url}: {e}")
        raise
    except OSError as e:
        print(f"Error saving file to {file_path}: {e}")
        raise
