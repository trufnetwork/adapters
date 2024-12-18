import os
import shutil
import zipfile
from typing import Optional


def extract_zip(zip_path: str, dest_path: str, overwrite: bool = False) -> None:
    """
    Extracts a ZIP file to a destination directory.

    Args:
        zip_path: Path to the ZIP file.
        dest_path: Path to the destination directory.
        overwrite: Whether to overwrite the destination directory if it exists.
    """
    if os.path.exists(dest_path) and not overwrite:
        return  # Already extracted

    # Clear destination directory if it exists and overwrite is True
    if os.path.exists(dest_path):
        for item in os.listdir(dest_path):
            item_path = os.path.join(dest_path, item)
            if os.path.isfile(item_path):
                os.remove(item_path)
            else:
                shutil.rmtree(item_path)

    os.makedirs(dest_path, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(dest_path)

    # Extract inner ZIP files
    for file in os.listdir(dest_path):
        if file.endswith(".zip"):
            inner_zip_path = os.path.join(dest_path, file)
            inner_dest_path = os.path.join(dest_path, file.replace(".zip", ""))
            with zipfile.ZipFile(inner_zip_path, "r") as inner_zip_ref:
                inner_zip_ref.extractall(inner_dest_path) 