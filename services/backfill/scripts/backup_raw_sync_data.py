"""Backs up the raw_sync data."""

import os
import shutil

from lib.constants import root_local_data_directory

raw_sync_directory = os.path.join(root_local_data_directory, "raw_sync")
temp_raw_sync_directory = os.path.join(root_local_data_directory, "temp_raw_sync")


def main():
    """Main function to backup the raw_sync data."""
    # Create the destination directory if it doesn't exist
    if not os.path.exists(temp_raw_sync_directory):
        os.makedirs(temp_raw_sync_directory)

    # Copy all data from raw_sync_directory to temp_raw_sync_directory
    for item in os.listdir(raw_sync_directory):
        source_path = os.path.join(raw_sync_directory, item)
        dest_path = os.path.join(temp_raw_sync_directory, item)

        if os.path.isdir(source_path):
            shutil.copytree(source_path, dest_path, dirs_exist_ok=True)
        else:
            shutil.copy2(source_path, dest_path)

    print(
        f"Successfully backed up data from {raw_sync_directory} to {temp_raw_sync_directory}"
    )


if __name__ == "__main__":
    main()
