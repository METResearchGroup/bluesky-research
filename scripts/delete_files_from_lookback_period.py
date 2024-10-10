"""Deletes files from a lookback period from a local storage."""

import os
from datetime import datetime, timezone, timedelta

from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from services.compact_all_services.helper import delete_empty_folders


def get_file_creation_time(filepath):
    ts = datetime.fromtimestamp(os.path.getctime(filepath), tz=timezone.utc).strftime(
        "%Y-%m-%d"
    )
    return ts


def delete_files_from_lookback_period(service: str, lookback_days: int):
    """Delete files from the last `lookback_days` days."""
    metadata = MAP_SERVICE_TO_METADATA[service]
    local_prefix = os.path.join(metadata["local_prefix"], "active")
    lookback_date = (
        datetime.now(timezone.utc) - timedelta(days=lookback_days)
    ).strftime("%Y-%m-%d")
    files_list = []
    for root, _, files in os.walk(local_prefix):
        for file in files:
            full_path = os.path.join(root, file)
            files_list.append(
                {"filepath": full_path, "timestamp": get_file_creation_time(full_path)}
            )
    total_files = len(files_list)
    files_to_delete = [file for file in files_list if file["timestamp"] > lookback_date]
    num_files_to_delete = len(files_to_delete)
    print(
        f"Found {num_files_to_delete} files to delete out of {total_files} total files."
    )
    for file in files_to_delete:
        os.remove(file["filepath"])
    delete_empty_folders(local_prefix)
    print(f"Deleted {num_files_to_delete} files.")


def main():
    services = [
        "study_user_activity",
        "sync_most_liked_posts",
        "daily_superposters",
        "consolidated_enriched_post_records",
        "ml_inference_sociopolitical",
        "in_network_user_activity",
        "scraped_user_social_network",
        "preprocessed_posts",
    ]
    lookback_days = 1
    for service in services:
        print(f"Deleting files for service: {service}")
        delete_files_from_lookback_period(service, lookback_days)
        print(f"Done deleting files for service: {service}")
    print("Done")


if __name__ == "__main__":
    main()
