"""Local filesystem cleanup after compaction."""

import os

from lib.db.manage_local_data import get_local_prefixes_for_service
from lib.log.logger import get_logger

logger = get_logger(__name__)


def delete_empty_folders(local_prefix: str) -> None:
    """Deletes empty folders from the local storage."""
    total_folders = 0
    total_folders_deleted = 0
    for root, dirs, _ in os.walk(local_prefix, topdown=False):
        for dir in dirs:
            dir_path = os.path.join(root, dir)
            if not os.listdir(dir_path):
                os.rmdir(dir_path)
                total_folders_deleted += 1
        total_folders += 1
    logger.info(
        f"Deleted {total_folders_deleted} empty folders out of {total_folders} total folders"
    )


def delete_empty_folders_for_service(service: str) -> None:
    local_prefixes = get_local_prefixes_for_service(service)
    for local_prefix in local_prefixes:
        delete_empty_folders(local_prefix)
