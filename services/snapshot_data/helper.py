"""Takes snapshots of data and stores it in external store."""

import os
import shutil

from lib.constants import root_local_data_directory, root_local_backup_data_directory
from lib.log.logger import get_logger

logger = get_logger(__name__)

excluded_cache_paths = [
    "__cache_perspective_api__",
    "__cache_sociopolitical__",
    "feed_analytics",
    "superposters",
    "user_session_logs",
    "preprocessed_data",
]


def migrate_directory_snapshot(relative_base_path: str) -> None:
    """Given a directory with 'active' and 'cache' subdirectories,
    copies the contents of the 'active' subdirectory to the 'cache' subdirectory.
    """
    current_active_directory = os.path.join(
        root_local_data_directory, relative_base_path, "active"
    )
    current_cache_directory = os.path.join(
        root_local_data_directory, relative_base_path, "cache"
    )

    new_active_directory = os.path.join(
        root_local_backup_data_directory, relative_base_path, "active"
    )
    new_cache_directory = os.path.join(
        root_local_backup_data_directory, relative_base_path, "cache"
    )

    if os.path.exists(current_active_directory):
        if not os.path.exists(new_active_directory):
            os.makedirs(new_active_directory)
        partition_dirs = os.listdir(current_active_directory)
        for partition_dir in partition_dirs:
            if not partition_dir.startswith("partition_date="):
                logger.info(
                    f"Skipping partition directory (probably old and uncompacted): {partition_dir}"
                )
                continue
            partition_dir_path = os.path.join(current_active_directory, partition_dir)
            filenames = os.listdir(partition_dir_path)
            new_partition_dir_path = os.path.join(new_active_directory, partition_dir)
            if not os.path.exists(new_partition_dir_path):
                os.makedirs(new_partition_dir_path)
            for filename in filenames:
                shutil.copy2(
                    src=os.path.join(partition_dir_path, filename),
                    dst=new_partition_dir_path,
                )
    else:
        logger.info(
            f"Current active directory does not exist: {current_active_directory}"
        )

    if os.path.exists(current_cache_directory):
        if not os.path.exists(new_cache_directory):
            os.makedirs(new_cache_directory)
        partition_dirs = os.listdir(current_cache_directory)
        for partition_dir in partition_dirs:
            partition_dir_path = os.path.join(current_cache_directory, partition_dir)
            filenames = os.listdir(partition_dir_path)
            new_partition_dir_path = os.path.join(new_cache_directory, partition_dir)
            if not os.path.exists(new_partition_dir_path):
                os.makedirs(new_partition_dir_path)
            for filename in filenames:
                shutil.copy2(
                    src=os.path.join(partition_dir_path, filename),
                    dst=new_partition_dir_path,
                )
    else:
        logger.info(
            f"Current cache directory does not exist: {current_cache_directory}"
        )


def snapshot_study_user_likes() -> None:
    relative_base_path = os.path.join("study_user_activity", "create", "like")
    migrate_directory_snapshot(relative_base_path)


def snapshot_study_user_posts() -> None:
    relative_base_path = os.path.join("study_user_activity", "create", "post")
    migrate_directory_snapshot(relative_base_path)


def snapshot_study_user_like_on_user_post() -> None:
    relative_base_path = os.path.join(
        "study_user_activity", "create", "like_on_user_post"
    )
    migrate_directory_snapshot(relative_base_path)


def snapshot_study_user_reply_to_user_post() -> None:
    relative_base_path = os.path.join(
        "study_user_activity", "create", "reply_to_user_post"
    )
    migrate_directory_snapshot(relative_base_path)


def snapshot_study_user_activity() -> None:
    """Takes snapshot of the data that came from the study user activity."""
    logger.info("-" * 10)
    logger.info("Snapshotting study user activity...")
    logger.info("Snapshotting study user likes...")
    snapshot_study_user_likes()
    logger.info("DONE: snapshotting study user likes")
    logger.info("Snapshotting study user posts...")
    snapshot_study_user_posts()
    logger.info("DONE: snapshotting study user posts")
    logger.info("Snapshotting study user like on user post...")
    snapshot_study_user_like_on_user_post()
    logger.info("DONE: snapshotting study user like on user post")
    logger.info("Snapshotting study user reply to user post...")
    snapshot_study_user_reply_to_user_post()
    logger.info("DONE: snapshotting study user reply to user post")
    logger.info("DONE: snapshotting study user activity")
    logger.info("-" * 10)


def snapshot_in_network_user_activity() -> None:
    """Takes snapshot of the data that came from the in-network user activity."""
    relative_base_path = "in_network_user_activity"
    migrate_directory_snapshot(relative_base_path)


def snapshot_firehose_data() -> None:
    """Takes snapshot of the data that came from the firehose."""
    logger.info("-" * 10)
    logger.info("Snapshotting firehose data...")
    logger.info("Snapshotting study user activity...")
    snapshot_study_user_activity()
    logger.info("DONE: snapshotting study user activity")
    logger.info("Snapshotting in-network user activity...")
    snapshot_in_network_user_activity()
    logger.info("DONE: snapshotting in-network user activity")
    logger.info("-" * 10)


def snapshot_most_liked_data() -> None:
    """Takes snapshot of the data that came from the most liked feed."""
    relative_path = "sync/most_liked"
    migrate_directory_snapshot(relative_path)


def snapshot_synced_data() -> None:
    """Takes snapshot of the data that came from the syncs."""
    logger.info("-" * 10)
    logger.info("Snapshotting firehose data...")
    snapshot_firehose_data()
    logger.info("DONE: snapshotting firehose data")
    logger.info("Snapshotting most liked data...")
    snapshot_most_liked_data()
    logger.info("DONE: snapshotting most liked data")
    logger.info("DONE: Snapshotting synced data.")
    logger.info("-" * 10)


def snapshot_preprocessed_posts() -> None:
    logger.info("-" * 10)
    logger.info("Snapshotting preprocessed posts...")
    logger.info("Snapshotting preprocessed posts... done")
    logger.info("-" * 10)


def snapshot_superposters() -> None:
    relative_path = "daily_superposters"
    migrate_directory_snapshot(relative_path)


def snapshot_ml_inference_perspective_api() -> None:
    firehose_relative_path = os.path.join("ml_inference_perspective_api", "firehose")
    most_liked_relative_path = os.path.join(
        "ml_inference_perspective_api", "most_liked"
    )
    migrate_directory_snapshot(firehose_relative_path)
    migrate_directory_snapshot(most_liked_relative_path)


def snapshot_ml_inference_sociopolitical() -> None:
    firehose_relative_path = os.path.join("ml_inference_sociopolitical", "firehose")
    most_liked_relative_path = os.path.join("ml_inference_sociopolitical", "most_liked")
    migrate_directory_snapshot(firehose_relative_path)
    migrate_directory_snapshot(most_liked_relative_path)


def snapshot_integrations() -> None:
    logger.info("-" * 10)
    logger.info("Snapshotting integrations data...")
    logger.info("Snapshotting superposters...")
    snapshot_superposters()
    logger.info("DONE: snapshotting superposters")
    logger.info("Snapshotting ml inference perspective api...")
    snapshot_ml_inference_perspective_api()
    logger.info("DONE: snapshotting ml inference perspective api")
    logger.info("Snapshotting ml inference sociopolitical...")
    snapshot_ml_inference_sociopolitical()
    logger.info("DONE: snapshotting ml inference sociopolitical")
    logger.info("DONE: Snapshotting integrations data.")
    logger.info("-" * 10)


def snapshot_consolidated_posts() -> None:
    logger.info("-" * 10)
    logger.info("Snapshotting consolidated posts...")
    relative_path = "consolidated_enriched_post_records"
    migrate_directory_snapshot(relative_path)
    logger.info("Snapshotting consolidated posts... done")
    logger.info("-" * 10)


def snapshot_data() -> None:
    """Takes a snapshot of the data in the backup data directory and stores it
    in the backup directory.
    """
    if not os.path.exists(root_local_backup_data_directory):
        os.makedirs(root_local_backup_data_directory)

    snapshot_synced_data()
    snapshot_preprocessed_posts()
    snapshot_integrations()
    snapshot_consolidated_posts()


if __name__ == "__main__":
    snapshot_data()
