"""Compaction of local partitioned datasets (used by SLURM / pipeline handler)."""

from typing import Literal

import pandas as pd

from lib.constants import default_lookback_days
from lib.db.manage_local_data import (
    delete_files,
    export_data_to_local_storage,
    list_filenames,
    load_data_from_local_storage,
)
from lib.helper import track_performance
from lib.log.logger import get_logger

from services.compact_all_services.cleanup import delete_empty_folders_for_service
from services.compact_all_services.constants import default_export_format

logger = get_logger(__name__)

LOCAL_COMPACTION_SERVICE_NAMES: tuple[str, ...] = (
    "preprocessed_posts",
    "in_network_user_activity",
    "scraped_user_social_network",
    "study_user_activity",
    "study_user_likes",
    "study_user_like_on_user_post",
    "sync_most_liked_posts",
    "daily_superposters",
    "user_session_logs",
    "feed_analytics",
    "post_scores",
    "consolidated_enriched_post_records",
    "ml_inference_sociopolitical",
    "ml_inference_perspective_api",
)


@track_performance
def compact_local_service(
    service: str,
    export_format: Literal["json", "parquet"] = default_export_format,
    lookback_days: int = default_lookback_days,
    delete_old_files: bool = True,
) -> None:
    """Compacts the local data for a service.

    Loads the data from local storage and exports it to local storage. Then
    optionally (default=True) deletes old files from previous compaction
    sessions.
    """
    df: pd.DataFrame = load_data_from_local_storage(service)  # type: ignore
    filenames: list[str] = list_filenames(service)  # type: ignore

    if service in [
        "preprocessed_posts",
        "ml_inference_perspective_api",
        "ml_inference_sociopolitical",
    ]:
        if len(df) == 0:
            logger.warning(f"No data found for service={service}")
            return
        grouped = df.groupby("source")
        firehose_df = grouped.get_group("firehose")
        most_liked_df = grouped.get_group("most_liked")
        export_data_to_local_storage(
            service=service,
            df=firehose_df,
            custom_args={"source": "firehose"},  # type: ignore
        )
        export_data_to_local_storage(
            service=service,
            df=most_liked_df,
            custom_args={"source": "most_liked"},  # type: ignore
        )
    elif service == "study_user_activity":
        export_data_to_local_storage(
            service=service, df=df, custom_args={"record_type": "post"}
        )
    else:
        export_data_to_local_storage(
            service=service,
            df=df,
            export_format=export_format,  # type: ignore
            lookback_days=lookback_days,
        )
    if delete_old_files:
        logger.info(
            f"Deleting {len(filenames)} files from local storage for service={service}"
        )
        delete_files(filenames)
        delete_empty_folders_for_service(service)


def compact_all_local_services() -> None:
    for service in LOCAL_COMPACTION_SERVICE_NAMES:
        compact_local_service(service, delete_old_files=True)


if __name__ == "__main__":
    compact_all_local_services()
