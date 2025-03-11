import pandas as pd

from lib.db.queue import Queue
from lib.constants import timestamp_format, partition_date_format

queue_name_to_queue_dict = {
    "ml_inference_ime": Queue(
        queue_name="input_ml_inference_ime", create_new_queue=False
    ),
    "ml_inference_perspective_api": Queue(
        queue_name="input_ml_inference_perspective_api", create_new_queue=False
    ),
    "ml_inference_sociopolitical": Queue(
        queue_name="input_ml_inference_sociopolitical", create_new_queue=False
    ),
}


def insert_missing_posts_to_backfill_queue(
    integration_posts_to_backfill_df: pd.DataFrame, queue_name: str
) -> None:
    """For any posts that are missing labels, re-insert them into the queue."""
    if pd.api.types.is_datetime64_any_dtype(
        integration_posts_to_backfill_df["preprocessing_timestamp"]
    ):
        integration_posts_to_backfill_df["preprocessing_timestamp"] = (
            integration_posts_to_backfill_df[
                "preprocessing_timestamp"
            ].dt.strftime(timestamp_format)
        )
    else:
        integration_posts_to_backfill_df["preprocessing_timestamp"] = (
            integration_posts_to_backfill_df["preprocessing_timestamp"].astype(str)
        )

    if pd.api.types.is_datetime64_any_dtype(
        integration_posts_to_backfill_df["partition_date"]
    ):
        integration_posts_to_backfill_df["partition_date"] = (
            integration_posts_to_backfill_df[
                "partition_date"
            ].dt.strftime(partition_date_format)
        )
    else:
        integration_posts_to_backfill_df["partition_date"] = (
            integration_posts_to_backfill_df["partition_date"].astype(str)
        )

    items = integration_posts_to_backfill_df.to_dict(orient="records")
    queue = queue_name_to_queue_dict[queue_name]
    queue.batch_add_items_to_queue(items=items, metadata=None)
