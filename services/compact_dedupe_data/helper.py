"""Helper functions for compact dedupe data."""

from datetime import datetime, timedelta, timezone
import os
from typing import Optional

import pandas as pd

from lib.aws.athena import Athena
from lib.aws.dynamodb import DynamoDB
from lib.aws.glue import Glue
from lib.aws.s3 import S3, ROOT_BUCKET
from lib.constants import timestamp_format
from lib.helper import generate_current_datetime_str, track_performance
from lib.log.logger import get_logger
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel

athena = Athena()
dynamodb = DynamoDB()
glue = Glue()
s3 = S3()

dynamodb_table_name = "compaction_sessions"
logger = get_logger(__name__)

pd.set_option("display.max_columns", None)
pd.set_option("max_colwidth", None)


def get_latest_compaction_session() -> Optional[dict]:
    """Get the latest compaction session from the DynamoDB table."""
    try:
        sessions: list[dict] = dynamodb.get_all_items_from_table(
            table_name=dynamodb_table_name
        )  # noqa
        if not sessions:
            logger.info("No enrichment consolidation sessions found.")
            return None
        sorted_sessions = sorted(
            sessions,
            key=lambda x: x.get("compaction_timestamp", {}).get("S", ""),
            reverse=True,
        )  # noqa
        return sorted_sessions[0]
    except Exception as e:
        logger.error(f"Failed to get latest compaction session: {e}")
        raise


def insert_compaction_session(compaction_session: dict):
    """Inserts a compaction session into the DynamoDB table."""
    try:
        dynamodb.insert_item_into_table(
            item=compaction_session, table_name=dynamodb_table_name
        )
        logger.info(
            f"Successfully inserted compaction session: {compaction_session}"  # noqa
        )  # noqa
    except Exception as e:
        logger.error(f"Failed to insert compaction session: {e}")  # noqa
        raise


# TODO: at some point, I should split up the compressed files tbh
# into files of size X (not sure if I will get to that scale).
def export_preprocessed_posts(
    posts: list[FilteredPreprocessedPostModel],
    timestamp: Optional[str] = None,
):
    """Exports the deduped and compacted preprocessed posts to S3."""
    partition_key = S3.create_partition_key_based_on_timestamp(
        timestamp_str=timestamp or generate_current_datetime_str()
    )
    filename = "preprocessed_posts.jsonl"
    full_key = os.path.join(
        "preprocessed_data",
        "preprocessed_posts",
        "preprocessing_source=preprocessed_compressed_deduped_posts",
        partition_key,
        filename,
    )  # noqa
    post_dicts = [post.dict() for post in posts]
    s3.write_dicts_jsonl_to_s3(data=post_dicts, key=full_key)
    logger.info(f"Exported deduped and compacted preprocessed posts to {full_key}")  # noqa


def get_existing_keys(fetch_all_keys: bool = False) -> list[str]:
    """Get existing keys from preprocessing.

    Excludes compacted files by default.
    """
    res: list[str] = []
    root_keys = [
        os.path.join(
            "preprocessed_data",
            "preprocessed_posts",
            "preprocessing_source=preprocessed_firehose_posts",
        ),
        os.path.join(
            "preprocessed_data",
            "preprocessed_posts",
            "preprocessing_source=preprocessed_most_liked_posts",
        ),
    ]
    if fetch_all_keys:
        root_keys.append(
            os.path.join(
                "preprocessed_data",
                "preprocessed_posts",
                "preprocessing_source=preprocessed_compressed_deduped_posts",
            )
        )
    for root_key in root_keys:
        res.extend(s3.list_keys_given_prefix(root_key))
    return res


def delete_keys(keys: list[str]):
    total_keys = len(keys)
    for i, key in enumerate(keys):
        s3.client.delete_object(Bucket=ROOT_BUCKET, Key=key)
        if i % 10 == 0:
            logger.info(f"Deleted {i} keys out of {total_keys}")
    logger.info(f"Deleted {len(keys)} keys")


# NOTE: in the future, could move this computation to Athena. TBH probably
# isn't running at the scale where this would be a bottleneck, but
# it's always good to have the option.
@track_performance
def compact_dedupe_preprocessed_data(
    backfill_period: Optional[str] = None,
    backfill_duration: Optional[int] = None,
    fetch_all_keys: bool = False,
):
    if backfill_duration is not None and backfill_period in ["days", "hours"]:
        current_time = datetime.now(timezone.utc)
        if backfill_period == "days":
            backfill_time = current_time - timedelta(days=backfill_duration)
            logger.info(f"Backfilling {backfill_duration} days of data.")
        elif backfill_period == "hours":
            backfill_time = current_time - timedelta(hours=backfill_duration)
            logger.info(f"Backfilling {backfill_duration} hours of data.")
    else:
        backfill_time = None

    # load previous session data
    latest_enrichment_consolidation_session: dict = get_latest_compaction_session()  # noqa
    if latest_enrichment_consolidation_session is not None:
        enrichment_consolidation_timestamp: str = (
            latest_enrichment_consolidation_session["compaction_timestamp"]["S"]  # noqa
        )
    else:
        enrichment_consolidation_timestamp = None

    if backfill_time is not None:
        backfill_timestamp = backfill_time.strftime(timestamp_format)
        timestamp = backfill_timestamp
    else:
        timestamp = enrichment_consolidation_timestamp
    query = f"""
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY uri ORDER BY preprocessing_timestamp DESC) AS row_num
        FROM preprocessed_posts
        {f"WHERE preprocessing_timestamp > '{timestamp}'" if timestamp else ""}
    ) ranked
    WHERE row_num = 1
    """
    df = athena.query_results_as_df(query)
    df_dicts = df.to_dict(orient="records")
    df_dicts = athena.parse_converted_pandas_dicts(df_dicts)
    df_dicts_cleaned = [post for post in df_dicts if post["text"] is not None]
    df_dict_models = [
        FilteredPreprocessedPostModel(**df_dict) for df_dict in df_dicts_cleaned
    ]
    del df
    del df_dicts
    del df_dicts_cleaned
    # load existing keys from S3 (load before exporting new file so that
    # we have all the key names for the non-compacted files).
    existing_keys: list[str] = get_existing_keys(fetch_all_keys=fetch_all_keys)
    logger.info(f"Number of existing keys: {len(existing_keys)}")
    # export new file to S3.
    if timestamp is None:
        # should happen only if there are no previous sessions
        # and a lookback period is not specified.
        timestamp = generate_current_datetime_str()
    export_preprocessed_posts(posts=df_dict_models, timestamp=timestamp)
    # drop existing keys from S3.
    delete_keys(existing_keys)
    # re-trigger Glue crawler to recognize new data and partitions
    # NOTE: running MSCK REPAIR TABLE so that partitioned data can be
    # available immediately. We do this here since we really don't want
    # downtime, since downstream services will need to be able to query
    # the data. We should run both though since using the Glue crawler
    # is more thorough and also updates the table catalog.
    # athena.run_query("MSCK REPAIR TABLE preprocessed_posts")
    # glue.start_crawler(crawler_name="preprocessed_posts_crawler")
    logger.info("Successfully compacted dedupe preprocessed data")
    compaction_session = {
        "compaction_timestamp": generate_current_datetime_str(),
        "total_posts_after_compaction": len(df_dict_models),
    }
    insert_compaction_session(compaction_session=compaction_session)


if __name__ == "__main__":
    compact_dedupe_preprocessed_data()
