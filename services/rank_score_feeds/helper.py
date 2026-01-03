"""Helper functions for the rank_score_feeds service."""

import json

import pandas as pd

from lib.aws.athena import Athena
from lib.aws.dynamodb import DynamoDB
from lib.aws.glue import Glue
from lib.aws.s3 import S3
from lib.constants import (
    current_datetime_str,
    default_lookback_days,
)
from lib.datetime_utils import TimestampFormat, calculate_lookback_datetime_str
from lib.db.data_processing import parse_converted_pandas_dicts
from lib.db.manage_local_data import (
    export_data_to_local_storage,
)
from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from lib.log.logger import get_logger
from services.calculate_superposters.load_data import load_latest_superposters
from services.calculate_superposters.models import CalculateSuperposterSource
from services.consolidate_enrichment_integrations.load_data import load_enriched_posts
from services.participant_data.social_network import load_user_social_network_map
from services.rank_score_feeds.config import feed_config
from services.rank_score_feeds.models import (
    FeedInputData,
    LatestFeeds,
    ScoredPostModel,
)

consolidated_enriched_posts_table_name = "consolidated_enriched_post_records"
user_to_social_network_map_table_name = "user_social_networks"
feeds_root_s3_key = "custom_feeds"
dynamodb_table_name = "rank_score_feed_sessions"

athena = Athena()
s3 = S3()
dynamodb = DynamoDB()
glue = Glue()
logger = get_logger(__name__)


def load_feed_input_data(lookback_days: int = default_lookback_days) -> FeedInputData:
    """Load feed input data from multiple services.

    Loads and returns the latest processed data from multiple services:
    - Consolidated enriched posts (filtered by lookback window)
    - User social network relationship mappings
    - Superposter DIDs for identifying high-volume authors

    Args:
        lookback_days: Number of days to look back when loading enriched posts.
            Defaults to the configured default lookback period.

    Returns:
        FeedInputData containing:
            - consolidate_enrichment_integrations: DataFrame of enriched posts
            - scraped_user_social_network: Mapping of user DIDs to their connection DIDs
            - superposters: Set of superposter author DIDs
    """
    lookback_datetime_str = calculate_lookback_datetime_str(
        lookback_days, format=TimestampFormat.BLUESKY
    )

    feed_input_data = FeedInputData(
        consolidate_enrichment_integrations=load_enriched_posts(
            latest_timestamp=lookback_datetime_str
        ),
        scraped_user_social_network=load_user_social_network_map(),
        superposters=load_latest_superposters(
            source=CalculateSuperposterSource.LOCAL,
            latest_timestamp=lookback_datetime_str,
        ),
    )

    return feed_input_data


def export_post_scores(scores_to_export: list[dict]):
    """Exports post scores to local storage."""
    output: list[ScoredPostModel] = []
    for score in scores_to_export:
        output.append(
            ScoredPostModel(
                uri=score["uri"],
                text=score["text"],
                engagement_score=score["engagement_score"],
                treatment_score=score["treatment_score"],
                scored_timestamp=current_datetime_str,
                source=score["source"],
            )
        )
    output_jsons = [post.dict() for post in output]
    dtypes_map = MAP_SERVICE_TO_METADATA["post_scores"]["dtypes_map"]
    df = pd.DataFrame(output_jsons)
    if "partition_date" not in df.columns:
        df["partition_date"] = pd.to_datetime(df["scored_timestamp"]).dt.date
    df = df.astype(dtypes_map)
    export_data_to_local_storage(df=df, service="post_scores")


def load_latest_feeds() -> LatestFeeds:
    """Loads the latest feeds per user, from S3.

    Returns a model containing a map of user handles to the set of URIs
    of posts in their latest feed.
    """
    query = """
    SELECT *
    FROM custom_feeds
    WHERE (bluesky_handle, feed_generation_timestamp) IN (
        SELECT bluesky_handle, MAX(feed_generation_timestamp)
        FROM custom_feeds
        GROUP BY bluesky_handle
    )
    """
    df = athena.query_results_as_df(query=query)
    df_dicts = df.to_dict(orient="records")
    df_dicts = parse_converted_pandas_dicts(df_dicts)

    feeds_dict: dict[str, set[str]] = {}
    for df_dict in df_dicts:
        handle = df_dict["bluesky_handle"]
        feed = json.loads(df_dict["feed"])
        uris = {post["item"] for post in feed}
        feeds_dict[handle] = uris

    return LatestFeeds(feeds=feeds_dict)


def do_rank_score_feeds(
    users_to_create_feeds_for: list[str] | None = None,
    export_new_scores: bool = True,
    test_mode: bool = False,
):
    """Do the rank score feeds.

    This is a thin wrapper around FeedGenerationOrchestrator.run() to maintain
    backward compatibility with existing entrypoints.

    Also takes as optional input a list of Bluesky users (by handle) to create
    feeds for. If None, will create feeds for all users.

    Also takes as optional input a flag to skip exporting post scores to S3.
    """
    # Lazy import to avoid circular dependency with orchestrator.py
    from services.rank_score_feeds.orchestrator import FeedGenerationOrchestrator

    orchestrator = FeedGenerationOrchestrator(feed_config=feed_config)
    orchestrator.run(
        users_to_create_feeds_for=users_to_create_feeds_for,
        export_new_scores=export_new_scores,
        test_mode=test_mode,
    )


if __name__ == "__main__":
    do_rank_score_feeds()
