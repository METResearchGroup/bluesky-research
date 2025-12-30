"""Helper functions for the rank_score_feeds service."""

from datetime import datetime, timezone
import json
import os
import random

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
    CustomFeedModel,
    CustomFeedPost,
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


def insert_feed_generation_session(feed_generation_session: dict):
    try:
        dynamodb.insert_item_into_table(
            item=feed_generation_session, table_name=dynamodb_table_name
        )
        logger.info(
            f"Successfully inserted feed generation session: {feed_generation_session}"
        )
    except Exception as e:
        logger.error(f"Failed to insert feed generation session: {e}")
        raise


def export_results(user_to_ranked_feed_map: dict, timestamp: str):
    """Exports results. Partitions on user DID.

    Exports to both S3 and the cache.
    """
    outputs: list[CustomFeedModel] = []
    for _, user_obj in user_to_ranked_feed_map.items():
        data = {
            "feed_id": f"{user_obj['bluesky_user_did']}::{timestamp}",
            "user": user_obj["bluesky_user_did"],
            "bluesky_handle": user_obj["bluesky_handle"],
            "bluesky_user_did": user_obj["bluesky_user_did"],
            "condition": user_obj["condition"],
            "feed_statistics": user_obj["feed_statistics"],
            "feed": user_obj["feed"],
            "feed_generation_timestamp": timestamp,
        }
        custom_feed_model = CustomFeedModel(**data)
        outputs.append(custom_feed_model.dict())
    partition_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    s3.write_dicts_jsonl_to_s3(
        data=outputs,
        key=os.path.join(
            feeds_root_s3_key,
            "active",
            f"partition_date={partition_date}",
            f"custom_feeds_{timestamp}.jsonl",
        ),
    )
    logger.info(f"Exported {len(user_to_ranked_feed_map)} feeds to S3 and to cache.")


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


def calculate_in_network_posts_for_user(
    user_did: str,
    user_to_social_network_map: dict,
    candidate_in_network_user_activity_posts_df: pd.DataFrame,  # noqa
) -> list[str]:
    """Calculates the possible in-network and out-of-network posts.

    Loops through the posts and if it is from the most liked feed, add as out-of-network,
    otherwise it will be checked against the user's social network to see if
    that post was written by someone in that user's social network.
    """
    # get the followee/follower DIDs for the user's social network.
    # This should only be empty if the user doesn't follow anyone (which is
    # possible and has been observed) or if their social network hasn't been
    # synced yet.
    in_network_social_network_dids = user_to_social_network_map.get(user_did, [])  # noqa
    # filter the candidate in-network user activity posts to only include the
    # ones that are in the user's social network.
    in_network_post_uris: list[str] = candidate_in_network_user_activity_posts_df[
        candidate_in_network_user_activity_posts_df["author_did"].isin(
            in_network_social_network_dids
        )
    ]["uri"].tolist()
    return in_network_post_uris


# make sure that no single user appears more than X times.
def filter_posts_by_author_count(
    posts_df: pd.DataFrame, max_count: int
) -> pd.DataFrame:
    """Returns the first X rows of each author, filtering out the rest."""
    return posts_df.groupby("author_did").head(max_count)


def jitter_feed(feed: list[CustomFeedPost], jitter_amount: int) -> list[CustomFeedPost]:
    """Jitters the feed by a random amount.

    This lets us experiment with slight movements in feed order,
    controlled by `jitter_amount`.
    """
    n = len(feed)
    result = feed.copy()
    for i in range(n):
        shift = random.randint(-jitter_amount, jitter_amount)
        new_pos = max(0, min(n - 1, i + shift))
        if i != new_pos:
            result.insert(new_pos, result.pop(i))
    return result


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


def create_ranked_candidate_feed(
    condition: str,
    in_network_candidate_post_uris: list[str],
    post_pool: pd.DataFrame,
    max_feed_length: int,
    max_in_network_posts_ratio: float,
) -> list[CustomFeedPost]:
    """Create a ranked candidate feed.

    Returns a list of dicts of post URIs, their scores, and whether or
    not the post is in-network.

    Priorities for posts:
    1. In-network posts
    2. Out-of-network most-liked posts
    """
    if post_pool is None or len(post_pool) == 0:
        raise ValueError(
            "post_pool cannot be None. This means that a user condition is unexpected/invalid"
        )  # noqa
    in_network_posts_df = post_pool[
        post_pool["uri"].isin(in_network_candidate_post_uris)
    ]
    out_of_network_source = (
        "most_liked"
        if condition in ["engagement", "representative_diversification"]
        else "firehose"
    )
    out_of_network_posts_df = post_pool[
        (~post_pool["uri"].isin(in_network_candidate_post_uris))
        & (post_pool["source"] == out_of_network_source)
    ]

    # get the number of in-network posts to include.
    total_in_network_posts = len(in_network_posts_df)
    max_in_network_posts = int(max_feed_length * max_in_network_posts_ratio)
    max_allowed_in_network_posts = min(total_in_network_posts, max_in_network_posts)
    in_network_posts_df = in_network_posts_df.iloc[:max_allowed_in_network_posts]

    # do edge-casing
    if len(post_pool) == 0:
        return []
    if len(in_network_candidate_post_uris) == 0:
        # if no in-network posts, return the out-of-network posts.
        feed = [
            CustomFeedPost(item=post["uri"], is_in_network=False)
            for _, post in out_of_network_posts_df.iterrows()
        ]
        return feed
    in_network_post_set = set(in_network_posts_df["uri"].tolist())

    # Combine in-network and out-of-network posts while maintaining order
    output_posts_df = pd.concat([in_network_posts_df, out_of_network_posts_df])

    feed: list[CustomFeedPost] = [
        CustomFeedPost(
            item=post["uri"], is_in_network=post["uri"] in in_network_post_set
        )
        for _, post in output_posts_df.iterrows()
    ]

    return feed


def postprocess_feed(
    feed: list[CustomFeedPost],
    max_feed_length: int,
    max_prop_old_posts: float,
    feed_preprocessing_multiplier: int,
    jitter_amount: int,
    previous_post_uris: set[str] = None,
) -> list[CustomFeedPost]:
    """Postprocesses the feed."""
    # do feed postprocessing on a subset of the feed to save time.
    feed = feed[: int(max_feed_length * feed_preprocessing_multiplier)]

    # ensure that there's a maximum % of old posts in the feed, so we
    # always have some fresh content.
    if previous_post_uris:
        max_num_old_posts = int(max_feed_length * max_prop_old_posts)
        old_post_count = 0
        processed_feed = []
        for post in feed:
            if post.item in previous_post_uris:
                if old_post_count < max_num_old_posts:
                    old_post_count += 1
                    processed_feed.append(post)
            else:
                processed_feed.append(post)
        feed = processed_feed

    # truncate feed
    feed = feed[:max_feed_length]

    # jitter feed to slightly shuffle ordering
    feed = jitter_feed(feed=feed, jitter_amount=jitter_amount)

    # validate feed lengths:
    if len(feed) != max_feed_length:
        raise ValueError(
            f"Feed length is not equal to max_feed_length: {len(feed)} != {max_feed_length}"
        )

    return feed


def generate_feed_statistics(feed: list[CustomFeedPost]) -> str:
    """Generates statistics about a given feed."""
    feed_length = len(feed)
    total_in_network = sum([post.is_in_network for post in feed])
    prop_in_network = (
        round(total_in_network / feed_length, 3) if feed_length > 0 else 0.0
    )
    res = {
        "prop_in_network": prop_in_network,
        "total_in_network": total_in_network,
        "feed_length": feed_length,
    }
    return json.dumps(res)


def calculate_feed_analytics(
    user_to_ranked_feed_map: dict[str, dict],
    timestamp: str,
) -> dict:
    """Calculates analytics for a given user to ranked feed map."""
    session_analytics: dict = {}
    session_analytics["total_feeds"] = len(user_to_ranked_feed_map)
    session_analytics["total_posts"] = sum(
        [len(feed["feed"]) for feed in user_to_ranked_feed_map.values()]
    )  # noqa
    session_analytics["total_in_network_posts"] = sum(
        [
            sum([post.is_in_network for post in feed["feed"]])
            for feed in user_to_ranked_feed_map.values()
        ]
    )  # noqa
    session_analytics["total_in_network_posts_prop"] = (
        round(
            session_analytics["total_in_network_posts"]
            / session_analytics["total_posts"],
            2,
        )
        if session_analytics["total_posts"] > 0
        else 0.0
    )
    engagement_feed_uris: list[str] = [
        post.item
        for feed in user_to_ranked_feed_map.values()
        if feed["condition"] == "engagement"
        for post in feed["feed"]  # noqa
    ]
    treatment_feed_uris: list[str] = [
        post.item
        for feed in user_to_ranked_feed_map.values()
        if feed["condition"] == "representative_diversification"
        for post in feed["feed"]  # noqa
    ]
    overlap_engagement_uris_in_treatment_uris = set(engagement_feed_uris).intersection(
        set(treatment_feed_uris)
    )
    overlap_treatment_uris_in_engagement_uris = set(treatment_feed_uris).intersection(
        set(engagement_feed_uris)
    )
    total_unique_engagement_uris = len(set(engagement_feed_uris))
    total_unique_treatment_uris = len(set(treatment_feed_uris))
    prop_treatment_uris_in_engagement_uris = (
        round(
            len(overlap_treatment_uris_in_engagement_uris)
            / (total_unique_treatment_uris + 1),  # to avoid division by zero
            3,
        )
        if total_unique_treatment_uris > 0
        else 0.0
    )
    prop_engagement_uris_in_treatment_uris = (
        round(
            len(overlap_engagement_uris_in_treatment_uris)
            / (total_unique_engagement_uris + 1),  # to avoid division by zero
            3,
        )
        if total_unique_treatment_uris > 0
        else 0.0
    )
    session_analytics["total_unique_engagement_uris"] = total_unique_engagement_uris
    session_analytics["total_unique_treatment_uris"] = total_unique_treatment_uris
    session_analytics["prop_overlap_treatment_uris_in_engagement_uris"] = (
        prop_treatment_uris_in_engagement_uris
    )
    session_analytics["prop_overlap_engagement_uris_in_treatment_uris"] = (
        prop_engagement_uris_in_treatment_uris
    )
    session_analytics["total_feeds_per_condition"] = {}
    for condition in [
        "representative_diversification",
        "engagement",
        "reverse_chronological",
    ]:
        session_analytics["total_feeds_per_condition"][condition] = sum(
            [
                feed["condition"] == condition
                for feed in user_to_ranked_feed_map.values()
            ]
        )
    session_analytics["session_timestamp"] = timestamp
    return session_analytics


def export_feed_analytics(analytics: dict) -> None:
    """Exports feed analytics to S3."""
    dtype_map = MAP_SERVICE_TO_METADATA["feed_analytics"]["dtypes_map"]
    df = pd.DataFrame([analytics])
    df = df.astype(dtype_map)
    export_data_to_local_storage(df=df, service="feed_analytics")
    logger.info("Exported session feed analytics.")


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
