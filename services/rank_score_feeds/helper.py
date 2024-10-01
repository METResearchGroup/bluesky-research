"""Helper functions for the rank_score_feeds service."""

from datetime import timedelta
import json
import os
from typing import Union

import pandas as pd

from lib.aws.athena import Athena
from lib.aws.dynamodb import DynamoDB
from lib.aws.glue import Glue
from lib.aws.s3 import S3
from lib.constants import (
    convert_pipeline_to_bsky_dt_format,
    current_datetime,
    current_datetime_str,
    default_lookback_days,
    timestamp_format,
)
from lib.db.manage_local_data import (
    export_data_to_local_storage,
    load_data_from_local_storage,
)
from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger
from lib.serverless_cache import (
    default_cache_name,
    default_long_lived_ttl_seconds,
    ServerlessCache,
)
from services.consolidate_enrichment_integrations.models import (
    ConsolidatedEnrichedPostModel,
)  # noqa
from services.participant_data.helper import get_all_users
from services.participant_data.models import UserToBlueskyProfileModel
from services.preprocess_raw_data.classify_language.model import classify
from services.rank_score_feeds.constants import max_feed_length
from services.rank_score_feeds.manual_excludelist import load_users_to_exclude
from services.rank_score_feeds.models import (
    CustomFeedModel,
    CustomFeedPost,
    ScoredPostModel,
)
from services.rank_score_feeds.scoring import calculate_post_scores

consolidated_enriched_posts_table_name = "consolidated_enriched_post_records"
user_to_social_network_map_table_name = "user_social_networks"
feeds_root_s3_key = "custom_feeds"
dynamodb_table_name = "rank_score_feed_sessions"
max_num_times_user_can_appear_in_feed = 3

athena = Athena()
s3 = S3()
dynamodb = DynamoDB()
glue = Glue()
logger = get_logger(__name__)
serverless_cache = ServerlessCache()


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
    for user, user_obj in user_to_ranked_feed_map.items():
        data = {
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
        # in the cache, all I need are the list of post URIs, so we will
        # export only that.
        feed_uris = [post.item for post in custom_feed_model.feed]
        cache_key = f"user_did={user}"
        serverless_cache.set(
            cache_name=default_cache_name,
            key=cache_key,
            value=json.dumps(feed_uris),
            ttl=default_long_lived_ttl_seconds,
        )
    s3.write_dicts_jsonl_to_s3(
        data=outputs,
        key=os.path.join(feeds_root_s3_key, f"custom_feeds_{timestamp}.jsonl"),
    )
    logger.info(f"Exported {len(user_to_ranked_feed_map)} feeds to S3 and to cache.")


def preprocess_data(consolidated_enriched_posts_df: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses the data."""
    # immplement filtering (e.g., English-language filtering)
    # looks like Bluesky's language filter is broken, so I'll do my own manual
    # filtering here (could've done it upstream too tbh, this is just the quickest
    # fix). My implementation is also imperfect, but it gets more correct.
    # It uses fasttext. Some still make it through, but it's pretty good.
    logger.info(
        f"Number of posts before filtering: {len(consolidated_enriched_posts_df)}"
    )
    consolidated_enriched_posts_df = consolidated_enriched_posts_df[
        consolidated_enriched_posts_df["text"].apply(filter_post_is_english)
    ]
    logger.info(
        f"Number of posts after filtering: {len(consolidated_enriched_posts_df)}"
    )

    # Deduplication based on unique URIs, keeping the most recent consolidation_timestamp
    len_before = consolidated_enriched_posts_df.shape[0]
    consolidated_enriched_posts_df = consolidated_enriched_posts_df.sort_values(
        by="consolidation_timestamp", ascending=False
    ).drop_duplicates(subset="uri", keep="first")
    len_after = consolidated_enriched_posts_df.shape[0]

    if len_before != len_after:
        logger.info(f"Deduplicated posts from {len_before} to {len_after}.")

    # manually filter post authors
    excludes = load_users_to_exclude()
    bsky_handles_to_exclude = excludes["bsky_handles_to_exclude"]
    bsky_dids_to_exclude = excludes["bsky_dids_to_exclude"]
    consolidated_enriched_posts_df = consolidated_enriched_posts_df[
        consolidated_enriched_posts_df.apply(
            lambda x: x["author_did"] not in bsky_dids_to_exclude
            and x["author_handle"] not in bsky_handles_to_exclude,
            axis=1,
        )
    ]
    return consolidated_enriched_posts_df


def load_latest_processed_data(
    lookback_days: int = default_lookback_days,
) -> dict[str, Union[dict, list[ConsolidatedEnrichedPostModel]]]:  # noqa
    """Loads the latest consolidated enriched posts as well as the latest
    user social network."""
    lookback_datetime = current_datetime - timedelta(days=lookback_days)
    lookback_datetime_str = lookback_datetime.strftime(timestamp_format)
    lookback_datetime_str = convert_pipeline_to_bsky_dt_format(lookback_datetime_str)

    output = {}

    # get posts
    posts_df: pd.DataFrame = load_data_from_local_storage(
        service="consolidated_enriched_post_records",
        latest_timestamp=lookback_datetime_str,
    )
    df_dicts = posts_df.to_dict(orient="records")
    df_dicts = athena.parse_converted_pandas_dicts(df_dicts)
    df_models = [ConsolidatedEnrichedPostModel(**post) for post in df_dicts]
    output["consolidate_enrichment_integrations"] = df_models

    # get user social network
    user_social_network_df: pd.DataFrame = load_data_from_local_storage(
        service="scraped_user_social_network", latest_timestamp=lookback_datetime_str
    )
    social_dicts = user_social_network_df.to_dict(orient="records")
    social_dicts = athena.parse_converted_pandas_dicts(social_dicts)

    res = {}
    for row in social_dicts:
        if row["relationship_to_study_user"] == "follower":
            study_user_did = row["follow_did"]
            connection_did = row["follower_did"]
        elif row["relationship_to_study_user"] == "follow":
            study_user_did = row["follower_did"]
            connection_did = row["follow_did"]
        else:
            logger.warning(f"Skipping row with unknown relationship: {row}")
            continue  # Skip if relationship is not recognized
        if study_user_did not in res:
            res[study_user_did] = []
        res[study_user_did].append(connection_did)

    output["scraped_user_social_network"] = res

    # load latest superposters
    superposters_df: pd.DataFrame = load_data_from_local_storage(
        service="daily_superposters", latest_timestamp=lookback_datetime_str
    )
    superposters_lst: list[dict] = json.loads(superposters_df["superposters"].iloc[0])
    output["superposters"] = set([res["author_did"] for res in superposters_lst])

    return output


def export_post_scores(post_uri_to_post_score_map: dict[str, str]):
    """Exports post scores to local storage."""
    output: list[ScoredPostModel] = []
    for post_uri, post_obj in post_uri_to_post_score_map.items():
        output.append(
            ScoredPostModel(
                uri=post_uri,
                text=post_obj["post"].text,
                engagement_score=post_obj["score"]["engagement_score"],
                treatment_score=post_obj["score"]["treatment_score"],
                scored_timestamp=current_datetime_str,
                source=post_obj["post"].source,
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
    candidate_in_network_user_activity_posts: list[ConsolidatedEnrichedPostModel],  # noqa
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
    in_network_post_uris: list[str] = [
        post.uri
        for post in candidate_in_network_user_activity_posts
        if post.author_did in in_network_social_network_dids
    ]
    return in_network_post_uris


# TODO: should implement 50:50 balancing at some point tbh.
# TODO: should I also return the source feed (firehose/most_liked), to make
# sure that I am balancing the in-network and out-of-network posts?
# TODO: add 50/50 balancing (or some similar sort of balancing) between
# in-network and out-of-network posts.
def create_ranked_candidate_feed(
    condition: str,
    in_network_candidate_post_uris: list[str],
    post_pool: list[ConsolidatedEnrichedPostModel],
    max_feed_length: int = max_feed_length,
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
    in_network_posts: list[ConsolidatedEnrichedPostModel] = [
        post for post in post_pool if post.uri in in_network_candidate_post_uris
    ]
    out_of_network_posts: list[ConsolidatedEnrichedPostModel] = [
        post
        for post in post_pool
        if post.uri not in in_network_candidate_post_uris
        and (
            post.source == "most_liked"
            if condition in ["engagement", "representative_diversification"]
            else post.source == "firehose"
        )
    ]

    # make sure that no single user appears more than X times.
    in_network_author_to_count_map = {}
    out_of_network_author_to_count_map = {}
    in_network_res = []
    out_of_network_res = []

    for post in in_network_posts:
        author_id = post.author_did
        if (
            in_network_author_to_count_map.get(author_id, 0)
            < max_num_times_user_can_appear_in_feed
        ):
            in_network_res.append(post)
            in_network_author_to_count_map[author_id] = (
                in_network_author_to_count_map.get(author_id, 0) + 1
            )

    for post in out_of_network_posts:
        author_id = post.author_did
        if (
            out_of_network_author_to_count_map.get(author_id, 0)
            < max_num_times_user_can_appear_in_feed
        ):
            out_of_network_res.append(post)
            out_of_network_author_to_count_map[author_id] = (
                out_of_network_author_to_count_map.get(author_id, 0) + 1
            )

    in_network_posts = in_network_res
    out_of_network_posts = out_of_network_res

    total_in_network_posts = len(in_network_posts)
    max_in_network_posts = max_feed_length // 2
    max_allowed_in_network_posts = min(total_in_network_posts, max_in_network_posts)
    in_network_posts = in_network_posts[:max_allowed_in_network_posts]
    if len(post_pool) == 0:
        return []
    if len(in_network_candidate_post_uris) == 0:
        # if no in-network posts, return the out-of-network posts.
        feed = [
            CustomFeedPost(item=post.uri, is_in_network=False)
            for post in out_of_network_posts[:max_feed_length]
        ]
        return feed
    output_posts: list[ConsolidatedEnrichedPostModel] = []
    in_network_post_set = set(post.uri for post in in_network_posts)

    # First, add all in-network posts to the output_posts while maintaining order
    for post in in_network_posts:
        if post.uri in in_network_post_set:
            output_posts.append(post)

    # Then, fill the remaining spots in output_posts with other posts from post_pool
    for post in out_of_network_posts:
        if post.uri not in in_network_post_set and len(output_posts) < max_feed_length:
            output_posts.append(post)

    # # Now, sort the output posts based on the order that they were in in the
    # # original post pool. If post A was before post B, I want that to be true
    # # in the output posts. This implementation just ensures that we have a
    # # requisite amount of in-network posts in our feeds.
    # uri_to_post_map: dict[str, ConsolidatedEnrichedPostModel] = {
    #     post.uri: post for post in output_posts
    # }
    # post_pool_uris = [
    #     post.uri for post in post_pool if post.uri in uri_to_post_map.keys()
    # ]
    # res: list[ConsolidatedEnrichedPostModel] = []
    # for uri in post_pool_uris:
    #     res.append(uri_to_post_map[uri])

    # Ensure output_posts does not exceed max_feed_length

    feed = [
        CustomFeedPost(item=post.uri, is_in_network=post.uri in in_network_post_set)
        for post in output_posts[:max_feed_length]
    ]

    return feed


def filter_post_is_english(text: str) -> bool:
    """Filters for if a post is English.

    Returns True if the post has text and that text is in English. Returns False
    if the post either has no text or that text is not in English.

    Looks like posts are being labeled as "en" by Bluesky even when they're not
    in English, so they're passing our filters since we assume Bluesky's language
    filter is correct. Given that that isn't the case, we do it manually here.
    """
    if not text:
        return False
    text = text.replace("\n", " ").strip()
    return classify(text=text)


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
    users_to_create_feeds_for: list[str] = None,
    skip_export_post_scores: bool = False,
):
    """Do the rank score feeds.

    Also takes as optional input a list of Bluesky users (by handle) to create
    feeds for. If None, will create feeds for all users.

    Also takes as optional input a flag to skip exporting post scores to S3.
    """
    logger.info("Starting rank score feeds.")
    # load data
    study_users: list[UserToBlueskyProfileModel] = get_all_users()

    test_mode = False
    if test_mode:
        # TODO: just do the test users
        test_user_handles = [
            "testblueskyaccount.bsky.social",
            "testblueskyuserv2.bsky.social",
            "markptorres.bsky.social",
        ]
        study_users = [
            user for user in study_users if user.bluesky_handle in test_user_handles
        ]

    latest_data: dict = load_latest_processed_data()
    consolidated_enriched_posts: list[ConsolidatedEnrichedPostModel] = latest_data[
        "consolidate_enrichment_integrations"
    ]
    consolidated_enriched_posts_df = pd.DataFrame(
        [post.dict() for post in consolidated_enriched_posts]
    )
    user_to_social_network_map: dict = latest_data["scraped_user_social_network"]
    superposter_dids: set[str] = latest_data["superposters"]
    logger.info(f"Loaded {len(superposter_dids)} superposters.")  # noqa

    # preprocess the data
    consolidated_enriched_posts_df = preprocess_data(consolidated_enriched_posts_df)

    # calculate scores for all the posts. Load any pre-existing scores and then
    # calculate scores for new posts. Export scores for new posts.
    post_scores, new_post_uris = calculate_post_scores(
        posts=consolidated_enriched_posts,
        superposter_dids=superposter_dids,
        load_previous_scores=True,
    )  # noqa
    post_uri_to_post_score_map: dict[str, dict] = {
        post.uri: {"post": post, "score": score}
        for post, score in zip(consolidated_enriched_posts, post_scores)
    }
    logger.info(f"Calculated {len(post_uri_to_post_score_map)} post scores.")
    scores_to_export = {
        uri: score
        for uri, score in post_uri_to_post_score_map.items()
        if uri in new_post_uris
    }

    # export scores to storage.
    if not skip_export_post_scores:
        logger.info(f"Exporting {len(scores_to_export)} post scores.")  # noqa
        export_post_scores(post_uri_to_post_score_map=scores_to_export)

    # list of all in-network user posts, across all study users. Needs to be
    # filtered for the in-network posts relevant for a given study user.
    candidate_in_network_user_activity_posts: list[ConsolidatedEnrichedPostModel] = [  # noqa
        post for post in consolidated_enriched_posts if post.source == "firehose"
    ]
    # yes, popular posts can also be in-network. For now we'll treat them as
    # out-of-network (and perhaps revisit treating them as in-network as well.)
    # TODO: revisit this.
    out_of_network_user_activity_posts: list[ConsolidatedEnrichedPostModel] = [
        post for post in consolidated_enriched_posts if post.source == "most_liked"
    ]
    out_of_network_post_uris: list[str] = [
        post.uri for post in out_of_network_user_activity_posts
    ]
    logger.info(
        f"Loaded {len(candidate_in_network_user_activity_posts)} in-network posts."
    )  # noqa
    logger.info(f"Loaded {len(out_of_network_post_uris)} out-of-network posts.")  # noqa
    # get lists of in-network and out-of-network posts
    user_to_in_network_post_uris_map: dict[str, list[str]] = {
        user.bluesky_user_did: calculate_in_network_posts_for_user(
            user_did=user.bluesky_user_did,
            user_to_social_network_map=user_to_social_network_map,
            candidate_in_network_user_activity_posts=(
                candidate_in_network_user_activity_posts
            ),
        )
        for user in study_users
    }

    # sort posts per condition.
    all_posts: list[dict] = [
        post_score_dict for post_score_dict in post_uri_to_post_score_map.values()
    ]

    # pass posts through manual filtering

    # reverse chronological: sort by most recent posts descending
    reverse_chronological_post_pool = sorted(
        all_posts, key=lambda x: x["post"].synctimestamp, reverse=True
    )
    reverse_chronological_post_pool: list[ConsolidatedEnrichedPostModel] = [
        post_score_dict["post"]
        for post_score_dict in reverse_chronological_post_pool
        if post_score_dict["post"].source == "firehose"
    ]

    # engagement posts: sort by engagement score descending
    engagement_post_pool: list[dict] = sorted(
        all_posts, key=lambda x: x["score"]["engagement_score"], reverse=True
    )
    engagement_post_pool: list[ConsolidatedEnrichedPostModel] = [
        post_score_dict["post"] for post_score_dict in engagement_post_pool
    ]

    # treatment posts: sort by treatment score descending.
    treatment_post_pool: list[dict] = sorted(
        all_posts, key=lambda x: x["score"]["treatment_score"], reverse=True
    )
    treatment_post_pool: list[ConsolidatedEnrichedPostModel] = [
        post_score_dict["post"] for post_score_dict in treatment_post_pool
    ]

    if users_to_create_feeds_for:
        logger.info(
            f"Creating custom feeds for {len(users_to_create_feeds_for)} users provided in the input."
        )  # noqa
        study_users: list[UserToBlueskyProfileModel] = [
            user
            for user in study_users
            if user.bluesky_handle in users_to_create_feeds_for
        ]

    # then, pass in these to create_ranked_candidate_feed to create the
    # feed. We already pre-sort the posts so we don't have to do
    # this step multiple times.
    # create feeds for each user. Map feeds to users.
    user_to_ranked_feed_map: dict[str, dict] = {}
    for user in study_users:
        feed: list[CustomFeedPost] = create_ranked_candidate_feed(
            condition=user.condition,
            in_network_candidate_post_uris=(
                user_to_in_network_post_uris_map[user.bluesky_user_did]
            ),
            post_pool=(
                reverse_chronological_post_pool
                if user.condition == "reverse_chronological"
                else engagement_post_pool
                if user.condition == "engagement"
                else treatment_post_pool
                if user.condition == "representative_diversification"
                else None
            ),
            max_feed_length=max_feed_length,
        )
        user_to_ranked_feed_map[user.bluesky_user_did] = {
            "feed": feed,
            "bluesky_handle": user.bluesky_handle,
            "bluesky_user_did": user.bluesky_user_did,
            "condition": user.condition,
            "feed_statistics": generate_feed_statistics(feed=feed),
        }
        if len(feed) == 0:
            logger.error(
                f"No feed created for user {user.bluesky_user_did}. This shouldn't happen..."
            )

    # insert default feed, for users that aren't logged in or for if a user
    # isn't in the study but opens the link.
    default_feed: list[CustomFeedPost] = create_ranked_candidate_feed(
        condition="reverse_chronological",
        in_network_candidate_post_uris=[],
        post_pool=reverse_chronological_post_pool,
        max_feed_length=max_feed_length,
    )
    user_to_ranked_feed_map["default"] = {
        "feed": default_feed,
        "bluesky_handle": "default",
        "bluesky_user_did": "default",
        "condition": "default",
        "feed_statistics": generate_feed_statistics(feed=default_feed),
    }

    timestamp = generate_current_datetime_str()

    # calculate analytics
    analytics: dict = calculate_feed_analytics(
        user_to_ranked_feed_map=user_to_ranked_feed_map,
        timestamp=timestamp,
    )  # noqa
    export_feed_analytics(analytics=analytics)

    # write feeds to s3
    export_results(user_to_ranked_feed_map=user_to_ranked_feed_map, timestamp=timestamp)
    feed_generation_session = {
        "feed_generation_timestamp": timestamp,
        "number_of_new_feeds": len(user_to_ranked_feed_map),
    }
    insert_feed_generation_session(feed_generation_session)


if __name__ == "__main__":
    do_rank_score_feeds()
