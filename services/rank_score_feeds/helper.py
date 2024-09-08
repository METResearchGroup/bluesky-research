"""Helper functions for the rank_score_feeds service."""

from datetime import timedelta
import os

import numpy as np

from lib.aws.athena import Athena
from lib.aws.dynamodb import DynamoDB
from lib.aws.glue import Glue
from lib.aws.s3 import S3
from lib.constants import current_datetime, timestamp_format
from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger
from services.calculate_superposters.helper import load_latest_superposters
from services.consolidate_enrichment_integrations.models import (
    ConsolidatedEnrichedPostModel,
)  # noqa
from services.participant_data.helper import get_all_users
from services.participant_data.models import UserToBlueskyProfileModel
from services.rank_score_feeds.models import CustomFeedModel

max_feed_length = 50
default_lookback_days = 5
consolidated_enriched_posts_table_name = "consolidated_enriched_post_records"
user_to_social_network_map_table_name = "user_social_networks"
feeds_root_s3_key = "custom_feeds"
dynamodb_table_name = "rank_score_feed_sessions"

default_similarity_score = 0.75
average_popular_post_like_count = 250
coef_toxicity = 0.965
coef_constructiveness = 1.02

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
    """Exports results. Partitions on user DID."""
    for user, ranked_feed in user_to_ranked_feed_map.items():
        data = {
            "user": user,
            "feed": ranked_feed,
            "feed_generation_timestamp": timestamp,
        }
        custom_feed_model = CustomFeedModel(**data)
        s3.write_dict_json_to_s3(
            data=custom_feed_model.dict(),
            key=os.path.join(
                feeds_root_s3_key,
                f"user_did={user}",
                f"{user}_{timestamp}.json",
            ),
        )
    logger.info(f"Exported {len(user_to_ranked_feed_map)} feeds to S3.")


# NOTE: probably just want the most recent X days possibly, no?
# In practice, probably want to err on the side of more recent content.
def load_latest_consolidated_enriched_posts(
    lookback_days: int = default_lookback_days,
) -> list[ConsolidatedEnrichedPostModel]:
    """Load the latest consolidated enriched posts."""
    lookback_datetime = current_datetime - timedelta(days=lookback_days)
    lookback_datetime_str = lookback_datetime.strftime(timestamp_format)
    query = f"""
    SELECT *
    FROM {consolidated_enriched_posts_table_name} 
    WHERE consolidation_timestamp > '{lookback_datetime_str}'
    """
    df = athena.query_results_as_df(query)
    df_dicts = df.to_dict(orient="records")
    df_dicts = athena.parse_converted_pandas_dicts(df_dicts)
    return [ConsolidatedEnrichedPostModel(**post) for post in df_dicts]


def load_user_social_network() -> dict[str, list[str]]:
    """Loads a user's social network (followees/followers).

    Returns a list of users mapped to a list of their followees/followers
    (list of DIDs).
    """
    query = f"""
    SELECT * FROM {user_to_social_network_map_table_name}
    """
    df = athena.query_results_as_df(query)
    df_dicts = df.to_dict(orient="records")
    df_dicts = athena.parse_converted_pandas_dicts(df_dicts)

    res = {}
    for row in df_dicts:
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
    return res


def calculate_post_score(post: ConsolidatedEnrichedPostModel) -> float:
    """Calculate a post's score.

    Calculates what a score's post would be, depending on whether or not it's
    used for the engagement or the treatment condition.
    """
    engagement_score = None
    treatment_score = None
    engagement_coef = 1.0
    if post.sociopolitical_was_successfully_labeled and post.is_sociopolitical:
        # if sociopolitical, uprank/downrank based on toxicity/constructiveness.
        treatment_coef = (
            post.prob_toxic * coef_toxicity
            + post.prob_constructive * coef_constructiveness
        ) / (post.prob_toxic + post.prob_constructive)
    else:
        # if it's not sociopolitical, use a coef of 1.
        treatment_coef = 1.0
    if post.like_count:
        engagement_score = engagement_coef * np.log(post.like_count + 1)
        treatment_score = treatment_coef * np.log(post.like_count + 1)
    elif post.similarity_score:
        expected_like_count = average_popular_post_like_count * post.similarity_score
        engagement_score = engagement_coef * np.log(expected_like_count + 1)
        treatment_score = treatment_coef * np.log(expected_like_count + 1)
    else:
        # if we didn't have a like count or similarity score, return a
        # score where we assume both of those.
        expected_like_count = average_popular_post_like_count * default_similarity_score
        engagement_score = engagement_coef * np.log(expected_like_count + 1)
        treatment_score = treatment_coef * np.log(expected_like_count + 1)
    return {"engagement_score": engagement_score, "treatment_score": treatment_score}


def calculate_post_scores(posts: list[ConsolidatedEnrichedPostModel]) -> list[dict]:  # noqa
    """Calculate scores for a list of posts."""
    return [calculate_post_score(post) for post in posts]


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
    in_network_social_network_dids = user_to_social_network_map[user_did]
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
    in_network_candidate_post_uris: list[str],
    post_pool: list[ConsolidatedEnrichedPostModel],
    max_feed_length: int = max_feed_length,
) -> list[tuple[str, float]]:
    """Create a ranked candidate feed.

    Returns a list of tuples of post URIs and their scores.
    """
    if post_pool is None:
        raise ValueError(
            "post_pool cannot be None. This means that a user condition is unexpected/invalid"
        )  # noqa
    in_network_posts: list[ConsolidatedEnrichedPostModel] = [
        post for post in post_pool if post.uri in in_network_candidate_post_uris
    ]
    total_in_network_posts = len(in_network_posts)
    max_in_network_posts = max_feed_length // 2
    max_allowed_in_network_posts = min(total_in_network_posts, max_in_network_posts)
    in_network_posts = in_network_posts[:max_allowed_in_network_posts]
    if len(post_pool) == 0:
        return []
    if len(in_network_candidate_post_uris) == 0:
        return [(post.uri, 0.0) for post in post_pool]
    output_posts: list[ConsolidatedEnrichedPostModel] = []
    in_network_post_set = set(post.uri for post in in_network_posts)

    # First, add all in-network posts to the output_posts while maintaining order
    for post in post_pool:
        if post.uri in in_network_post_set:
            output_posts.append(post)

    # Then, fill the remaining spots in output_posts with other posts from post_pool
    for post in post_pool:
        if post.uri not in in_network_post_set and len(output_posts) < max_feed_length:
            output_posts.append(post)

    # Now, sort the output posts based on the order that they were in in the
    # original post pool. If post A was before post B, I want that to be true
    # in the output posts. This implementation just ensures that we have a
    # requisite amount of in-network posts in our feeds.
    uri_to_post_map: dict[str, ConsolidatedEnrichedPostModel] = {
        post.uri: post for post in output_posts
    }
    post_pool_uris = [
        post.uri for post in post_pool if post.uri in uri_to_post_map.keys()
    ]
    res: list[ConsolidatedEnrichedPostModel] = []
    for uri in post_pool_uris:
        res.append(uri_to_post_map[uri])

    # Ensure output_posts does not exceed max_feed_length
    res = res[:max_feed_length]
    # return a default score, just for backwards compatibility. We won't use
    # this actual score.
    return [(post.uri, 0.0) for post in res]


def do_rank_score_feeds():
    """Do the rank score feeds."""
    logger.info("Starting rank score feeds.")
    # load data
    study_users: list[UserToBlueskyProfileModel] = get_all_users()
    consolidated_enriched_posts: list[ConsolidatedEnrichedPostModel] = (
        load_latest_consolidated_enriched_posts()
    )

    # immplement filtering (e.g., English-language filtering)
    # (this should've been caught during preprocessing, so I'm not sure why
    # it wasn't caught).
    logger.info(f"Number of posts before filtering: {len(consolidated_enriched_posts)}")
    consolidated_enriched_posts = [
        post
        for post in consolidated_enriched_posts
        if post.langs is not None and post.langs == "en"
    ]
    logger.info(f"Number of posts after filtering: {len(consolidated_enriched_posts)}")

    unique_uris = set()
    deduplicated_consolidated_enriched_posts = []

    for post in consolidated_enriched_posts:
        if post.uri not in unique_uris:
            unique_uris.add(post.uri)
            deduplicated_consolidated_enriched_posts.append(post)

    if len(consolidated_enriched_posts) != len(
        deduplicated_consolidated_enriched_posts
    ):
        logger.info(
            f"Deduplicated posts from {len(consolidated_enriched_posts)} to {len(deduplicated_consolidated_enriched_posts)}."
        )
    consolidated_enriched_posts = deduplicated_consolidated_enriched_posts
    user_to_social_network_map: dict = load_user_social_network()
    superposter_dids: set[str] = load_latest_superposters()
    logger.info(f"Loaded {len(superposter_dids)} superposters.")  # noqa

    # calculate scores for all the posts
    post_scores: list[dict] = calculate_post_scores(consolidated_enriched_posts)  # noqa
    post_uri_to_post_score_map: dict[str, dict] = {
        post.uri: {"post": post, "score": score}
        for post, score in zip(consolidated_enriched_posts, post_scores)
    }
    logger.info(f"Calculated {len(post_uri_to_post_score_map)} post scores.")

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

    # reverse chronological: sort by most recent posts descending
    reverse_chronological_post_pool = sorted(
        all_posts, key=lambda x: x["post"].created_at, reverse=True
    )
    reverse_chronological_post_pool: list[ConsolidatedEnrichedPostModel] = [
        post_score_dict["post"] for post_score_dict in reverse_chronological_post_pool
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

    # then, pass in these to create_ranked_candidate_feed to create the
    # feed. We already pre-sort the posts so we don't have to do
    # this step multiple times.
    # create feeds for each user. Map feeds to users.
    user_to_ranked_feed_map: dict[str, list[str]] = {
        user.bluesky_user_did: create_ranked_candidate_feed(
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
        for user in study_users
    }

    # insert default feed, for users that aren't logged in or for if a user
    # isn't in the study but opens the link.
    default_feed = create_ranked_candidate_feed(
        in_network_candidate_post_uris=[],
        post_pool=reverse_chronological_post_pool,
        max_feed_length=max_feed_length,
    )
    user_to_ranked_feed_map["default"] = default_feed

    # write feeds to s3
    timestamp = generate_current_datetime_str()
    export_results(user_to_ranked_feed_map=user_to_ranked_feed_map, timestamp=timestamp)
    feed_generation_session = {
        "feed_generation_timestamp": timestamp,
        "number_of_new_feeds": len(user_to_ranked_feed_map),
    }
    insert_feed_generation_session(feed_generation_session)


if __name__ == "__main__":
    do_rank_score_feeds()
