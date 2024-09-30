"""Scoring logic for feed generation."""

from datetime import datetime, timedelta, timezone
from typing import Literal

import numpy as np
import pandas as pd

from lib.constants import current_datetime, timestamp_format, default_lookback_days
from lib.db.manage_local_data import load_data_from_local_storage
from lib.log.logger import get_logger
from services.consolidate_enrichment_integrations.models import (
    ConsolidatedEnrichedPostModel,
)  # noqa

default_similarity_score = 0.8
average_popular_post_like_count = 1250
coef_toxicity = 0.965
coef_constructiveness = 1.02
superposter_coef = 0.95
default_max_freshness_score = 3
default_lookback_hours = default_lookback_days * 24
freshness_decay_ratio: float = default_max_freshness_score / default_lookback_hours  # noqa
default_scoring_lookback_days = 1

logger = get_logger(__name__)

# TODO: add superposter information.
def score_treatment_algorithm(
    post: ConsolidatedEnrichedPostModel, superposter_dids: set[str]
) -> dict:
    """Score posts based on our treatment algorithm."""
    treatment_coef = 1.0

    # update post based on toxicity/constructiveness attributes.
    if (
        post.sociopolitical_was_successfully_labeled
        and post.is_sociopolitical
        and post.perspective_was_successfully_labeled
    ):
        if post.prob_constructive is None:
            # backwards-compatbility bug where prob_constructive wasn't
            # set, but the endpoints for prob_reasoning and prob_constructive
            # are actually the same.
            post.prob_constructive = post.prob_reasoning
        # if sociopolitical, uprank/downrank based on toxicity/constructiveness.
        treatment_coef = (
            post.prob_toxic * coef_toxicity
            + post.prob_constructive * coef_constructiveness
        ) / (post.prob_toxic + post.prob_constructive)

    # penalize post for being written by a superposter.
    if post.author_did in superposter_dids:
        treatment_coef *= superposter_coef

    return treatment_coef


def calculate_post_age(post: ConsolidatedEnrichedPostModel) -> int:
    """Calculate a post's age, in hours."""
    post_dt_object: datetime = datetime.strptime(
        post.synctimestamp, timestamp_format
    ).replace(tzinfo=timezone.utc)
    current_dt_object: datetime = datetime.now(timezone.utc)
    time_difference = current_dt_object - post_dt_object
    seconds_difference: float = time_difference.total_seconds()
    post_age_hours: float = round(seconds_difference / 3600, 2)
    if post_age_hours > default_lookback_hours:
        post_age_hours = default_lookback_hours
    return post_age_hours


def score_post_freshness(
    post: ConsolidatedEnrichedPostModel,
    max_freshness_score: float = default_max_freshness_score,
    decay_ratio: float = freshness_decay_ratio,
    score_func: Literal["linear", "exponential"] = "exponential",
) -> float:
    """Score a post's freshness. The older the post, the lower the score."""
    post_age_hours: float = calculate_post_age(post=post)
    if score_func == "linear":
        freshness_score: float = max_freshness_score - (post_age_hours * decay_ratio)
    elif score_func == "exponential":
        a = 1
        decay_factor = 1 - decay_ratio
        lambda_factor = 0.95
        # gives a score between 0 and 1
        freshness_coef = (a * decay_factor * lambda_factor) ** post_age_hours

        freshness_score = freshness_coef * max_freshness_score

    return freshness_score


def score_post_likeability(post: ConsolidatedEnrichedPostModel) -> float:
    """Score a post's likeability. We either use the actual like count or we
    use an estimation of the like count, based on how similar that post is to
    posts that are normally liked.

    Using default values for the expected post like count and similarity to most
    liked posts puts the firehose in the same distribution as the most liked
    posts, which is obviously not true (the posts with the most likes get the
    most likes for a reason), but doing it this way allows the firehose posts, which
    don't have any like counts, to be scored on a scale closer to that of the most
    liked posts. Without this, the most liked posts dominate the feed. This also
    has the secondary implication that we treat posts by followed account as being
    equally as likeable/giving as much utility to the user as if they saw the average
    popular feed, which is an OK implication. After all, the user chose to follow
    those accounts, so seeing those posts is still valuable to them.
    """
    try:
        if post.like_count:
            like_score = np.log(post.like_count + 1)
        elif post.similarity_score:
            expected_like_count = average_popular_post_like_count * post.similarity_score
            like_score = np.log(expected_like_count + 1)
        else:
            expected_like_count = average_popular_post_like_count * default_similarity_score
            like_score = np.log(expected_like_count + 1)
    except Exception as e:
        logger.error(f"Error calculating like score for post {post.uri}: {e}")
        expected_like_count = average_popular_post_like_count * default_similarity_score
        like_score = np.log(expected_like_count + 1)
    return like_score


def calculate_post_score(
    post: ConsolidatedEnrichedPostModel, superposter_dids: set[str]
) -> float:
    """Calculate a post's score.

    Calculates what a score's post would be, depending on whether or not it's
    used for the engagement or the treatment condition.
    """
    engagement_score = 0
    treatment_score = 0
    engagement_coef = 1.0
    treatment_coef: float = score_treatment_algorithm(
        post=post,
        superposter_dids=superposter_dids,
    )

    # set the base score to be based on the likeability of the post
    # adn the freshness of the post.
    post_likeability_score = score_post_likeability(post=post)
    post_freshness_score = score_post_freshness(post=post)

    engagement_score += post_likeability_score
    engagement_score += post_freshness_score
    treatment_score += post_likeability_score
    treatment_score += post_freshness_score

    # multiply scores by the engagement/treatment coefs.
    engagement_score *= engagement_coef
    treatment_score *= treatment_coef

    return {"engagement_score": engagement_score, "treatment_score": treatment_score}


def load_previous_post_scores(lookback_days: int = default_scoring_lookback_days) -> dict:
    """Load previous post scores from storage, for a given lookback period.

    Returns a dict with the post uri as the key and the scores dict as the
    value. The scores dict contains the engagement and treatment scores for the
    post.
    """
    lookback_timestamp = (current_datetime - timedelta(days=lookback_days)).strftime(
        "%Y-%m-%d"
    )
    logger.info(f"Loading previous post scores from {lookback_timestamp}.")
    df = load_data_from_local_storage(
        service="post_scores",
        directory="active",
        latest_timestamp=lookback_timestamp,
    )
    df = df.sort_values(by="scored_timestamp", ascending=False).drop_duplicates(subset="uri", keep="first")
    previous_scores = {
        row["uri"]: {
            "engagement_score": row["engagement_score"],
            "treatment_score": row["treatment_score"]
        }
        for _, row in df.iterrows()
        if not pd.isna(row["engagement_score"]) and not pd.isna(row["treatment_score"])
    }
    del df
    logger.info("Finished loading previous post scores.")
    return previous_scores


def calculate_post_scores(
    posts: list[ConsolidatedEnrichedPostModel],
    superposter_dids: set[str],
    load_previous_scores: bool = True
) -> tuple[list[dict], list[str]]:  # noqa
    """Calculate scores for a list of posts."""
    scores = []
    new_post_uris = []

    if not load_previous_scores:
        return [
            calculate_post_score(post=post, superposter_dids=superposter_dids)
            for post in posts
        ]
    else:
        # total_posts = len(posts)
        # total_new_scores = 0
        # previous_post_scores: dict = load_previous_post_scores()
        # for post in posts:
        #     if post.uri in previous_post_scores:
        #         scores.append(previous_post_scores[post.uri])
        #     else:
        #         scores.append(
        #             calculate_post_score(post=post, superposter_dids=superposter_dids)
        #         )
        #         total_new_scores += 1
        #         new_post_uris.append(post.uri)
        # logger.info(f"Calculated {total_new_scores}/{total_posts} new post scores.")
        total_posts = len(posts)
        previous_post_scores: dict = load_previous_post_scores()

        for post in posts:
            if post.uri in previous_post_scores:
                scores.append(previous_post_scores[post.uri])
            else:
                scores.append(
                    calculate_post_score(post=post, superposter_dids=superposter_dids)
                )
                new_post_uris.append(post.uri)

        new_post_uris = [uri for uri in new_post_uris if uri is not None]

        total_new_scores = len(new_post_uris)
        logger.info(f"Calculated {total_new_scores}/{total_posts} new post scores.")

    return list(scores), new_post_uris
