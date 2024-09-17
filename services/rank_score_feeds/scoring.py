"""Scoring logic for feed generation."""

from datetime import datetime, timezone
from typing import Literal

import numpy as np

from lib.constants import timestamp_format
from services.consolidate_enrichment_integrations.models import (
    ConsolidatedEnrichedPostModel,
)  # noqa
from services.rank_score_feeds.constants import default_lookback_days

# TODO: I should check this empirically.
default_similarity_score = 0.75
# TODO: I should check this empirically.
average_popular_post_like_count = 250
coef_toxicity = 0.965
coef_constructiveness = 1.02
superposter_coef = 0.95
default_max_freshness_score = 1
default_lookback_hours = default_lookback_days * 24
freshness_decay_ratio: float = default_max_freshness_score / default_lookback_hours  # noqa


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
        a = max_freshness_score
        decay_factor = 1 - decay_ratio
        lambda_factor = 0.95
        freshness_score = (a * decay_factor * lambda_factor) ** post_age_hours

        # Ensure exp_score is capped to (0, max_score)
        freshness_score = min(max(freshness_score, 0), max_freshness_score)

    return freshness_score


def score_post_likeability(post: ConsolidatedEnrichedPostModel) -> float:
    """Score a post's likeability. We either use the actual like count or we
    use an estimation of the like count, based on how similar that post is to
    posts that are normally liked.
    """
    like_score = None
    if post.like_count:
        like_score = np.log(post.like_count + 1)
    elif post.similarity_score:
        expected_like_count = average_popular_post_like_count * post.similarity_score
        like_score = np.log(expected_like_count + 1)
    else:
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


def calculate_post_scores(
    posts: list[ConsolidatedEnrichedPostModel],
    superposter_dids: set[str],
) -> list[dict]:  # noqa
    """Calculate scores for a list of posts."""
    return [
        calculate_post_score(post=post, superposter_dids=superposter_dids)
        for post in posts
    ]
