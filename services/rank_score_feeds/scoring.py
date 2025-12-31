"""Scoring logic for feed generation."""

from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from lib.constants import current_datetime, timestamp_format
from lib.db.manage_local_data import load_data_from_local_storage
from lib.log.logger import get_logger
from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.models import (
    FreshnessScoreFunction,
    PostScoreByAlgorithm,
)

logger = get_logger(__name__)

BASELINE_TREATMENT_ALGORITHM_COEF = 1.0


def _apply_superposter_penalty(
    post: pd.Series, superposter_dids: set[str], feed_config: FeedConfig
) -> float:
    """Apply a penalty to a post for being written by a superposter."""
    if post["author_did"] in superposter_dids:
        return feed_config.superposter_coef
    return 1.0


def _fix_constructive_endpoint_bug(post: pd.Series) -> pd.Series:
    """Backwards-compatibility bug where halfway through the study,
    the Google Perspective API endpoint for prob_constructive was replaced
    with prob_reasoning. This function fixes the bug by setting prob_constructive
    to prob_reasoning if prob_constructive is None.
    """
    if post["prob_constructive"] is None:
        post["prob_constructive"] = post["prob_reasoning"]
    return post


def _calculate_treatment_algorithm_score(
    post: pd.Series, feed_config: FeedConfig
) -> float:
    prob_toxic = float(post["prob_toxic"])
    prob_constructive = float(post["prob_constructive"])
    coef_toxicity = float(feed_config.coef_toxicity)
    coef_constructiveness = float(feed_config.coef_constructiveness)
    denominator = prob_toxic + prob_constructive
    if denominator == 0:
        # Avoid division by zero; return neutral score multiplier
        return 1.0
    return (
        prob_toxic * coef_toxicity + prob_constructive * coef_constructiveness
    ) / denominator


def _post_is_valid_for_treatment_algorithm(post: pd.Series) -> bool:
    """
    Check if a post is valid for the treatment algorithm.

    For a post to be valid for the treatment algorithm, it must:
    - be sociopolitical
    - have a successfully labeled sociopolitical category
    - have a successfully labeled perspective
    """
    return (
        bool(post.get("sociopolitical_was_successfully_labeled", False))
        and bool(post.get("is_sociopolitical", False))
        and bool(post.get("perspective_was_successfully_labeled", False))
    )


def score_treatment_algorithm(
    post: pd.Series, superposter_dids: set[str], feed_config: FeedConfig
) -> float:
    """Score posts based on our treatment algorithm.

    If a post is sociopolitical, uprank/downrank based on
    toxicity/constructiveness.
    """
    treatment_algorithm_score: float = BASELINE_TREATMENT_ALGORITHM_COEF
    if _post_is_valid_for_treatment_algorithm(post):
        post = _fix_constructive_endpoint_bug(post)
        treatment_algorithm_score = _calculate_treatment_algorithm_score(
            post=post, feed_config=feed_config
        )

    # penalize post for being written by a superposter.
    superposter_penalty: float = _apply_superposter_penalty(
        post=post, superposter_dids=superposter_dids, feed_config=feed_config
    )
    treatment_algorithm_score *= superposter_penalty

    return treatment_algorithm_score


def calculate_post_age(post: pd.Series, lookback_hours: float) -> float:
    """Calculate a post's age, in hours.

    Args:
        post: Post data containing synctimestamp.
        lookback_hours: Maximum age in hours. Posts older than this will be clamped.

    Returns:
        Post age in hours, clamped to lookback_hours maximum.
    """
    post_dt_object: datetime = datetime.strptime(
        post["synctimestamp"], timestamp_format
    ).replace(tzinfo=timezone.utc)
    current_dt_object: datetime = datetime.now(timezone.utc)
    time_difference = current_dt_object - post_dt_object
    seconds_difference: float = time_difference.total_seconds()
    post_age_hours: float = round(seconds_difference / 3600, 2)
    if post_age_hours > lookback_hours:
        post_age_hours = lookback_hours
    return post_age_hours


def calculate_linear_freshness_score(post: pd.Series, feed_config: FeedConfig) -> float:
    """Score a post's freshness using a linear function."""
    max_freshness_score = feed_config.default_max_freshness_score
    decay_ratio = feed_config.freshness_decay_ratio
    lookback_hours = feed_config.freshness_lookback_days * 24
    post_age_hours: float = calculate_post_age(post=post, lookback_hours=lookback_hours)
    return max_freshness_score - (post_age_hours * decay_ratio)


def calculate_exponential_freshness_score(
    post: pd.Series, feed_config: FeedConfig
) -> float:
    """Score a post's freshness using an exponential function."""
    max_freshness_score = feed_config.default_max_freshness_score
    decay_ratio = feed_config.freshness_decay_ratio
    lookback_hours = feed_config.freshness_lookback_days * 24
    post_age_hours: float = calculate_post_age(post=post, lookback_hours=lookback_hours)

    base_value = feed_config.freshness_exponential_base
    decay_factor = 1 - decay_ratio
    lambda_factor = feed_config.freshness_lambda_factor

    # gives a score between 0 and 1
    freshness_coef = (base_value * decay_factor * lambda_factor) ** post_age_hours
    freshness_score = freshness_coef * max_freshness_score
    return freshness_score


def score_post_freshness(
    post: pd.Series,
    feed_config: FeedConfig,
    score_func: FreshnessScoreFunction = FreshnessScoreFunction.EXPONENTIAL,
) -> float:
    """Score a post's freshness. The older the post, the lower the score."""
    if score_func == "linear":
        return calculate_linear_freshness_score(post=post, feed_config=feed_config)
    elif score_func == "exponential":
        return calculate_exponential_freshness_score(post=post, feed_config=feed_config)
    else:
        raise ValueError(f"Invalid freshness score function: {score_func}")


def _get_like_count_value(post: pd.Series) -> float | None:
    """Extract and validate like_count from post.

    Safely extract like_count with proper type handling.

    Returns:
        Like count as float, or None if missing/invalid.
    """
    like_count = post.get("like_count")
    if like_count is None or pd.isna(like_count):
        return None
    try:
        return float(like_count)
    except (ValueError, TypeError):
        return None


def _get_similarity_score_value(post: pd.Series) -> float | None:
    """Extract and validate similarity_score from post.

    Safely extract similarity_score with proper type handling.

    Returns:
        Similarity score as float, or None if missing/invalid.
    """
    similarity_score = post.get("similarity_score")
    if similarity_score is None or pd.isna(similarity_score):
        return None
    try:
        return float(similarity_score)
    except (ValueError, TypeError):
        return None


def _calculate_default_like_count_estimate(feed_config: FeedConfig) -> float:
    """Calculate the default like count estimate.

    Returns:
        Default like count estimate as float.
    """
    expected_like_count: float = (
        feed_config.average_popular_post_like_count
        * feed_config.default_similarity_score
    )
    return expected_like_count


def _calculate_score_based_on_like_count(like_count: float) -> float:
    """Calculate the score based on the like count (either actual or estimated).

    Returns:
        Score based on the like count as float.
    """
    return np.log(like_count + 1)


def score_post_likeability(post: pd.Series, feed_config: FeedConfig) -> float:
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

    default_expected_like_count: float = _calculate_default_like_count_estimate(
        feed_config
    )
    default_expected_like_score: float = _calculate_score_based_on_like_count(
        default_expected_like_count
    )

    try:
        # Step 1: If we empirically have a like count, use it.
        like_count = _get_like_count_value(post)
        if like_count:
            return _calculate_score_based_on_like_count(like_count)

        # Step 2: If we don't have a like count, use the similarity score.
        similarity_score = _get_similarity_score_value(post)
        if similarity_score:
            expected_like_count = (
                feed_config.average_popular_post_like_count * similarity_score
            )
            return _calculate_score_based_on_like_count(expected_like_count)

        # Step 3: If we don't have a like count or similarity score,
        # use the default like score calculation.
        return default_expected_like_score
    except Exception as e:
        logger.error(f"Error calculating like score for post {post['uri']}: {e}")
        return default_expected_like_score


def calculate_post_score(
    post: pd.Series, superposter_dids: set[str], feed_config: FeedConfig
) -> PostScoreByAlgorithm:
    """Calculate a post's score.

    Calculates what a score's post would be, depending on whether or not it's
    used for the engagement or the treatment condition.

    Calculates three forms of scores:
    - Likeability score: adds to the engagement and treatment scores.
    - Freshness score: adds to the engagement and treatment scores.
    - Treatment score: multiplies the treatment score by the treatment algorithm score.
    """
    try:
        engagement_score: float = 0
        treatment_score: float = 0

        # set the base score to be based on the likeability of the post
        # and the freshness of the post.
        post_likeability_score: float = score_post_likeability(
            post=post, feed_config=feed_config
        )
        post_freshness_score: float = score_post_freshness(
            post=post, feed_config=feed_config
        )

        engagement_score += post_likeability_score
        engagement_score += post_freshness_score
        treatment_score += post_likeability_score
        treatment_score += post_freshness_score

        # the treatment algorithm score is treated as a multiplier.
        treatment_algorithm_score: float = score_treatment_algorithm(
            post=post,
            superposter_dids=superposter_dids,
            feed_config=feed_config,
        )

        # multiply scores by the engagement/treatment coefs.
        engagement_score *= feed_config.engagement_coef
        treatment_score *= treatment_algorithm_score

        return PostScoreByAlgorithm(
            uri=str(post["uri"]),
            engagement_score=engagement_score,
            treatment_score=treatment_score,
        )
    except Exception as e:
        logger.error(f"Error calculating post score for post {post['uri']}: {e}")
        raise e


def load_previous_post_scores(
    feed_config: FeedConfig,
    lookback_days: int | None = None,
) -> dict:
    """DEPRECATED: Use ScoresRepository.load_cached_scores() instead.

    Load previous post scores from storage, for a given lookback period.

    Returns a dict with the post uri as the key and the scores dict as the
    value. The scores dict contains the engagement and treatment scores for the
    post.

    This function is kept for backward compatibility during migration.
    New code should use ScoresRepository.load_cached_scores().
    """
    if lookback_days is None:
        lookback_days = feed_config.default_scoring_lookback_days
    lookback_timestamp = (current_datetime - timedelta(days=lookback_days)).strftime(
        "%Y-%m-%d"
    )
    logger.info(f"Loading previous post scores from {lookback_timestamp}.")
    df = load_data_from_local_storage(
        service="post_scores",
        directory="active",
        latest_timestamp=lookback_timestamp,
    )
    df = df.sort_values(by="scored_timestamp", ascending=False).drop_duplicates(
        subset="uri", keep="first"
    )
    previous_scores = {
        row["uri"]: {
            "engagement_score": row["engagement_score"],
            "treatment_score": row["treatment_score"],
        }
        for _, row in df.iterrows()
        if not pd.isna(row["engagement_score"]) and not pd.isna(row["treatment_score"])
    }
    del df
    logger.info("Finished loading previous post scores.")
    return previous_scores


# NOTE: should consider setting default likes at the batch level.
def calculate_post_scores(
    posts: pd.DataFrame,
    superposter_dids: set[str],
    feed_config: FeedConfig,
    load_previous_scores: bool = True,
) -> tuple[list[dict], list[str]]:  # noqa
    """DEPRECATED: Use ScoringService.score_posts() instead.

    Calculate scores for a list of posts.

    This function is kept for backward compatibility during migration.
    New code should use ScoringService.score_posts().
    """
    scores = []
    new_post_uris = []

    if not load_previous_scores:
        return posts.apply(
            lambda post: calculate_post_score(
                post=post, superposter_dids=superposter_dids, feed_config=feed_config
            ),
            axis=1,
        ).tolist(), posts["uri"].tolist()
    else:
        total_posts = len(posts)
        previous_post_scores: dict = load_previous_post_scores(feed_config=feed_config)

        def process_post(post: pd.Series):
            if post["uri"] in previous_post_scores:
                return previous_post_scores[post["uri"]], None
            else:
                score = calculate_post_score(
                    post=post,
                    superposter_dids=superposter_dids,
                    feed_config=feed_config,
                )
                return score, post["uri"]

        results = posts.apply(process_post, axis=1)
        scores, new_post_uris = zip(*results)
        scores = list(scores)
        new_post_uris = [uri for uri in new_post_uris if uri is not None]

        total_new_scores = len(new_post_uris)
        logger.info(f"Calculated {total_new_scores}/{total_posts} new post scores.")

    return list(scores), new_post_uris
