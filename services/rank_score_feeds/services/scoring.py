"""Service for scoring posts with caching support."""

from datetime import datetime, timezone

import numpy as np
import pandas as pd

from lib.constants import current_datetime_str, timestamp_format
from lib.log.logger import get_logger
from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.models import (
    FreshnessScoreFunction,
    PostScoreByAlgorithm,
    ScoredPostModel,
    PostsSplitByScoringStatus,
)
from services.rank_score_feeds.repositories.scores_repo import ScoresRepositoryProtocol

logger = get_logger(__name__)

# Constants
DEFAULT_TREATMENT_ALGORITHM_SCORE: float = 1.0
DEFAULT_SUPERPOSTER_COEF: float = 1.0


# Scoring logic functions
def _apply_superposter_coef(
    post: pd.Series, superposter_dids: set[str], feed_config: FeedConfig
) -> float:
    """Apply a penalty to a post for being written by a superposter.

    <1 = penalize, >1 = reward. Defined in config and intended to be <1.

    Args:
        post: The post to score.
        superposter_dids: The set of superposter DIDs.
        feed_config: The feed configuration.

    Returns:
        The superposter coef as a float.
    """
    if post["author_did"] in superposter_dids:
        return feed_config.superposter_coef
    return DEFAULT_SUPERPOSTER_COEF


def _fix_constructive_endpoint_bug(post: pd.Series) -> pd.Series:
    """Backwards-compatibility bug where halfway through the study,
    the Google Perspective API endpoint for prob_constructive was replaced
    with prob_reasoning. This function fixes the bug by setting prob_constructive
    to prob_reasoning if prob_constructive is None.

    Note: This function mutates the input Series in place.
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
        return DEFAULT_TREATMENT_ALGORITHM_SCORE
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
    treatment_algorithm_score: float = DEFAULT_TREATMENT_ALGORITHM_SCORE
    if _post_is_valid_for_treatment_algorithm(post):
        post = _fix_constructive_endpoint_bug(post)
        treatment_algorithm_score = _calculate_treatment_algorithm_score(
            post=post, feed_config=feed_config
        )

    # penalize post for being written by a superposter.
    superposter_coef: float = _apply_superposter_coef(
        post=post, superposter_dids=superposter_dids, feed_config=feed_config
    )
    treatment_algorithm_score *= superposter_coef

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
        str(post["synctimestamp"]), timestamp_format
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
    max_freshness_score: float = feed_config.default_max_freshness_score
    decay_ratio: float = feed_config.freshness_decay_ratio
    lookback_hours: float = feed_config.freshness_lookback_days * 24
    post_age_hours: float = calculate_post_age(post=post, lookback_hours=lookback_hours)
    return max_freshness_score - (post_age_hours * decay_ratio)


def calculate_exponential_freshness_score(
    post: pd.Series, feed_config: FeedConfig
) -> float:
    """Score a post's freshness using an exponential function."""
    max_freshness_score: float = feed_config.default_max_freshness_score
    decay_ratio: float = feed_config.freshness_decay_ratio
    lookback_hours: float = feed_config.freshness_lookback_days * 24
    post_age_hours: float = calculate_post_age(post=post, lookback_hours=lookback_hours)

    base_value: float = feed_config.freshness_exponential_base
    decay_factor: float = 1 - decay_ratio
    lambda_factor: float = feed_config.freshness_lambda_factor

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
    if score_func == FreshnessScoreFunction.LINEAR:
        return calculate_linear_freshness_score(post=post, feed_config=feed_config)
    elif score_func == FreshnessScoreFunction.EXPONENTIAL:
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


class ScoringService:
    """Service for calculating post scores with caching.

    Orchestrates the scoring process:
    1. Loads cached scores from repository
    2. Calculates new scores for uncached posts
    3. Merges cached and new scores
    4. Saves new scores back to repository
    """

    def __init__(
        self,
        scores_repo: ScoresRepositoryProtocol,
        feed_config: FeedConfig,
    ):
        """Initialize scoring service.

        Args:
            scores_repo: Repository for loading/saving cached scores.
            feed_config: Configuration for scoring algorithm.
        """
        self.scores_repo = scores_repo
        self.config = feed_config

    def _validate_posts_for_scoring(self, posts_df: pd.DataFrame) -> None:
        """Validate posts for scoring and raise an error
        if the posts are empty or missing the 'uri' column.

        Args:
            posts_df: DataFrame containing posts to score. Must have 'uri' column.
        """
        if posts_df.empty:
            raise ValueError(
                "Empty posts DataFrame provided. Returning empty ScoredPosts."
            )

        if "uri" not in posts_df.columns:
            raise ValueError("posts_df must contain 'uri' column.")

    def _split_posts_by_scoring_status(
        self, posts_df: pd.DataFrame, cached_scores: list[PostScoreByAlgorithm]
    ) -> PostsSplitByScoringStatus:
        """Split posts into already scored and not yet scored.

        Args:
            posts_df: DataFrame containing posts to score. Must have 'uri' column.
            cached_scores: List of PostScoreByAlgorithm.

        Returns:
            PostsSplitByScoringStatus with:
                - already_scored: pd.DataFrame of posts that have already been scored
                - not_scored_yet: pd.DataFrame of posts that have not yet been scored
        """
        previously_scored_post_uris: list[str] = [score.uri for score in cached_scores]
        already_scored_mask = posts_df["uri"].isin(previously_scored_post_uris)
        return PostsSplitByScoringStatus(
            already_scored=posts_df[already_scored_mask].copy(),  # type: ignore[reportUnknownReturnType]
            not_scored_yet=posts_df[~already_scored_mask].copy(),  # type: ignore[reportUnknownReturnType]
        )

    def _calculate_post_scores(
        self,
        posts_pending_scoring: pd.DataFrame,
        superposter_dids: set[str],
        feed_config: FeedConfig,
    ) -> list[PostScoreByAlgorithm]:
        post_scores: list[PostScoreByAlgorithm] = []

        for _, post in posts_pending_scoring.iterrows():
            score_by_algorithm: PostScoreByAlgorithm = calculate_post_score(
                post=post,
                superposter_dids=superposter_dids,
                feed_config=feed_config,
            )
            post_scores.append(score_by_algorithm)  # type: ignore[reportUnknownReturnType]

        return post_scores

    def _add_scores_to_posts_df(
        self,
        posts_df: pd.DataFrame,
        scores: list[PostScoreByAlgorithm],
    ) -> pd.DataFrame:
        """Add new scores to posts DataFrame.

        Args:
            posts_df: DataFrame containing original posts
            scores: List of PostScoreByAlgorithm.

        Returns:
            DataFrame with posts and scores merged.

        Warns if either posts_df or scores is empty. This can happen and be
        intentional, we just want to log if it does:
        - Case 1: We rerun the job and the posts were already scored. In this
        case, we have no newly scored posts to run `_add_scores_to_posts_df`.
        - Case 2: We are running a job with no cached scores. In this case, we
        have no cached scores to add to the posts dataframe.

        For these cases, the corresponding posts_df should also be empty too (this
        (assuming upstream filtering logic is working correctly.) Therefore, we
        raise a ValueError if there is a scenario where posts_df is empty but
        scores is not empty.
        """
        if len(scores) == 0:
            logger.warning("Cannot add scores: scores list is empty.")
            result_df = posts_df.copy()
            if "engagement_score" not in result_df.columns:
                result_df["engagement_score"] = pd.Series(dtype=float)
            if "treatment_score" not in result_df.columns:
                result_df["treatment_score"] = pd.Series(dtype=float)
            return result_df

        if len(posts_df) == 0:
            raise ValueError("posts_df is empty but scores is not empty.")

        scores_df: pd.DataFrame = pd.DataFrame([score.model_dump() for score in scores])
        posts_with_scores: pd.DataFrame = posts_df.merge(
            scores_df, on="uri", how="inner"
        )
        if len(posts_with_scores) != len(scores):
            logger.warning(f"""
                Mismatch on number of posts and number of scores.
                - {len(scores)} posts scored
                - {len(posts_df)} posts that were due to be scored.
                - {len(posts_with_scores)} posts with scores post-merge
                Should investigate.
                """)
        return posts_with_scores

    def score_posts(
        self,
        posts_df: pd.DataFrame,
        superposter_dids: set[str],
        export_new_scores: bool = True,
    ) -> pd.DataFrame:
        """Score all posts, using cache when available.

        Args:
            posts_df: DataFrame containing posts to score. Must have 'uri' column.
            superposter_dids: Set of superposter DIDs for scoring algorithm.
            export_new_scores: If True, save newly calculated scores to repository.
                Set to False to skip export (useful for testing).

        Returns:
            DataFrame with posts and scores.
        Raises:
            ValueError: If posts_df is empty or missing required columns.
        """
        self._validate_posts_for_scoring(posts_df)

        # Step 1: Load cached scores
        cached_scores: list[PostScoreByAlgorithm] = self.scores_repo.load_cached_scores(
            lookback_days=self.config.default_scoring_lookback_days
        )

        # Step 2: Filter out posts that have already been scored.
        split_posts_by_scoring_status: PostsSplitByScoringStatus = (
            self._split_posts_by_scoring_status(
                posts_df=posts_df, cached_scores=cached_scores
            )
        )
        posts_already_scored: pd.DataFrame = (
            split_posts_by_scoring_status.already_scored
        )
        posts_pending_scoring: pd.DataFrame = (
            split_posts_by_scoring_status.not_scored_yet
        )

        # Step 3: Calculate scores for posts needing new scores.
        newly_calculated_scores: list[PostScoreByAlgorithm] = (
            self._calculate_post_scores(
                posts_pending_scoring=posts_pending_scoring,
                superposter_dids=superposter_dids,
                feed_config=self.config,
            )
        )

        # Step 4: Assemble dataframes with full posts and scores.
        # Create 2 versions of the posts dataframe:
        # 1. Posts from cached scores.
        # 2. Posts with new scores (these will be exported).
        posts_with_cached_scores: pd.DataFrame = self._add_scores_to_posts_df(
            posts_df=posts_already_scored, scores=cached_scores
        )
        posts_with_new_scores: pd.DataFrame = self._add_scores_to_posts_df(
            posts_df=posts_pending_scoring,
            scores=newly_calculated_scores,
        )

        # Step 5: Save new scores to repository (if enabled)
        if len(newly_calculated_scores) > 0 and export_new_scores:
            scores_to_export: list[ScoredPostModel] = self._prepare_scores_for_export(
                posts_with_new_scores=posts_with_new_scores,
            )
            self.scores_repo.save_scores(scores_to_export)

        logger.info(
            f"Scored {len(posts_with_new_scores)} posts. "
            f"{len(posts_with_cached_scores)} retrieved from cache."
        )

        posts_df_with_scores: pd.DataFrame = pd.concat(
            [
                posts_with_cached_scores,
                posts_with_new_scores,
            ]
        )

        return posts_df_with_scores

    def _prepare_scores_for_export(
        self, posts_with_new_scores: pd.DataFrame
    ) -> list[ScoredPostModel]:
        """Prepare new scores for export.

        Args:
            posts_with_new_scores: DataFrame with posts and new scores.

        Returns:
            List of ScoredPostModel.
        """
        scores_to_export: list[ScoredPostModel] = []

        for _, post in posts_with_new_scores.iterrows():
            scores_to_export.append(
                ScoredPostModel(
                    uri=str(post["uri"]),
                    text=str(post["text"]),
                    engagement_score=float(post["engagement_score"]),
                    treatment_score=float(post["treatment_score"]),
                    source=str(post["source"]),
                    scored_timestamp=current_datetime_str,
                )
            )

        return scores_to_export
