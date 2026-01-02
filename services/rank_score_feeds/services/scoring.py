"""Service for scoring posts with caching support."""

import pandas as pd

from lib.constants import current_datetime_str
from lib.log.logger import get_logger
from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.models import (
    PostScoreByAlgorithm,
    ScoredPostModel,
    PostsSplitByScoringStatus,
)
from services.rank_score_feeds.repositories.scores_repo import ScoresRepositoryProtocol
from services.rank_score_feeds.scoring import calculate_post_score

logger = get_logger(__name__)


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
