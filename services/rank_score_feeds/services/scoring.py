"""Service for scoring posts with caching support."""

import pandas as pd

from lib.constants import current_datetime_str
from lib.log.logger import get_logger
from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.models import (
    PostScoreByAlgorithm,
    ScoredPostModel,
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

    def _filter_posts_already_scored(
        self, posts_df: pd.DataFrame, cached_scores: list[PostScoreByAlgorithm]
    ) -> pd.DataFrame:
        """Filter out posts that have already been scored.

        Args:
            posts_df: DataFrame containing posts to score. Must have 'uri' column.
            cached_scores: List of PostScoreByAlgorithm.
        """
        previously_scored_post_uris: list[str] = [score.uri for score in cached_scores]
        return posts_df[~posts_df["uri"].isin(previously_scored_post_uris)]  # type: ignore[reportUnknownReturnType]

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
                feed_config=self.config,
            )
            post_scores.append(score_by_algorithm)  # type: ignore[reportUnknownReturnType]

        return post_scores

    def _add_new_scores_to_posts_df(
        self,
        posts_pending_scoring: pd.DataFrame,
        newly_calculated_scores: list[PostScoreByAlgorithm],
    ) -> pd.DataFrame:
        """Add new scores to posts DataFrame.

        Args:
            posts_pending_scoring: DataFrame containing posts to score. Must have 'uri' column.
            newly_calculated_scores: List of PostScoreByAlgorithm.
        """
        # Convert list of Pydantic models to list of dicts before creating DataFrame
        newly_calculated_scores_df: pd.DataFrame = pd.DataFrame(
            [score.model_dump() for score in newly_calculated_scores]
        )
        posts_with_scores: pd.DataFrame = posts_pending_scoring.merge(
            newly_calculated_scores_df, on="uri", how="inner"
        )
        if len(posts_with_scores) != len(posts_pending_scoring):
            logger.warning(f"""
                Mismatch on number of posts and number of scores.
                - {len(newly_calculated_scores)} posts scored
                - {len(posts_pending_scoring)} posts that were due to be scored.
                - {len(posts_with_scores)} posts with scores post-merge
                Should investigate.
                """)
        return posts_with_scores

    def score_posts(
        self,
        posts_df: pd.DataFrame,
        superposter_dids: set[str],
        save_new_scores: bool = True,
    ) -> list[PostScoreByAlgorithm]:
        """Score all posts, using cache when available.

        Args:
            posts_df: DataFrame containing posts to score. Must have 'uri' column.
            superposter_dids: Set of superposter DIDs for scoring algorithm.
            save_new_scores: If True, save newly calculated scores to repository.
                Set to False to skip export (useful for testing).

        Returns:
            List of PostScoreByAlgorithm.
        Raises:
            ValueError: If posts_df is empty or missing required columns.
        """
        self._validate_posts_for_scoring(posts_df)

        total_posts = len(posts_df)
        logger.info(f"Scoring {total_posts} posts.")

        # Step 1: Load cached scores
        cached_scores: list[PostScoreByAlgorithm] = self.scores_repo.load_cached_scores(
            lookback_days=self.config.default_scoring_lookback_days
        )

        # Step 2: Filter out posts that have already been scored.
        posts_pending_scoring: pd.DataFrame = self._filter_posts_already_scored(
            posts_df=posts_df, cached_scores=cached_scores
        )

        # Step 3: Calculate scores for posts needing new scores.
        newly_calculated_scores: list[PostScoreByAlgorithm] = (
            self._calculate_post_scores(
                posts_pending_scoring=posts_pending_scoring,
                superposter_dids=superposter_dids,
                feed_config=self.config,
            )
        )

        # Step 4: Add scores to DataFrame
        posts_with_new_scores: pd.DataFrame = self._add_new_scores_to_posts_df(
            posts_pending_scoring=posts_pending_scoring,
            newly_calculated_scores=newly_calculated_scores,
        )

        # Step 5: Save new scores to repository (if enabled)
        if len(newly_calculated_scores) > 0 and save_new_scores:
            scores_to_export: list[ScoredPostModel] = self._prepare_scores_for_export(
                posts_with_new_scores=posts_with_new_scores,
            )
            self.scores_repo.save_scores(scores_to_export)

        total_new_scores = len(posts_with_new_scores)
        logger.info(
            f"Scored {total_posts} posts. "
            f"{total_new_scores} new scores calculated, "
            f"{total_posts - total_new_scores} retrieved from cache."
        )

        return cached_scores + newly_calculated_scores

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
