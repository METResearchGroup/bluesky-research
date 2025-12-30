"""Service for scoring posts with caching support."""

import pandas as pd

from lib.log.logger import get_logger
from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.models import ScoredPosts
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

    def score_posts(
        self,
        posts_df: pd.DataFrame,
        superposter_dids: set[str],
        save_new_scores: bool = True,
    ) -> ScoredPosts:
        """Score all posts, using cache when available.

        Args:
            posts_df: DataFrame containing posts to score. Must have 'uri' column.
            superposter_dids: Set of superposter DIDs for scoring algorithm.
            save_new_scores: If True, save newly calculated scores to repository.
                Set to False to skip export (useful for testing).

        Returns:
            ScoredPosts containing:
                - posts_df: DataFrame with added 'engagement_score' and 'treatment_score' columns
                - new_post_uris: List of URIs for posts that were newly scored (not cached)

        Raises:
            ValueError: If posts_df is empty or missing required columns.
        """
        self._validate_posts_for_scoring(posts_df)

        total_posts = len(posts_df)
        logger.info(f"Scoring {total_posts} posts.")

        # Step 1: Load cached scores
        cached_scores = self.scores_repo.load_cached_scores(
            lookback_days=self.config.default_scoring_lookback_days
        )

        # Step 2: Calculate scores (cached or new)
        post_scores: list[dict[str, float]] = []
        new_post_uris: list[str] = []

        for _, post in posts_df.iterrows():
            uri = post["uri"]

            if uri in cached_scores:
                # Use cached score
                post_scores.append(cached_scores[uri])
            else:
                # Calculate new score
                score = calculate_post_score(
                    post=post,
                    superposter_dids=superposter_dids,
                    feed_config=self.config,
                )
                post_scores.append(score)
                new_post_uris.append(uri)

        # Step 3: Add scores to DataFrame
        engagement_scores = [score["engagement_score"] for score in post_scores]
        treatment_scores = [score["treatment_score"] for score in post_scores]

        posts_df = posts_df.copy()  # Avoid mutating input
        posts_df["engagement_score"] = engagement_scores
        posts_df["treatment_score"] = treatment_scores

        # Step 4: Save new scores to repository (if enabled)
        if new_post_uris and save_new_scores:
            scores_to_export = self._prepare_scores_for_export(
                posts_df=posts_df,
                new_post_uris=new_post_uris,
            )
            self.scores_repo.save_scores(scores_to_export)

        total_new_scores = len(new_post_uris)
        logger.info(
            f"Scored {total_posts} posts. "
            f"{total_new_scores} new scores calculated, "
            f"{total_posts - total_new_scores} retrieved from cache."
        )

        return ScoredPosts(
            posts_df=posts_df,
            new_post_uris=new_post_uris,
        )

    def _prepare_scores_for_export(
        self,
        posts_df: pd.DataFrame,
        new_post_uris: list[str],
    ) -> list[dict]:
        """Prepare new scores for export.

        Args:
            posts_df: DataFrame with scored posts.
            new_post_uris: List of URIs for newly scored posts.

        Returns:
            List of dictionaries ready for repository export.
        """
        scores_to_export = posts_df[posts_df["uri"].isin(new_post_uris)][
            ["uri", "text", "source", "engagement_score", "treatment_score"]
        ].to_dict("records")

        return scores_to_export  # type: ignore[return-value]
