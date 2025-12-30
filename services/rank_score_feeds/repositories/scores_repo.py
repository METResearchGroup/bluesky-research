"""Repository for loading and saving cached post scores."""

from datetime import timedelta
from typing import Protocol

import pandas as pd

from lib.constants import current_datetime
from lib.db.manage_local_data import (
    export_data_to_local_storage,
    load_data_from_local_storage,
)
from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from lib.log.logger import get_logger
from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.models import PostScoreByAlgorithm, ScoredPostModel

logger = get_logger(__name__)


class ScoresRepositoryProtocol(Protocol):
    """Protocol defining the scores repository interface.

    This protocol enables dependency injection and easy testing with mocks.
    """

    def load_cached_scores(
        self,
        lookback_days: int | None = None,
    ) -> list[PostScoreByAlgorithm]:
        """Load cached scores from storage.

        Args:
            lookback_days: Number of days to look back. If None, uses config default.

        Returns:
            List of PostScoreByAlgorithm. Empty list if no cached scores.
        """
        ...

    def save_scores(self, scores: list[ScoredPostModel]) -> None:
        """Save new scores to storage.

        Args:
            scores: List of ScoredPostModel.
        """
        ...


class ScoresRepository:
    """Repository implementation for post score caching.

    Handles loading cached scores from local storage and saving new scores.
    This isolates storage concerns from scoring logic.
    """

    def __init__(self, feed_config: FeedConfig):
        """Initialize repository with feed configuration.

        Args:
            feed_config: Configuration containing lookback defaults.
        """
        self.config = feed_config

    def load_cached_scores(
        self,
        lookback_days: int | None = None,
    ) -> list[PostScoreByAlgorithm]:
        """Load cached scores from local storage.

        Args:
            lookback_days: Number of days to look back. If None, uses
                feed_config.default_scoring_lookback_days.

        Returns:
            List of PostScoreByAlgorithm. Empty list if no cached scores.
        """
        if lookback_days is None:
            lookback_days = self.config.default_scoring_lookback_days

        lookback_timestamp = (
            current_datetime - timedelta(days=lookback_days)
        ).strftime("%Y-%m-%d")
        logger.info(f"Loading previous post scores from {lookback_timestamp}.")

        try:
            cached_post_scores_df: pd.DataFrame = load_data_from_local_storage(
                service="post_scores",
                directory="active",
                latest_timestamp=lookback_timestamp,
            )
            cached_post_scores_df = (
                cached_post_scores_df.sort_values(
                    by="scored_timestamp", ascending=False
                )
                .drop_duplicates(subset="uri", keep="first")
                .dropna(subset=["engagement_score", "treatment_score"])
            )

            previously_scored_posts: list[PostScoreByAlgorithm] = []

            for _, row in cached_post_scores_df.iterrows():
                previously_scored_posts.append(
                    PostScoreByAlgorithm(
                        uri=str(row["uri"]),
                        engagement_score=float(row["engagement_score"]),
                        treatment_score=float(row["treatment_score"]),
                    )
                )

            logger.info(
                f"Loaded {len(previously_scored_posts)} cached scores from storage."
            )
            return previously_scored_posts

        except Exception as e:
            logger.warning(f"Failed to load cached scores: {e}. Returning empty cache.")
            return []

    def save_scores(self, scores: list[ScoredPostModel]) -> None:
        """Save new scores to local storage.

        Args:
            scores: List of ScoredPostModel.
        """
        if not scores:
            logger.warning("No scores to save. Skipping export.")
            return

        logger.info(f"Saving {len(scores)} new post scores to storage.")

        # Convert to DataFrame with proper types
        score_dicts: list[dict] = [post.model_dump() for post in scores]
        dtypes_map = MAP_SERVICE_TO_METADATA["post_scores"]["dtypes_map"]
        score_df: pd.DataFrame = pd.DataFrame(score_dicts)

        if "partition_date" not in score_df.columns:
            score_df["partition_date"] = pd.to_datetime(
                score_df["scored_timestamp"]
            ).dt.date

        score_df = score_df.astype(dtypes_map)

        # Export to local storage
        export_data_to_local_storage(df=score_df, service="post_scores")

        logger.info(f"Successfully saved {len(scores)} scores to storage.")
