"""Service for ranking candidates."""

import pandas as pd

from services.participant_data.models import UserToBlueskyProfileModel
from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.models import CustomFeedPost


class RankingService:
    """Handles initial ranking of candidates."""

    def __init__(self, feed_config: FeedConfig):
        """Initialize with feed configuration.
        
        Args:
            feed_config: Configuration for feed generation algorithm.
        """
        self.config = feed_config

    def rank_candidates(
        self,
        user: UserToBlueskyProfileModel,
        candidate_pool: pd.DataFrame,
        in_network_post_uris: list[str],
    ) -> list[CustomFeedPost]:
        """Rank candidates to create initial feed.
        
        Args:
            user: User model with condition information.
            candidate_pool: Candidate pool to rank from.
            in_network_post_uris: List of in-network post URIs for personalization.
        
        Returns:
            List of ranked candidate posts.
        """
        # TODO: Implement
        raise NotImplementedError

