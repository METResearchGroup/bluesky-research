"""Service for orchestrating feed generation."""

import pandas as pd

from services.participant_data.models import UserToBlueskyProfileModel
from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.models import CustomFeedPost
from services.rank_score_feeds.services.ranking import RankingService
from services.rank_score_feeds.services.reranking import RerankingService


class FeedGenerationService:
    """Orchestrates feed generation for individual users."""

    def __init__(
        self,
        ranking_service: RankingService,
        reranking_service: RerankingService,
        feed_config: FeedConfig,
    ):
        """Initialize with ranking/reranking services and config.
        
        Args:
            ranking_service: Service for initial ranking.
            reranking_service: Service for re-ranking with business rules.
            feed_config: Configuration for feed generation algorithm.
        """
        self.ranking_service = ranking_service
        self.reranking_service = reranking_service
        self.config = feed_config

    def generate_feed_for_user(
        self,
        user: UserToBlueskyProfileModel,
        candidate_pool: pd.DataFrame,
        in_network_post_uris: list[str],
        previous_post_uris: set[str],
    ) -> dict:
        """Generate complete feed: ranking → reranking → statistics.
        
        Args:
            user: User to generate feed for.
            candidate_pool: Candidate pool to rank from.
            in_network_post_uris: In-network post URIs for this user.
            previous_post_uris: Previous feed URIs for freshness filtering.
        
        Returns:
            Dictionary containing feed and metadata.
        """
        # TODO: Implement
        raise NotImplementedError