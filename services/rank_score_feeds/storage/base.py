"""ABC base class for feed storage adapters."""

from abc import ABC, abstractmethod

from services.rank_score_feeds.models import (
    FeedGenerationSessionAnalytics,
    StoredFeedModel,
)


class FeedStorageAdapter(ABC):
    """ABC base class for feed storage adapters."""

    @abstractmethod
    def write_feeds(self, feeds: list[StoredFeedModel], timestamp: str) -> None:
        """Write feeds to storage."""
        raise NotImplementedError

    @abstractmethod
    def write_feed_generation_session_analytics(
        self,
        feed_generation_session_analytics: FeedGenerationSessionAnalytics,
        timestamp: str,
    ) -> None:
        """Write feed generation session analytics to storage."""
        raise NotImplementedError
