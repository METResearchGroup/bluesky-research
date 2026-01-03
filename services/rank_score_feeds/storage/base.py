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
        """Write feeds to storage.

        Args:
            feeds: List of feeds to write
            timestamp: Timestamp of the feed generation session

        Raises:
            StorageError: If write operation fails
        """
        raise NotImplementedError

    @abstractmethod
    def write_feed_generation_session_analytics(
        self,
        feed_generation_session_analytics: FeedGenerationSessionAnalytics,
        timestamp: str,
    ) -> None:
        """Write feed generation session analytics to storage.

        Args:
            feed_generation_session_analytics: Feed generation session analytics to write
            timestamp: Timestamp of the feed generation session

        Raises:
            StorageError: If write operation fails
        """
        raise NotImplementedError


class FeedTTLAdapter(ABC):
    """Abstract base class for feed TTL (time-to-live) adapters.

    Handles moving old feeds from active storage to cache, keeping
    only the most recent N feeds active.
    """

    @abstractmethod
    def move_to_cache(
        self,
        prefix: str,
        keep_count: int,
        sort_field: str = "Key",
    ) -> None:
        """Move old feeds from active to cache, keeping most recent N.

        Args:
            prefix: Root prefix to search for files (e.g., 'custom_feeds')
            keep_count: Number of most recent files to keep in active
            sort_field: Field to sort by (default: "Key" for S3)

        Raises:
            StorageError: If TTL operation fails
        """
        raise NotImplementedError


class SessionMetadataAdapter(ABC):
    """Abstract base class for session metadata adapters.

    Handles storing feed generation session metadata (timestamp,
    number of feeds generated, etc.).
    """

    @abstractmethod
    def insert_session_metadata(self, metadata: dict) -> None:
        """Insert feed generation session metadata.

        Args:
            metadata: Dictionary containing session metadata:
                - feed_generation_timestamp: str
                - number_of_new_feeds: int

        Raises:
            StorageError: If insert operation fails
        """
        raise NotImplementedError
