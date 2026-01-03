from services.rank_score_feeds.models import (
    FeedWithMetadata,
    FeedGenerationSessionAnalytics,
    StoredFeedModel,
)
from services.rank_score_feeds.repositories.feed_repo import FeedStorageRepository
from services.rank_score_feeds.storage.exceptions import StorageError


class DataExporterService:
    """Service for exporting data to storage."""

    def __init__(self, feed_storage_repository: FeedStorageRepository):
        """Initialize data exporter service."""
        from lib.log.logger import get_logger

        self.logger = get_logger(__name__)
        if not isinstance(feed_storage_repository, FeedStorageRepository):
            raise ValueError(
                "feed_storage_repository must be a subclass of FeedStorageRepository."
            )
        self.feed_storage_repository = feed_storage_repository

    def export_feeds(
        self, user_to_ranked_feed_map: dict[str, FeedWithMetadata], timestamp: str
    ) -> None:
        """Exports feeds."""
        try:
            transformed_feed_models: list[StoredFeedModel] = (
                self._transform_feed_with_metadata_to_export_models(
                    user_to_ranked_feed_map=user_to_ranked_feed_map,
                    timestamp=timestamp,
                )
            )
            self.feed_storage_repository.write_feeds(
                feeds=transformed_feed_models,
                timestamp=timestamp,
            )
        except Exception as e:
            self.logger.error(f"Failed to export feeds: {e}")
            raise StorageError(f"Failed to export feeds: {e}")

    def export_feed_generation_session_analytics(
        self,
        feed_generation_session_analytics: FeedGenerationSessionAnalytics,
        timestamp: str,
    ) -> None:
        """Exports feed generation session analytics."""
        try:
            self.feed_storage_repository.write_feed_generation_session_analytics(
                feed_generation_session_analytics=feed_generation_session_analytics,
                timestamp=timestamp,
            )
        except Exception as e:
            self.logger.error(
                f"Failed to export feed generation session analytics: {e}"
            )
            raise StorageError(
                f"Failed to export feed generation session analytics: {e}"
            )

    def _transform_feed_with_metadata_to_export_models(
        self, user_to_ranked_feed_map: dict[str, FeedWithMetadata], timestamp: str
    ) -> list[StoredFeedModel]:
        stored_feed_models: list[StoredFeedModel] = []
        for _, feed_with_metadata in user_to_ranked_feed_map.items():
            stored_feed_models.append(
                StoredFeedModel(
                    feed_id=f"{feed_with_metadata.user_did}::{timestamp}",
                    user=feed_with_metadata.user_did,
                    bluesky_handle=feed_with_metadata.bluesky_handle,
                    bluesky_user_did=feed_with_metadata.user_did,
                    condition=feed_with_metadata.condition,
                    feed_statistics=feed_with_metadata.feed_statistics,
                    feed=feed_with_metadata.feed,
                    feed_generation_timestamp=timestamp,
                )
            )
        return stored_feed_models
