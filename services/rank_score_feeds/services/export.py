from services.rank_score_feeds.models import FeedWithMetadata, FeedGenerationSessionAnalytics, StoredFeedModel


# TODO: for I/O, probably a good spot for the Repository pattern? That way, I can decouple
# the export.py from any lower-level specifics of I/O? Also can include adapters for S3 and local storage.
class DataExporterService:
    def __init__(self):
        pass

    def export_feeds(
        self,
        user_to_ranked_feed_map: dict[str, FeedWithMetadata],
        timestamp: str
    ) -> None:
        """Exports feeds."""
        transformed_feed_models: list[StoredFeedModel] = self._transform_feed_with_metadata_to_export_models(
            user_to_ranked_feed_map=user_to_ranked_feed_map,
            timestamp=timestamp,
        )
        partition_date = "" # TODO: should really get this from a helper.


    def _transform_feed_with_metadata_to_export_models(
        self,
        user_to_ranked_feed_map: dict[str, FeedWithMetadata],
        timestamp: str
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

    def export_feed_generation_session_analytics(
        self,
        feed_generation_session_analytics: FeedGenerationSessionAnalytics,
        timestamp: str
    ) -> None:
        """Exports feed generation session analytics."""
        pass
