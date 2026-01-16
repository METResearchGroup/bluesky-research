"""Service for enqueuing records."""

from lib.log.logger import get_logger
from services.backfill.models import EnqueueServicePayload, PostToEnqueueModel
from services.backfill.exceptions import EnqueueServiceError
from services.backfill.services.backfill_data_loader_service import (
    BackfillDataLoaderService,
)

logger = get_logger(__name__)


class EnqueueService:
    def __init__(self):
        self.enqueue_posts_service = EnqueuePostsService()
        self.enqueue_posts_used_in_feeds_service = EnqueuePostsUsedInFeedsService()

    def enqueue_records(self, payload: EnqueueServicePayload):
        try:
            if payload.record_type == "posts":
                self._enqueue_posts(payload)
            elif payload.record_type == "posts_used_in_feeds":
                self._enqueue_posts_used_in_feeds(payload)
            else:
                raise ValueError(f"Invalid record type: {payload.record_type}")
        except Exception as e:
            logger.error(f"Error enqueuing records: {e}")
            raise EnqueueServiceError(f"Error enqueuing records: {e}") from e
        logger.info("Records enqueued successfully.")

    def _enqueue_posts(self, payload: EnqueueServicePayload):
        self.enqueue_posts_service.enqueue_records(payload)

    def _enqueue_posts_used_in_feeds(self, payload: EnqueueServicePayload):
        self.enqueue_posts_used_in_feeds_service.enqueue_records(payload)


class EnqueuePostsService:
    """Service for enqueuing posts for a given integration."""

    def __init__(self):
        self.backfill_data_loader_service = BackfillDataLoaderService()

    def enqueue_records(self, payload: EnqueueServicePayload):
        logger.info(
            f"Loading posts to enqueue for integrations: {payload.integrations}..."
        )
        for integration_name in payload.integrations:
            posts: list[PostToEnqueueModel] = self._load_posts_to_enqueue(
                integration_name=integration_name,
                start_date=payload.start_date,
                end_date=payload.end_date,
            )
            self._insert_posts_to_queue(integration_name=integration_name, posts=posts)
        logger.info(
            f"Finished enqueuing posts for integrations: {payload.integrations}."
        )

    def _load_posts_to_enqueue(
        self,
        integration_name: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[PostToEnqueueModel]:
        # TODO: loop load posts and add posts to be integration-specific
        # (since not all URIs will be shared across all integrations).
        # TODO: delegate to shared dataloader service.
        logger.info(f"Loading posts to enqueue for {integration_name}...")
        # TODO: need to tell to dataloader service that we want all posts,
        # not just posts used in feeds.
        posts: list[PostToEnqueueModel] = (
            self.backfill_data_loader_service.load_posts_to_enqueue(
                integration_name=integration_name,
                start_date=start_date,
                end_date=end_date,
            )
        )
        logger.info(f"Loaded {len(posts)} posts to enqueue for {integration_name}.")
        return posts

    def _insert_posts_to_queue(
        self, integration_name: str, posts: list[PostToEnqueueModel]
    ) -> None:
        pass


class EnqueuePostsUsedInFeedsService:
    def enqueue_records(self, payload: EnqueueServicePayload):
        pass
