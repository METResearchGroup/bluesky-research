"""Service for enqueuing records."""

from lib.log.logger import get_logger
from services.backfill.models import (
    EnqueueServicePayload,
    PostScope,
    PostToEnqueueModel,
)
from services.backfill.exceptions import EnqueueServiceError
from services.backfill.services.backfill_data_loader_service import (
    BackfillDataLoaderService,
)
from services.backfill.services.queue_manager_service import QueueManagerService

logger = get_logger(__name__)


class EnqueueService:
    def __init__(self):
        self.backfill_data_loader_service = BackfillDataLoaderService()
        self.queue_manager_service = QueueManagerService()

    def enqueue_records(self, payload: EnqueueServicePayload):
        try:
            post_scope = PostScope(payload.record_type)
            for integration_name in payload.integrations:
                self._enqueue_records_for_single_integration(
                    integration_name=integration_name,
                    post_scope=post_scope,
                    start_date=payload.start_date,
                    end_date=payload.end_date,
                )
            logger.info("Enqueuing records completed successfully.")
        except Exception as e:
            logger.error(f"Error enqueuing records: {e}")
            raise EnqueueServiceError(f"Error enqueuing records: {e}") from e

    def _enqueue_records_for_single_integration(
        self,
        integration_name: str,
        post_scope: PostScope,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> None:
        """Enqueue records for a single integration."""
        posts: list[PostToEnqueueModel] = self._load_posts_to_enqueue(
            integration_name=integration_name,
            post_scope=post_scope,
            start_date=start_date,
            end_date=end_date,
        )
        self.queue_manager_service.insert_posts_to_queue(
            integration_name=integration_name,
            posts=posts,
        )

    def _load_posts_to_enqueue(
        self,
        integration_name: str,
        post_scope: PostScope,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[PostToEnqueueModel]:
        return self.backfill_data_loader_service.load_posts_to_enqueue(
            integration_name=integration_name,
            post_scope=post_scope,
            start_date=start_date,
            end_date=end_date,
        )
