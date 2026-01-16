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

    def enqueue_records(self, payload: EnqueueServicePayload) -> None:
        try:
            self._validate_payload(payload=payload)
            post_scope = PostScope(payload.record_type)
            total_integrations: int = len(payload.integrations)
            for i, integration_name in enumerate(payload.integrations):
                logger.info(
                    f"[Progress: {i+1}/{total_integrations}] Enqueuing records for integration: {integration_name}"
                )
                self._enqueue_records_for_single_integration(
                    integration_name=integration_name,
                    post_scope=post_scope,
                    start_date=payload.start_date,
                    end_date=payload.end_date,
                )
                logger.info(
                    f"[Progress: {i+1}/{total_integrations}] Completed enqueuing records for integration: {integration_name}"
                )
            logger.info(
                f"[Progress: {total_integrations}/{total_integrations}] Enqueuing records completed successfully."
            )
        except Exception as e:
            logger.error(f"Error enqueuing records: {e}")
            raise EnqueueServiceError(f"Error enqueuing records: {e}") from e

    def _enqueue_records_for_single_integration(
        self,
        integration_name: str,
        post_scope: PostScope,
        start_date: str,
        end_date: str,
    ) -> None:
        """Enqueue records for a single integration.

        NOTE: we're OK with loading all posts in memory for now. It's not caused
        any issues with our implementation. If OOM becomes a problem, we will
        consider using generators or chunking.
        """
        posts: list[PostToEnqueueModel] = (
            self.backfill_data_loader_service.load_posts_to_enqueue(
                integration_name=integration_name,
                post_scope=post_scope,
                start_date=start_date,
                end_date=end_date,
            )
        )
        self.queue_manager_service.insert_posts_to_queue(
            integration_name=integration_name,
            posts=posts,
        )

    def _validate_payload(self, payload: EnqueueServicePayload) -> None:
        """Runs validation checks on the payload.

        We do validation independent of the CLI app in app.py to allow for
        service-level validation AND to allow for this app to be run independent
        of the CLI app.
        """
        if payload.record_type not in [
            PostScope.ALL_POSTS.value,
            PostScope.FEED_POSTS.value,
        ]:
            raise ValueError(
                f"Invalid record type: {payload.record_type}"
            ) from ValueError(f"Invalid record type: {payload.record_type}")
        if payload.integrations is None or len(payload.integrations) == 0:
            raise ValueError("Integrations list is empty") from ValueError(
                "Integrations list is empty"
            )
        if payload.start_date is None or payload.end_date is None:
            raise ValueError("Start and end date are required") from ValueError(
                "Start and end date are required"
            )
        if payload.start_date >= payload.end_date:
            raise ValueError("Start date must be before end date") from ValueError(
                "Start date must be before end date"
            )
