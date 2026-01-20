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

logger = get_logger(__file__)


class EnqueueService:
    def __init__(
        self,
        source_data_location: str = "local",
        backfill_data_loader_service: BackfillDataLoaderService | None = None,
        queue_manager_service: QueueManagerService | None = None,
    ):
        """Initialize EnqueueService.

        Args:
            source_data_location: Data source location ("local" or "s3"). Used to
                construct BackfillDataLoaderService if not provided.
            backfill_data_loader_service: Optional BackfillDataLoaderService instance.
                If not provided, creates one using the source_data_location.
            queue_manager_service: Optional QueueManagerService instance.
                If not provided, creates a new instance.
        """
        if backfill_data_loader_service is None:
            from services.backfill.repositories.repository import (
                BackfillDataRepository,
            )

            repo = BackfillDataRepository.from_source_data_location(
                source_data_location
            )
            backfill_data_loader_service = BackfillDataLoaderService(
                data_repository=repo
            )
        self.backfill_data_loader_service = backfill_data_loader_service
        self.queue_manager_service = queue_manager_service or QueueManagerService()

    def enqueue_records(self, payload: EnqueueServicePayload) -> None:
        """Enqueue records for all specified integrations.

        Args:
            payload: Configuration for enqueuing records. Already validated by Pydantic.

        Raises:
            EnqueueServiceError: If enqueuing fails for any reason.
        """
        try:
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
        if len(posts) == 0:
            logger.warning(f"No posts to enqueue for integration: {integration_name}")
            return
        self.queue_manager_service.insert_posts_to_queue(
            integration_name=integration_name,
            posts=posts,
        )
