"""Service for enqueuing records."""

import hashlib

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


def _deterministically_sample_posts_by_uri(
    posts: list[PostToEnqueueModel], sample_proportion: float
) -> list[PostToEnqueueModel]:
    """Deterministically sample posts by hashing `uri`.

    This is intended to produce a stable sample across:
    - integrations (sampling is applied once on a shared base set)
    - machines/runs (no RNG state/seed required)
    """
    if sample_proportion <= 0.0:
        return []
    if sample_proportion >= 1.0:
        return posts

    sampled: list[PostToEnqueueModel] = []
    denom = float(2**64)
    for post in posts:
        digest = hashlib.sha256(post.uri.encode("utf-8")).digest()
        bucket = int.from_bytes(digest[:8], "big") / denom
        if bucket < sample_proportion:
            sampled.append(post)
    return sampled


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
            base_posts: list[PostToEnqueueModel] = (
                self.backfill_data_loader_service.load_posts_by_scope(
                    post_scope=post_scope,
                    start_date=payload.start_date,
                    end_date=payload.end_date,
                )
            )
            logger.info(f"Loaded {len(base_posts)} base posts (scope={post_scope}).")

            sampled_base_posts: list[PostToEnqueueModel] = base_posts
            if payload.sample_records:
                sampled_base_posts = _deterministically_sample_posts_by_uri(
                    posts=base_posts,
                    sample_proportion=float(payload.sample_proportion or 1.0),
                )
                logger.info(
                    f"Sampled base posts (proportion={payload.sample_proportion}): "
                    f"{len(base_posts)} -> {len(sampled_base_posts)}"
                )

            total_integrations: int = len(payload.integrations)
            for i, integration_name in enumerate(payload.integrations):
                logger.info(
                    f"[Progress: {i+1}/{total_integrations}] Enqueuing records for integration: {integration_name}"
                )
                posts_to_enqueue = self.backfill_data_loader_service.filter_out_previously_classified_posts(
                    posts=sampled_base_posts,
                    integration_name=integration_name,
                    start_date=payload.start_date,
                    end_date=payload.end_date,
                )
                if len(posts_to_enqueue) == 0:
                    logger.warning(
                        f"No posts to enqueue for integration: {integration_name}"
                    )
                    continue
                self.queue_manager_service.insert_posts_to_queue(
                    integration_name=integration_name,
                    posts=posts_to_enqueue,
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
