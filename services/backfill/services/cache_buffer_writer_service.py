from services.backfill.repositories.adapters import LocalStorageAdapter
from services.backfill.repositories.repository import BackfillDataRepository
from services.backfill.services.queue_manager_service import QueueManagerService
from lib.log.logger import get_logger

logger = get_logger(__file__)


class CacheBufferWriterService:
    """Manages writing cache buffers to the databases.

    We decouple writing the cache to permanent storage from
    clearing the cache. However, in our setup, we do the writes
    first and then clear the cache. This assumes that whatever
    was in the cache at the time of running clear_cache was already
    written to permanent storage. This is a known limitation and
    assumption that works for our use case. But it can fail for cases
    like multiple concurrent runs all trying to write the cache to
    permanent storage and then clear it. For our use case where this
    is run as a single-threaded application, this is OK.
    """

    def __init__(
        self,
        data_repository: BackfillDataRepository | None = None,
        queue_manager_service: QueueManagerService | None = None,
    ):
        """Initialize the cache buffer writer service.

        Args:
            data_repository: Optional BackfillDataRepository instance for dependency injection.
                              If not provided, creates a new instance using LocalStorageAdapter.
            queue_manager_service: Optional QueueManagerService instance for dependency injection.
                                  If not provided, creates a new instance.
        """
        self.data_repository = data_repository or BackfillDataRepository(
            adapter=LocalStorageAdapter()
        )
        self.queue_manager_service = queue_manager_service or QueueManagerService()

    def write_cache(self, service: str):
        # TODO: delegate to a queue service AND to the repo.
        # TODO: update BackfillDataRepository and the adapters
        # to do the writing component.
        pass

    def clear_cache(self, service: str):
        """Clears the cache for the given service by loading IDs and deleting them.

        This is an efficient implementation that only loads queue item IDs
        (not the full payload data) before deleting them.

        Args:
            service: The name of the service/integration to clear the cache for.
        """
        try:
            logger.info(f"Loading queue item IDs for service {service}...")
            ids: list[int] = self.queue_manager_service.load_queue_item_ids(
                integration_name=service, queue_type="output"
            )
            if len(ids) == 0:
                logger.info(f"No queue items to delete for service {service}")
                return

            logger.info(f"Deleting {len(ids)} queue item IDs for service {service}...")
            self.queue_manager_service.delete_records_from_queue(
                integration_name=service,
                queue_type="output",
                queue_ids_to_delete=ids,
            )
            logger.info(
                f"Successfully deleted {len(ids)} queue items for service {service}"
            )
        except Exception as e:
            logger.error(f"Error clearing cache for service {service}: {e}")
            raise
