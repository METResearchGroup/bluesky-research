from services.backfill.exceptions import CacheBufferWriterServiceError
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
        source_data_location: str = "local",
        data_repository: BackfillDataRepository | None = None,
        queue_manager_service: QueueManagerService | None = None,
    ):
        """Initialize the cache buffer writer service.

        Args:
            source_data_location: Data source location ("local" or "s3"). Used to
                construct BackfillDataRepository if not provided.
            data_repository: Optional BackfillDataRepository instance for dependency injection.
                If not provided, creates a new instance using source_data_location.
            queue_manager_service: Optional QueueManagerService instance for dependency injection.
                If not provided, creates a new instance.
        """
        if data_repository is None:
            data_repository = BackfillDataRepository.from_source_data_location(
                source_data_location
            )
        self.data_repository = data_repository
        self.queue_manager_service = queue_manager_service or QueueManagerService()

    def write_cache(self, integration_name: str) -> None:
        """Writes the cache buffer to permanent storage for the given integration.

        Loads all records from the output queue for the specified integration
        and writes them to permanent storage via the data repository.

        Args:
            integration_name: The name of the integration to write the cache for.

        Raises:
            CacheBufferWriterServiceError: If loading records from the queue or
                writing to storage fails.
        """
        try:
            records: list[dict] = self.queue_manager_service.load_records_from_queue(
                integration_name=integration_name
            )
            self.data_repository.write_records_to_storage(
                integration_name=integration_name, records=records
            )
            logger.info(
                f"Successfully wrote {len(records)} records to storage for integration {integration_name}"
            )
        except Exception as e:
            logger.error(f"Error writing cache for integration {integration_name}: {e}")
            raise CacheBufferWriterServiceError(
                f"Error writing cache for integration {integration_name}: {e}"
            ) from e

    def clear_cache(self, integration_name: str) -> None:
        """Clears the cache for the given integration by loading IDs and deleting them.

        This is an efficient implementation that only loads queue item IDs
        (not the full payload data) before deleting them.

        Args:
            integration_name: The name of the integration to clear the cache for.
        """
        try:
            logger.info(f"Loading queue item IDs for integration {integration_name}...")
            ids: list[int] = self.queue_manager_service.load_queue_item_ids(
                integration_name=integration_name, queue_type="output"
            )
            if len(ids) == 0:
                logger.info(
                    f"No queue items to delete for integration {integration_name}"
                )
                return

            logger.info(
                f"Deleting {len(ids)} queue item IDs for integration {integration_name}..."
            )
            self.queue_manager_service.delete_records_from_queue(
                integration_name=integration_name,
                queue_type="output",
                queue_ids_to_delete=ids,
            )
            logger.info(
                f"Successfully deleted {len(ids)} queue items for integration {integration_name}"
            )
        except Exception as e:
            logger.error(
                f"Error clearing cache for integration {integration_name}: {e}"
            )
            raise CacheBufferWriterServiceError(
                f"Error clearing cache for integration {integration_name}: {e}"
            ) from e
