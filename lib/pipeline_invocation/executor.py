"""Pipeline execution with dependency injection for testability."""

from typing import Callable, Optional

from lib.aws.dynamodb import DynamoDB
from lib.helper import RUN_MODE, track_performance
from lib.log.logger import get_logger
from lib.metadata.models import RunExecutionMetadata
from lib.pipeline_invocation.constants import dynamodb_table_name
from lib.pipeline_invocation.errors import MetadataWriteError, ObservabilityError
from lib.pipeline_invocation.models import IntegrationRequest
from lib.pipeline_invocation.registry import PipelineHandlerRegistry
from tenacity import retry, stop_after_attempt, wait_exponential

logger = get_logger(__file__)

dynamodb = DynamoDB()


# Default implementation factories (lazy loading to avoid circular imports)
def _default_wandb_logger() -> Callable[[str, dict], None]:
    """Factory for default WandB logger implementation.

    Returns a no-op logger if wandb is not installed (e.g., in test environments).
    """
    try:
        import wandb
    except ImportError:
        # Return no-op logger if wandb not available (e.g., in test environments)
        def log_to_wandb_noop(service: str, metadata: dict) -> None:
            logger.debug(f"WandB not available, skipping logging for {service}")
            return None

        return log_to_wandb_noop

    def log_to_wandb(service: str, metadata: dict) -> None:
        """Log metadata to WandB."""
        if RUN_MODE == "test":
            return
        try:
            # Only initialize if no active run exists to avoid creating orphaned runs
            if wandb.run is None:
                wandb.init(project=service)
            wandb.log(metadata)
        except Exception as e:
            logger.warning(f"Failed to log to WandB (non-critical): {e}")
            raise ObservabilityError(service, "wandb", e)

    return log_to_wandb


def _default_load_metadata() -> Callable[[str], dict]:
    """Factory for default metadata loader."""
    # Import here to avoid circular imports
    return load_latest_run_metadata


def _default_write_metadata() -> Callable[[RunExecutionMetadata], None]:
    """Factory for default metadata writer."""
    # Import here to avoid circular imports
    return write_run_metadata_to_db


@track_performance
def run_integration_service(
    request: IntegrationRequest,
    previous_run_metadata: Optional[dict] = None,
) -> RunExecutionMetadata:
    """Runs an integration service."""
    service = request.service
    payload = request.payload
    if not previous_run_metadata:
        previous_run_metadata = {}
    service_handler: Callable = PipelineHandlerRegistry.get_handler(service)
    run_metadata = service_handler(event=payload, context=None)
    transformed_run_metadata = RunExecutionMetadata(**run_metadata)
    return transformed_run_metadata


def load_latest_run_metadata(service: str) -> dict:
    """Load the latest run metadata for a service from DynamoDB."""
    filtered_items: list[dict] = dynamodb.query_items_by_service(
        table_name=dynamodb_table_name,
        service=service,
    )

    if not filtered_items:
        logger.info(
            f"No previous run metadata found for {service}. Returning empty dict."
        )
        return {}

    # Sort filtered items by inference_timestamp in descending order
    sorted_items = sorted(
        filtered_items,
        key=lambda x: x.get("timestamp", {}).get("S", ""),
        reverse=True,
    )  # noqa

    logger.info(f"Latest run metadata: {sorted_items[0]}")

    # Return the most recent item
    return sorted_items[0]


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    reraise=True,
)
def write_run_metadata_to_db(run_metadata: RunExecutionMetadata) -> None:
    """Write metadata with retry logic.

    Raises:
        MetadataWriteError: If all retries fail (critical failure).
    """
    try:
        dynamodb.insert_item_into_table(
            item=run_metadata.model_dump(),
            table_name=dynamodb_table_name,
        )
        logger.info(
            f"Successfully inserted execution metadata: {run_metadata.model_dump()}"
        )
    except Exception as e:
        logger.error(f"Error writing execution metadata to DynamoDB: {e}")
        raise MetadataWriteError(run_metadata.service, e) from e


@track_performance
def run_integration_request(
    request: IntegrationRequest,
    wandb_logger: Optional[Callable[[str, dict], None]] = None,
    load_metadata: Optional[Callable[[str], dict]] = None,
    write_metadata: Optional[Callable[[RunExecutionMetadata], None]] = None,
) -> RunExecutionMetadata:
    """Runs an integration request with dependency injection.

    This is the internal API that supports dependency injection for testing.
    The public API (invoke_pipeline_handler) should not expose DI parameters.

    Args:
        request: The integration request to execute
        wandb_logger: Optional WandB logger callable (defaults to real implementation via factory)
        load_metadata: Optional metadata loader callable (defaults to real implementation)
        write_metadata: Optional metadata writer callable (defaults to real implementation)

    Returns:
        RunExecutionMetadata: Execution metadata from the handler

    Note:
        Default implementations use lazy loading to avoid circular imports.
        Import statements are inside factory functions, not at module level.
    """
    # Use dependency injection or defaults (lazy-loaded via factories)
    wandb_log = wandb_logger or _default_wandb_logger()
    load_meta = load_metadata or _default_load_metadata()
    write_meta = write_metadata or _default_write_metadata()

    # Load previous metadata
    previous_run_metadata = load_meta(request.service)

    # Execute handler
    run_metadata = run_integration_service(request, previous_run_metadata)

    # Write metadata
    write_meta(run_metadata)

    # Log to WandB (non-blocking - failures are logged but don't raise)
    try:
        wandb_log(request.service, run_metadata.model_dump())
    except ObservabilityError:
        # Observability failures are logged but don't break pipeline execution
        logger.warning(
            f"Observability logging failed for {request.service}, continuing execution"
        )

    return run_metadata
