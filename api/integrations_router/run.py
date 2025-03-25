"""Logic for running integration requests."""

from typing import Callable, Optional

from api.integrations_router.map import get_handler
from api.integrations_router.models import IntegrationRequest
from lib.aws.dynamodb import DynamoDB
from lib.helper import track_performance
from lib.log.logger import get_logger
from lib.metadata.models import RunExecutionMetadata
from lib.telemetry.wandb import log_run_to_wandb
from services.ml_inference.helper import dynamodb_table_name

logger = get_logger(__file__)

dynamodb = DynamoDB()


@log_run_to_wandb(service_name="run_integration_service")
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
    service_handler: Callable = get_handler(service)
    run_metadata = service_handler(event=payload, context=None)
    transformed_run_metadata = RunExecutionMetadata(**run_metadata)
    return transformed_run_metadata


def load_latest_run_metadata(service: str) -> dict:
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


def write_run_metadata_to_db(run_metadata: RunExecutionMetadata) -> None:
    """Writes execution metadata to DynamoDB."""
    try:
        dynamodb.insert_item_into_table(
            item=run_metadata.dict(),
            table_name=dynamodb_table_name,
        )
        logger.info(f"Successfully inserted execution metadata: {run_metadata.dict()}")
    except Exception as e:
        logger.error(f"Error writing execution metadata to DynamoDB: {e}")
        raise


@track_performance
def run_integration_request(request: IntegrationRequest) -> RunExecutionMetadata:
    """Runs an integration request."""
    previous_run_metadata: dict = load_latest_run_metadata(request.service)
    run_metadata: RunExecutionMetadata = run_integration_service(
        request=request,
        previous_run_metadata=previous_run_metadata,
    )
    write_run_metadata_to_db(run_metadata=run_metadata)
    return run_metadata
