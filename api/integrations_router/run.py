"""Logic for running integration requests."""

from typing import Callable

from api.integrations_router.map import MAP_INTEGRATION_REQUEST_TO_SERVICE
from api.integrations_router.models import (
    IntegrationRequest,
    IntegrationResponse,
    RunExecutionMetadata,
)
from lib.aws.dynamodb import DynamoDB
from lib.helper import track_performance
from lib.log.logger import get_logger

logger = get_logger(__file__)

dynamodb = DynamoDB()


@track_performance
def run_integration_service(request: IntegrationRequest) -> RunExecutionMetadata:
    """Runs an integration service."""
    service = request.service
    payload = request.payload
    service_fn: Callable = MAP_INTEGRATION_REQUEST_TO_SERVICE[service]
    execution_metadata = service_fn(**payload)
    transformed_execution_metadata = RunExecutionMetadata(**execution_metadata)
    return transformed_execution_metadata


def write_execution_metadata_to_db(execution_metadata: RunExecutionMetadata) -> None:
    """Writes execution metadata to DynamoDB."""
    try:
        dynamodb.insert_item_into_table(
            item=execution_metadata.dict(),
            table_name=execution_metadata.metadata_table_name,
        )
        logger.info(
            f"Successfully inserted execution metadata: {execution_metadata.dict()}"
        )
    except Exception as e:
        logger.error(f"Error writing execution metadata to DynamoDB: {e}")
        raise


@track_performance
def run_integration_request(request: IntegrationRequest) -> IntegrationResponse:
    """Runs an integration request."""
    execution_metadata: RunExecutionMetadata = run_integration_service(request=request)
    write_execution_metadata_to_db(execution_metadata=execution_metadata)
    return IntegrationResponse(
        service=execution_metadata.service,
        timestamp=execution_metadata.timestamp,
        status_code=execution_metadata.status_code,
        body=execution_metadata.body,
    )
