"""Logic for running integration requests."""

from typing import Callable

from api.integrations_router.map import MAP_INTEGRATION_REQUEST_TO_SERVICE
from api.integrations_router.models import (
    IntegrationRequest,
    IntegrationResponse,
    RunExecutionMetadata,
)
from lib.log.logger import get_logger

logger = get_logger(__file__)


def transform_run_metadata(
    request: IntegrationRequest, execution_metadata: dict
) -> RunExecutionMetadata:
    """Transforms run metadata into the expected format."""
    return RunExecutionMetadata(request=request, execution_metadata=execution_metadata)


def run_integration_service(request: IntegrationRequest) -> RunExecutionMetadata:
    """Runs an integration service."""
    service = request.service
    payload = request.payload
    service_fn: Callable = MAP_INTEGRATION_REQUEST_TO_SERVICE[service]
    execution_metadata = service_fn(**payload)
    transformed_execution_metadata = transform_run_metadata(
        request=request, execution_metadata=execution_metadata
    )
    return transformed_execution_metadata


# TODO: check how I do this elsewhere.
def write_execution_metadata_to_db(
    request: IntegrationRequest, execution_metadata: RunExecutionMetadata
) -> None:
    """Writes execution metadata to DynamoDB."""
    pass


def run_integration_request(request: IntegrationRequest) -> IntegrationResponse:
    """Runs an integration request."""
    try:
        execution_metadata = run_integration_service(request=request)
        write_execution_metadata_to_db(
            request=request, execution_metadata=execution_metadata
        )
    except Exception as e:
        logger.error(f"Error running integration request: {e}")
        execution_metadata = {}
    write_execution_metadata_to_db(
        request=request, execution_metadata=execution_metadata
    )
    job_status = execution_metadata.get("job_status", "failed")
    return IntegrationResponse(status=job_status)
